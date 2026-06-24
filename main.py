"""
stock-radar FastAPI 後端
對應 Streamlit app.py 的所有資料邏輯
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pandas as pd
import requests
from io import StringIO
import re
import datetime
import urllib3
import unicodedata
import yfinance as yf
import json
import os
import hashlib
import hmac
import time
from typing import Optional
from functools import lru_cache

try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSHEETS_AVAILABLE = True
except ImportError:
    GSHEETS_AVAILABLE = False

try:
    import bcrypt
    BCRYPT_AVAILABLE = True
except ImportError:
    BCRYPT_AVAILABLE = False

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = FastAPI(title="stock-radar API")

ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

JWT_SECRET = os.environ.get("JWT_SECRET", "change-me-in-production")
SPREADSHEET_URL = os.environ.get("SPREADSHEET_URL", "")
GCP_SERVICE_ACCOUNT_JSON = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
GOOGLE_DRIVE_HQ_DATA_URL = "https://drive.google.com/file/d/112sWHyGbfuNyOEN2M85wIhWtHj1MqKj5/view?usp=drive_link"
GOOGLE_DRIVE_BRANCH_DATA_URL = "https://drive.google.com/file/d/1C6axJwaHq3SFRslODK8m28WRYFDd90x_/view?usp=drive_link"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}

def _b64url(data: bytes) -> str:
    import base64
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

def _b64url_decode(s: str) -> bytes:
    import base64
    s += "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s)

def create_token(username: str, role: str, expire_hours: int = 24 * 7) -> str:
    header = _b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = _b64url(json.dumps({
        "sub": username,
        "role": role,
        "exp": int(time.time()) + expire_hours * 3600
    }).encode())
    sig = _b64url(hmac.new(JWT_SECRET.encode(), f"{header}.{payload}".encode(), hashlib.sha256).digest())
    return f"{header}.{payload}.{sig}"

def verify_token(token: str) -> dict:
    try:
        parts = token.split(".")
        if len(parts) != 3: raise ValueError("bad token")
        header, payload_b64, sig = parts
        expected_sig = _b64url(hmac.new(JWT_SECRET.encode(), f"{header}.{payload_b64}".encode(), hashlib.sha256).digest())
        if not hmac.compare_digest(sig, expected_sig): raise ValueError("bad sig")
        payload = json.loads(_b64url_decode(payload_b64))
        if payload.get("exp", 0) < time.time(): raise ValueError("expired")
        return payload
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))

def get_current_user(authorization: str = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing token")
    return verify_token(authorization[7:])

@lru_cache(maxsize=1)
def get_gsheets_client():
    if not GSHEETS_AVAILABLE or not GCP_SERVICE_ACCOUNT_JSON:
        return None
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        info = json.loads(GCP_SERVICE_ACCOUNT_JSON)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        return gspread.authorize(creds)
    except Exception:
        return None

def get_worksheet(sheet_name: str):
    client = get_gsheets_client()
    if not client: return None
    try:
        doc = client.open_by_url(SPREADSHEET_URL.split("?")[0])
        try:
            return doc.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = doc.add_worksheet(title=sheet_name, rows="1000", cols="2")
            ws.append_row(["Username", f"{sheet_name}JSON"])
            return ws
    except Exception:
        return None

def sheets_load(sheet_name: str, username: str) -> list:
    ws = get_worksheet(sheet_name)
    if not ws: return []
    try:
        cell = ws.find(username, in_column=1)
        if cell:
            data = ws.cell(cell.row, 2).value
            if data: return json.loads(data)
    except Exception:
        pass
    return []

def sheets_save(sheet_name: str, username: str, data: list) -> bool:
    ws = get_worksheet(sheet_name)
    if not ws: return False
    try:
        data_str = json.dumps(data, ensure_ascii=False)
        cell = ws.find(username, in_column=1)
        if cell:
            ws.update_cell(cell.row, 2, data_str)
        else:
            ws.append_row([username, data_str])
        return True
    except Exception:
        return False

def _download_drive(url: str) -> Optional[str]:
    file_id = url.split("/")[-2]
    dl_url = f"https://drive.google.com/uc?export=download&id={file_id}&t={int(time.time())}"
    try:
        r = requests.get(dl_url, stream=True, verify=False, timeout=15)
        r.raise_for_status()
        return r.text
    except Exception:
        return None

def _load_hq() -> dict:
    content = _download_drive(GOOGLE_DRIVE_HQ_DATA_URL)
    if not content: return {}
    result = {}
    for line in content.strip().split("\n"):
        if "\t" in line and not line.startswith("證券商代號"):
            parts = line.split("\t")
            if len(parts) == 2:
                result[parts[0].strip()] = parts[1].strip()
    return result

def _load_branches() -> str:
    content = _download_drive(GOOGLE_DRIVE_BRANCH_DATA_URL)
    if not content: return ""
    return content.strip().lstrip("'").rstrip("'")

def _build_broker_db(raw: str, hq_map: dict):
    tree = {}; name_map = {}
    for group_str in raw.strip().split(";"):
        if not group_str: continue
        parts = group_str.split("!")
        if not parts: continue
        head_info = parts[0].split(",")
        if len(head_info) != 2: continue
        bid, bname = head_info[0].strip(), head_info[1].replace("亚", "亞").strip()
        final_bname = hq_map.get(bid, bname)
        branches = {}
        for p in parts[1:]:
            if "," in p:
                br_id, br_name_raw = p.split(",", 1)
                br_name = br_name_raw.replace("亚", "亞").strip()
                if br_name not in branches:
                    branches[br_name] = br_id.strip()
                    name_map[br_name] = {"hq_id": bid, "br_id": br_id.strip(), "hq_name": final_bname}
        if final_bname not in branches:
            branches[final_bname] = bid
            name_map[final_bname] = {"hq_id": bid, "br_id": bid, "hq_name": final_bname}
        tree[final_bname] = {"bid": bid, "branches": branches}
    final_tree = {}
    for hq_name, hq_data in tree.items():
        seen = set(); unique = {}
        for br_name, br_id in hq_data["branches"].items():
            if br_name not in seen:
                unique[br_name] = br_id; seen.add(br_name)
        final_tree[hq_name] = {"bid": hq_data["bid"], "branches": unique}
    if "北城證券" in final_tree and "北城" in final_tree:
        if final_tree["北城證券"]["bid"] == final_tree["北城"]["bid"]:
            del final_tree["北城"]
            if "北城" in name_map: del name_map["北城"]
    return final_tree, name_map

print("載入券商資料庫...")
_HQ_DATA = _load_hq()
_RAW_BRANCH = _load_branches()
UI_TREE, BROKER_MAP = _build_broker_db(_RAW_BRANCH, _HQ_DATA)

GEO_MAP = {}
for br_name, br_info in BROKER_MAP.items():
    if "-" in br_name:
        loc = br_name.split("-")[-1].replace("(停)", "").strip()
        if loc:
            if loc not in GEO_MAP: GEO_MAP[loc] = {}
            GEO_MAP[loc][br_name] = br_info

print(f"券商資料庫載入完成：{len(BROKER_MAP)} 個分點，{len(GEO_MAP)} 個地緣關鍵字")

def get_stock_id(name_str: str) -> Optional[str]:
    s = unicodedata.normalize("NFKC", str(name_str).strip()).replace(" ", "")
    m = re.match(r"^(\d+[A-Za-z])(?![A-Za-z])", s)
    if m: return m.group(1).upper()
    m = re.match(r"^(\d+)", s)
    if m: return m.group(1).upper()
    return None

def calculate_macd(closes: list, fast: int, slow: int, signal: int):
    def ema(vals, span):
        result = [None] * len(vals)
        k = 2 / (span + 1)
        for i, v in enumerate(vals):
            if v is None: continue
            if result[i-1] is None: result[i] = v
            else: result[i] = v * k + result[i-1] * (1 - k)
        return result
    exp1 = ema(closes, fast)
    exp2 = ema(closes, slow)
    macd_line = [a - b if a and b else None for a, b in zip(exp1, exp2)]
    sig_line = ema([v for v in macd_line if v is not None], signal)
    sig_full = [None] * len(macd_line)
    j = 0
    for i, v in enumerate(macd_line):
        if v is not None:
            sig_full[i] = sig_line[j] if j < len(sig_line) else None
            j += 1
    hist = [a - b if a is not None and b is not None else None for a, b in zip(macd_line, sig_full)]
    return macd_line, sig_full, hist

class LoginRequest(BaseModel):
    email: str
    password: str

@app.post("/api/auth/login")
def login(req: LoginRequest):
    if not BCRYPT_AVAILABLE:
        raise HTTPException(500, "bcrypt not installed")
    ws = get_worksheet("Users")
    if not ws:
        raise HTTPException(500, "Cannot connect to Users sheet")
    try:
        records = ws.get_all_records()
        email_lower = req.email.strip().lower()
        for row in records:
            if str(row.get("email", "")).strip().lower() == email_lower:
                status = str(row.get("status", "")).strip().lower()
                if status == "pending":
                    raise HTTPException(403, "帳號審核中")
                if status != "active":
                    raise HTTPException(403, "帳號已停用")
                exp_str = str(row.get("expire_date", "2099-12-31")).strip()
                try:
                    if datetime.date.today() > datetime.datetime.strptime(exp_str, "%Y-%m-%d").date():
                        raise HTTPException(403, "帳號已到期")
                except ValueError:
                    pass
                stored_hash = str(row.get("password_hash", "")).strip()
                if bcrypt.checkpw(req.password.encode(), stored_hash.encode()):
                    username = str(row.get("username", req.email)).strip()
                    role = str(row.get("role", "member")).strip().lower()
                    token = create_token(username, role)
                    return {"token": token, "username": username, "role": role}
                else:
                    raise HTTPException(401, "密碼錯誤")
        raise HTTPException(401, "找不到此 email")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/brokers/tree")
def get_broker_tree():
    result = {}
    for hq_name, hq_data in UI_TREE.items():
        result[hq_name] = {"bid": hq_data["bid"], "branches": list(hq_data["branches"].keys())}
    return result

@app.get("/api/brokers/map")
def get_broker_map():
    return BROKER_MAP

@app.get("/api/brokers/geo")
def get_geo_map():
    return {loc: list(branches.keys()) for loc, branches in GEO_MAP.items()}

@app.get("/api/broker/stocks")
def broker_stocks(hq_id: str, br_id: str, start: str, end: str, unit: str = "shares"):
    c_param = "B" if unit == "amount" else "E"
    col_buy = "買進金額" if unit == "amount" else "買進張數"
    col_sell = "賣出金額" if unit == "amount" else "賣出張數"
    url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm?a={hq_id}&b={br_id}&c={c_param}&e={start}&f={end}"
    try:
        res = requests.get(url, headers=HEADERS, verify=False, timeout=25)
        res.encoding = "big5"
        def extract_name(match):
            m = re.search(r"GenLink2stk\s*\(\s*['\"](?:AS)?([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", match.group(0), re.IGNORECASE)
            if m: return f"{m.group(1).strip()}{m.group(2).strip()}"
            return ""
        processed = re.sub(r"<script[^>]*>(?:(?!</script>).)*GenLink2stk\s*\([^)]+\).*?</script>", extract_name, res.text, flags=re.IGNORECASE | re.DOTALL)
        tables = pd.read_html(StringIO(processed))
        df_all = pd.DataFrame()
        for tb in tables:
            if tb.shape[1] < 3: continue
            if any(w in str(tb) for w in ["買進","賣出","張數","金額","股票名稱"]):
                if tb.shape[1] >= 8:
                    l = tb.iloc[:,[0,1,2]].copy(); l.columns=["股票名稱",col_buy,col_sell]
                    r = tb.iloc[:,[5,6,7]].copy(); r.columns=["股票名稱",col_buy,col_sell]
                    df_all = pd.concat([df_all,l,r], ignore_index=True)
                else:
                    tmp = tb.iloc[:,[0,1,2]].copy(); tmp.columns=["股票名稱",col_buy,col_sell]
                    df_all = pd.concat([df_all,tmp], ignore_index=True)
        if df_all.empty: return []
        df_all["股票名稱"] = df_all["股票名稱"].astype(str).str.strip()
        df_all = df_all[~df_all["股票名稱"].str.contains("名稱|買進|賣出|合計|說明|註|差額|請選擇|nan|NaN|None|^\\s*$", na=False)]
        df_all = df_all[df_all["股票名稱"].apply(lambda x: bool(get_stock_id(x)))].copy()
        for c in [col_buy, col_sell]:
            df_all[c] = pd.to_numeric(df_all[c].astype(str).str.replace(",",""), errors="coerce").fillna(0)
        df_all["總額"] = df_all[col_buy] + df_all[col_sell]
        df_all = df_all[df_all["總額"] > 0].copy()
        df_all["買%"] = (df_all[col_buy] / df_all["總額"] * 100).round(1)
        df_all["賣%"] = (df_all[col_sell] / df_all["總額"] * 100).round(1)
        df_all["股票代號"] = df_all["股票名稱"].apply(get_stock_id)
        def strip_id(name, sid):
            if sid and name.startswith(sid):
                return name[len(sid):].strip()
            return name
        df_all["股票名稱"] = df_all.apply(lambda r: strip_id(r["股票名稱"], r["股票代號"]), axis=1)
        df_all = df_all.replace([float('inf'), float('-inf')], 0).fillna(0)
        return df_all.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/stock/brokers")
def stock_brokers(sid: str, start: str, end: str):
    url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco.djhtm?a={sid}&e={start}&f={end}"
    try:
        res = requests.get(url, headers=HEADERS, verify=False, timeout=25)
        res.encoding = "big5"
        tables = pd.read_html(StringIO(res.text))
        df_all = pd.DataFrame()
        for tb in tables:
            if tb.shape[1] == 10:
                l = tb.iloc[:,[0,1,2]].copy(); l.columns=["券商","買","賣"]
                r = tb.iloc[:,[5,6,7]].copy(); r.columns=["券商","買","賣"]
                df_all = pd.concat([df_all,l,r], ignore_index=True)
        if df_all.empty: return []
        df_all = df_all.dropna(subset=["券商"])
        df_all = df_all[~df_all["券商"].astype(str).str.contains("券商|合計|平均|說明|註", na=False)]
        for c in ["買","賣"]:
            df_all[c] = pd.to_numeric(df_all[c].astype(str).str.replace(",",""), errors="coerce").fillna(0)
        df_all["合計"] = df_all["買"] + df_all["賣"]
        df_all = df_all[df_all["合計"] > 0].copy()
        df_all["買進%"] = (df_all["買"]/df_all["合計"]*100).round(1)
        df_all["賣出%"] = (df_all["賣"]/df_all["合計"]*100).round(1)
        df_all = df_all.replace([float('inf'), float('-inf')], 0).fillna(0)
        return df_all.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/stock/broker_history")
def broker_history(sid: str, br_id: str, start: str = "2015-01-01"):
    today_str = datetime.date.today().strftime("%Y-%m-%d")
    url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco0/zco0.djhtm?A={sid}&BHID={br_id}&b={br_id}&C=3&D={start}&E={today_str}&ver=V3"
    try:
        res = requests.get(url, headers=HEADERS, verify=False, timeout=20)
        res.encoding = "big5"
        stock_name = ""
        m = re.search(r"對\s+([^\(]+)\(\s*" + re.escape(sid) + r"\s*\)個股", res.text)
        if m: stock_name = m.group(1).strip()
        tables = pd.read_html(StringIO(res.text))
        records = []
        for tb in tables:
            if tb.shape[1] == 5 and "日期" in str(tb.iloc[0].values):
                df_b = tb.copy()
                df_b.columns = ["Date","買進","賣出","總額","買賣超"]
                df_b = df_b.drop(0)
                df_b = df_b[~df_b["Date"].str.contains("日期|合計|說明", na=False)].copy()
                df_b["Date"] = pd.to_datetime(df_b["Date"].astype(str).str.replace(" ",""))
                df_b["買賣超"] = pd.to_numeric(df_b["買賣超"].astype(str).str.replace(",",""), errors="coerce").fillna(0)
                df_b["Date"] = df_b["Date"].dt.strftime("%Y-%m-%d")
                records = df_b[["Date","買賣超"]].to_dict(orient="records")
                break
        return {"stock_name": stock_name, "records": records}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/stock/kline")
def stock_kline(sid: str, start: str = "2015-01-01", interval: str = "1d"):
    """TAB4：K線資料 proxy"""
    import math, time as time_mod
    YAHOO_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    }
    is_60m = (interval == "60m")
    now = int(time_mod.time())
    if is_60m:
        p1 = now - 60 * 86400
        p2 = now + 86400
        range_param = f"period1={p1}&period2={p2}"
    else:
        try:
            start_dt = datetime.datetime.strptime(start, "%Y-%m-%d")
            p1 = int(start_dt.timestamp())
        except Exception:
            p1 = now - 10 * 365 * 86400
        p2 = now + 86400  # 明天，確保拿到今天最新資料
        range_param = f"period1={p1}&period2={p2}"
    special = sid.startswith("^") or "=" in sid
    if sid.upper() in ("TXF", "TXF=F", "台指期"):
        suffixes_list = ["TXF=F", "TWF=F", "^TWII"]
    elif special:
        suffixes_list = [sid]
    else:
        suffixes_list = [f"{sid}.TW", f"{sid}.TWO"]
    for ticker in suffixes_list:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?{range_param}&interval={interval}&includePrePost=false"
            r = requests.get(url, headers=YAHOO_HEADERS, timeout=20)
            if not r.ok: continue
            j = r.json()
            result = j.get("chart", {}).get("result", [])
            if not result: continue
            result = result[0]
            timestamps = result.get("timestamp", [])
            quotes = result.get("indicators", {}).get("quote", [{}])[0]
            meta = result.get("meta", {})
            stock_name = meta.get("longName") or meta.get("shortName") or ticker
            if not timestamps or not quotes: continue
            data = []
            for i, ts in enumerate(timestamps):
                c = quotes.get("close", [])[i] if i < len(quotes.get("close", [])) else None
                o = quotes.get("open", [])[i] if i < len(quotes.get("open", [])) else None
                h = quotes.get("high", [])[i] if i < len(quotes.get("high", [])) else None
                l = quotes.get("low", [])[i] if i < len(quotes.get("low", [])) else None
                if c is None or (isinstance(c, float) and math.isnan(c)): continue
                if is_60m:
                    data.append({"Date": ts, "Open": o, "High": h, "Low": l, "Close": c})
                else:
                    dt_str = datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
                    if dt_str < start: continue
                    data.append({"Date": dt_str, "Open": o, "High": h, "Low": l, "Close": c})
            if data:
                # 日線模式：如果最新日線資料不是今天，嘗試用60分線聚合補上今日K棒
                if not is_60m:
                    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
                    last_date = data[-1]["Date"]
                    if last_date < today_str:
                        try:
                            # 抓最近幾天的60分線
                            p1_60 = now - 7 * 86400
                            p2_60 = now + 86400
                            url60 = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?period1={p1_60}&period2={p2_60}&interval=60m&includePrePost=false"
                            r60 = requests.get(url60, headers=YAHOO_HEADERS, timeout=20)
                            if r60.ok:
                                j60 = r60.json()
                                res60 = j60.get("chart", {}).get("result", [])
                                if res60:
                                    res60 = res60[0]
                                    ts60 = res60.get("timestamp", [])
                                    q60 = res60.get("indicators", {}).get("quote", [{}])[0]
                                    # 把60分線groupby日期
                                    by_date = {}
                                    for i, ts in enumerate(ts60):
                                        c = q60.get("close", [])[i] if i < len(q60.get("close", [])) else None
                                        o = q60.get("open", [])[i] if i < len(q60.get("open", [])) else None
                                        h = q60.get("high", [])[i] if i < len(q60.get("high", [])) else None
                                        l = q60.get("low", [])[i] if i < len(q60.get("low", [])) else None
                                        if c is None or (isinstance(c, float) and math.isnan(c)): continue
                                        # Yahoo timestamp是UTC，台灣時間+8小時
                                        dt_local = datetime.datetime.utcfromtimestamp(ts) + datetime.timedelta(hours=8)
                                        d_str = dt_local.strftime("%Y-%m-%d")
                                        if d_str <= last_date: continue  # 只處理 last_date 之後的日期
                                        if d_str not in by_date:
                                            by_date[d_str] = {"Open": o, "High": h, "Low": l, "Close": c}
                                        else:
                                            bd = by_date[d_str]
                                            if h is not None and (bd["High"] is None or h > bd["High"]): bd["High"] = h
                                            if l is not None and (bd["Low"] is None or l < bd["Low"]): bd["Low"] = l
                                            bd["Close"] = c  # 最後一筆當收盤
                                    # 補上日K
                                    for d_str in sorted(by_date.keys()):
                                        bd = by_date[d_str]
                                        data.append({"Date": d_str, "Open": bd["Open"], "High": bd["High"], "Low": bd["Low"], "Close": bd["Close"]})
                        except Exception:
                            pass
                return {"suffix": ticker, "data": data, "stock_name": stock_name, "interval": interval}
        except Exception:
            continue
    raise HTTPException(404, f"找不到 {sid} 的K線資料（interval={interval}）")

_vip_cache: dict = {}
_VIP_CACHE_TTL = 300

@app.get("/api/vip/scan")
def vip_scan(sheet: str = "ScanResult", user: dict = Depends(get_current_user)):
    if user.get("role") != "vip":
        raise HTTPException(403, "VIP 限定")
    now = datetime.datetime.now().timestamp()
    cached = _vip_cache.get(sheet)
    if cached and (now - cached["ts"]) < _VIP_CACHE_TTL:
        return cached["data"]
    ws = get_worksheet(sheet)
    if not ws: return []
    try:
        data = ws.get_all_records()
        _vip_cache[sheet] = {"data": data, "ts": now}
        return data
    except Exception as e:
        raise HTTPException(500, str(e))

# ── 籌碼鎖定率 ──
# 鎖定率 = 主力券商買超張數 / (發行張數 - 董監持股張數) * 100%
# 分母（發行/董監）月更，做成快取表；分子每日從富邦排行/個股頁爬。
_chip_ref: dict = {}            # sid -> {"issued_z":發行張數, "dir_z":董監張數, "name":簡稱}
_chip_ref_ts = 0.0
_CHIP_REF_TTL = 6 * 3600
_chip_rank_cache: dict = {}     # market -> {"data":[...], "ts":...}
_CHIP_RANK_TTL = 600

def _num(x) -> float:
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return 0.0

def _fetch_json(url: str):
    r = requests.get(url, headers=HEADERS, verify=False, timeout=30)
    r.encoding = "utf-8"
    return r.json()

def _build_chip_ref() -> dict:
    ref: dict = {}
    # 上市基本資料：已發行普通股數（最後一欄），退而求其次用 實收資本額/面額
    try:
        for row in _fetch_json("https://openapi.twse.com.tw/v1/opendata/t187ap03_L"):
            sid = str(row.get("公司代號", "")).strip()
            if not sid:
                continue
            ik = next((k for k in row if "已發行" in k), None)
            shares = _num(row.get(ik)) if ik else 0.0
            if shares <= 0:
                par = _num(re.sub(r"[^\d.]", "", str(row.get("普通股每股面額", "10")))) or 10.0
                shares = _num(row.get("實收資本額")) / par if par else 0.0
            ref[sid] = {"issued_z": shares / 1000, "dir_z": 0.0, "name": str(row.get("公司簡稱", "")).strip()}
    except Exception as e:
        print(f"[chip_ref] 上市基本資料 失敗: {e}")
    # 上櫃基本資料：IssueShares 為已發行股數
    try:
        for row in _fetch_json("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"):
            sid = str(row.get("SecuritiesCompanyCode", "")).strip()
            if not sid:
                continue
            ref[sid] = {"issued_z": _num(row.get("IssueShares")) / 1000, "dir_z": 0.0,
                        "name": str(row.get("CompanyAbbreviation", "")).strip()}
    except Exception as e:
        print(f"[chip_ref] 上櫃基本資料 失敗: {e}")
    # 董監持股（上市+上櫃）：同一持股人常因多席次/多職稱重複列（法人董事尤甚，
    # 例如陽明法人佔多席→同筆持股列多次）。以「姓名」去重，同一人取最大持股只算一次，
    # 否則會嚴重灌水（2609 naive 加總後董監>發行→分母負數被丟掉）。
    # ponytail: 以姓名去重；極少數同公司同名不同人會略少算，可接受。
    holders: dict = {}   # sid -> {姓名: 持股(股)}
    for url in ["https://openapi.twse.com.tw/v1/opendata/t187ap11_L",
                "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap11_O"]:
        try:
            for row in _fetch_json(url):
                sid = str(row.get("公司代號", "")).strip()
                if sid in ref:
                    nm = str(row.get("姓名", "")).strip()
                    h = _num(row.get("目前持股"))
                    cur = holders.setdefault(sid, {})
                    if h > cur.get(nm, 0.0):
                        cur[nm] = h
        except Exception as e:
            print(f"[chip_ref] 董監持股 {url} 失敗: {e}")
    for sid, hd in holders.items():
        ref[sid]["dir_z"] = sum(hd.values()) / 1000
    print(f"[chip_ref] 建表完成：{len(ref)} 檔")
    return ref

def get_chip_ref() -> dict:
    global _chip_ref, _chip_ref_ts
    now = time.time()
    if not _chip_ref or (now - _chip_ref_ts) > _CHIP_REF_TTL:
        built = _build_chip_ref()
        if built:
            _chip_ref, _chip_ref_ts = built, now
    return _chip_ref

def _chip_rate(sid: str, net_z: float, ref: dict):
    """回傳該檔鎖定率 dict，分母不足或查無基本資料則回 None。"""
    info = ref.get(sid)
    if not info:
        return None
    denom = info["issued_z"] - info["dir_z"]
    if denom <= 0:
        return None
    return {"sid": sid, "name": info["name"], "net_buy": int(round(net_z)),
            "issued": int(round(info["issued_z"])), "dir": int(round(info["dir_z"])),
            "denom": int(round(denom)), "rate": round(net_z / denom * 100, 2)}

def _parse_ranking(market: str) -> dict:
    """富邦主力買超排行 → {sid: 買超張數}。market 0=上市 1=上櫃。"""
    url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_F_{market}_1.djhtm"
    res = requests.get(url, headers=HEADERS, verify=False, timeout=25)
    res.encoding = "big5"
    parts = re.split(r"Link2Stk\('([^']+)'\)", res.text)
    out = {}
    for i in range(1, len(parts) - 1, 2):
        sid = parts[i].strip()
        nums = re.findall(r't3n1">&nbsp;([\d,.]+)', parts[i + 1])  # [成交價, 買進, 賣出, 買超]
        if len(nums) >= 4:
            out[sid] = _num(nums[-1])
    return out

@app.get("/api/vip/chip_lock")
def vip_chip_lock(market: str = "all", user: dict = Depends(get_current_user)):
    if user.get("role") != "vip":
        raise HTTPException(403, "VIP 限定")
    cached = _chip_rank_cache.get(market)
    if cached and (time.time() - cached["ts"]) < _CHIP_RANK_TTL:
        return cached["data"]
    ref = get_chip_ref()
    if not ref:
        raise HTTPException(503, "發行/董監資料尚未就緒，請稍後再試")
    markets = ["0", "1"] if market == "all" else [market]
    rows = []
    for mk in markets:
        try:
            for sid, net in _parse_ranking(mk).items():
                r = _chip_rate(sid, net, ref)
                if r:
                    r["market"] = "上市" if mk == "0" else "上櫃"
                    rows.append(r)
        except Exception as e:
            print(f"[chip_lock] 排行 market={mk} 失敗: {e}")
    rows.sort(key=lambda x: -x["rate"])
    _chip_rank_cache[market] = {"data": rows, "ts": time.time()}
    return rows

@app.get("/api/stock/chip_lock")
def stock_chip_lock(sid: str, start: str, end: str):
    sid = sid.strip().upper()
    ref = get_chip_ref()
    info = ref.get(sid)
    if not info:
        raise HTTPException(404, f"查無 {sid} 的發行/董監資料（ETF、興櫃或新上市可能無）")
    url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco.djhtm?a={sid}&e={start}&f={end}"
    try:
        res = requests.get(url, headers=HEADERS, verify=False, timeout=25)
        res.encoding = "big5"
        def grab(label):
            m = re.search(re.escape(label) + r"</td>\s*<td[^>]*>([\d,]+)", res.text)
            return _num(m.group(1)) if m else 0.0
        net = grab("合計買超張數") - grab("合計賣超張數")
    except Exception as e:
        raise HTTPException(500, str(e))
    r = _chip_rate(sid, net, ref)
    if not r:
        raise HTTPException(422, f"{sid} 分母不足（發行-董監≤0）")
    r["start"], r["end"] = start, end
    return r

def _parse_djbcd(sid: str):
    """富邦主力進出比較圖資料：CZCO.DJBCD?A=sid。
    單行，空白分三段（日期MMDD / 股價 / 主力買賣超），段內逗號分隔，約45個交易日。
    回傳 [(YYYY-MM-DD, 主力買賣超張), ...] 依時間排序。"""
    url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/CZCO.DJBCD?A={sid}"
    res = requests.get(url, headers={**HEADERS, "Referer": f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco.djhtm?a={sid}"},
                       verify=False, timeout=20)
    toks = [x for x in re.split(r"[,\s]+", res.text.strip()) if x]
    n = len(toks) // 3
    if n == 0:
        return []
    mmdd, mf = toks[:n], toks[2 * n:3 * n]
    # MMDD 無年份：以今天為錨往回推，月份變大代表跨年（前一年）
    today = datetime.date.today()
    yrs = [today.year] * n
    y, prev_m = today.year, None
    for i in range(n - 1, -1, -1):
        try:
            m = int(mmdd[i][:2])
        except Exception:
            continue
        if prev_m is not None and m > prev_m:
            y -= 1
        yrs[i] = y
        prev_m = m
    out = []
    for i in range(n):
        try:
            d = f"{yrs[i]:04d}-{mmdd[i][:2]}-{mmdd[i][2:4]}"
            out.append((d, _num(mf[i])))
        except Exception:
            continue
    return out

@app.get("/api/stock/chip_lock_series")
def stock_chip_lock_series(sid: str):
    """逐日籌碼鎖定率序列（約45交易日）：rate = 區間起累積主力買賣超 / 流通 * 100%。"""
    sid = sid.strip().upper()
    ref = get_chip_ref()
    info = ref.get(sid)
    if not info:
        raise HTTPException(404, f"查無 {sid} 的發行/董監資料（ETF、興櫃或新上市可能無）")
    denom = info["issued_z"] - info["dir_z"]
    if denom <= 0:
        raise HTTPException(422, f"{sid} 分母不足（發行-董監≤0）")
    series = _parse_djbcd(sid)
    if not series:
        raise HTTPException(502, "主力進出資料抓取失敗")
    cum, data = 0.0, []
    for d, mf in series:
        cum += mf
        data.append({"date": d, "mf": int(round(mf)), "cum": int(round(cum)),
                     "rate": round(cum / denom * 100, 3)})
    return {"sid": sid, "name": info["name"], "denom": int(round(denom)), "data": data}

@app.get("/api/watchlist")
def get_watchlist(user: dict = Depends(get_current_user)):
    return sheets_load("Watchlist", user["sub"])

class WatchlistSaveRequest(BaseModel):
    items: list

@app.post("/api/watchlist")
def save_watchlist(req: WatchlistSaveRequest, user: dict = Depends(get_current_user)):
    ok = sheets_save("Watchlist", user["sub"], req.items)
    if not ok: raise HTTPException(500, "儲存失敗")
    return {"ok": True}

@app.get("/api/working_group")
def get_working_group(user: dict = Depends(get_current_user)):
    return sheets_load("WorkingGroup", user["sub"])

@app.post("/api/working_group")
def save_working_group(req: WatchlistSaveRequest, user: dict = Depends(get_current_user)):
    ok = sheets_save("WorkingGroup", user["sub"], req.items)
    if not ok: raise HTTPException(500, "儲存失敗")
    return {"ok": True}

@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.datetime.now().isoformat()}

@app.on_event("startup")
async def warmup_cache():
    async def _warm():
        await asyncio.sleep(3)
        try:
            now = datetime.datetime.now().timestamp()
            for sheet in ["ScanResult", "ScanResult_W", "ScanResult_S"]:
                ws = get_worksheet(sheet)
                if ws:
                    data = ws.get_all_records()
                    _vip_cache[sheet] = {"data": data, "ts": now}
                    print(f"[warmup] {sheet}: {len(data)} rows cached")
        except Exception as e:
            print(f"[warmup] failed: {e}")
    asyncio.create_task(_warm())

@app.get("/api/admin/reload_brokers")
def reload_brokers():
    global _HQ_DATA, _RAW_BRANCH, UI_TREE, BROKER_MAP, GEO_MAP
    print("手動重新載入券商資料庫...")
    _HQ_DATA = _load_hq()
    _RAW_BRANCH = _load_branches()
    UI_TREE, BROKER_MAP = _build_broker_db(_RAW_BRANCH, _HQ_DATA)
    new_geo_map = {}
    for br_name, br_info in BROKER_MAP.items():
        if "-" in br_name:
            loc = br_name.split("-")[-1].replace("(停)", "").strip()
            if loc:
                if loc not in new_geo_map: new_geo_map[loc] = {}
                new_geo_map[loc][br_name] = br_info
    GEO_MAP = new_geo_map
    msg = f"更新成功！最新分點數量：{len(BROKER_MAP)} 個"
    print(msg)
    return {"status": "ok", "message": msg, "total_branches": len(BROKER_MAP)}

@app.get("/api/txf/kline")
def txf_kline(start: str = "2013-01-01"):
    """台指期K線，供前端計算MACD狀態用"""
    for ticker in ["TXF=F", "TWF=F", "^TWII"]:
        try:
            df = yf.download(ticker, period="max", interval="1d", progress=False, auto_adjust=True)
            if df.empty: continue
            df = df.dropna(subset=["Close"])
            data = []
            for idx, row in df.iterrows():
                dt_str = idx.strftime("%Y-%m-%d")
                if dt_str < start: continue
                try:
                    data.append({"Date": dt_str, "Close": float(row["Close"])})
                except Exception:
                    continue
            if data:
                return {"ticker": ticker, "data": data}
        except Exception:
            continue
    return {"ticker": None, "data": []}

@app.get("/")
def root():
    return {"app": "stock-radar API", "version": "2.0"}

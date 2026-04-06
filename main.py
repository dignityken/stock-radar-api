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

# ── CORS：允許 GitHub Pages 來源 ──
ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# 環境變數
# ==========================================
JWT_SECRET = os.environ.get("JWT_SECRET", "change-me-in-production")
SPREADSHEET_URL = os.environ.get("SPREADSHEET_URL", "")
GCP_SERVICE_ACCOUNT_JSON = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")  # JSON 字串
GOOGLE_DRIVE_HQ_DATA_URL = "https://drive.google.com/file/d/112sWHyGbfuNyOEN2M85wIhWtHj1MqKj5/view?usp=drive_link"
GOOGLE_DRIVE_BRANCH_DATA_URL = "https://drive.google.com/file/d/1C6axJwaHq3SFRslODK8m28WRYFDd90x_/view?usp=drive_link"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}

# ==========================================
# 簡易 JWT（不依賴第三方，手刻 HS256）
# ==========================================
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

# ==========================================
# Google Sheets 連線
# ==========================================
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

# ==========================================
# 券商資料庫（啟動時載入一次）
# ==========================================
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
    # dedup
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

# GEO_MAP
GEO_MAP = {}
for br_name, br_info in BROKER_MAP.items():
    if "-" in br_name:
        loc = br_name.split("-")[-1].replace("(停)", "").strip()
        if loc:
            if loc not in GEO_MAP: GEO_MAP[loc] = {}
            GEO_MAP[loc][br_name] = br_info

print(f"券商資料庫載入完成：{len(BROKER_MAP)} 個分點，{len(GEO_MAP)} 個地緣關鍵字")

# ==========================================
# 工具函數
# ==========================================
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
    # pad sig_line back
    sig_full = [None] * len(macd_line)
    j = 0
    for i, v in enumerate(macd_line):
        if v is not None:
            sig_full[i] = sig_line[j] if j < len(sig_line) else None
            j += 1
    hist = [a - b if a is not None and b is not None else None for a, b in zip(macd_line, sig_full)]
    return macd_line, sig_full, hist

# ==========================================
# 認證 endpoints
# ==========================================
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

# ==========================================
# 券商資料庫 endpoints
# ==========================================
@app.get("/api/brokers/tree")
def get_broker_tree():
    """回傳完整券商樹（供前端下拉選單用）"""
    result = {}
    for hq_name, hq_data in UI_TREE.items():
        result[hq_name] = {
            "bid": hq_data["bid"],
            "branches": list(hq_data["branches"].keys())
        }
    return result

@app.get("/api/brokers/map")
def get_broker_map():
    """回傳分點名稱→ID對照（供前端查詢用）"""
    return BROKER_MAP

@app.get("/api/brokers/geo")
def get_geo_map():
    """回傳地緣關鍵字列表"""
    return {loc: list(branches.keys()) for loc, branches in GEO_MAP.items()}

# ==========================================
# 爬蟲 endpoints（TAB1 / TAB2 / TAB3 核心）
# ==========================================
@app.get("/api/broker/stocks")
def broker_stocks(hq_id: str, br_id: str, start: str, end: str, unit: str = "shares"):
    """TAB1/TAB3：特定分點買賣的所有股票"""
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
        # 把名稱中的代號前綴去掉，只留中文名稱
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
    """TAB2：特定股票的所有買賣券商"""
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
        # 清除 NaN/Inf 避免 JSON 序列化錯誤
        df_all = df_all.replace([float('inf'), float('-inf')], 0).fillna(0)
        return df_all.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/stock/broker_history")
def broker_history(sid: str, br_id: str, start: str = "2015-01-01"):
    """TAB4：特定股票 × 分點的歷史買賣超"""
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

    if is_60m:
        # 60m 只能最近60天，用 period1/period2
        now = int(time_mod.time())
        p1 = now - 60 * 86400
        p2 = now + 86400
        range_param = f"period1={p1}&period2={p2}"
    else:
        try:
            start_dt = datetime.datetime.strptime(start, "%Y-%m-%d")
            years = (datetime.datetime.now() - start_dt).days / 365
            range_str = "20y" if years > 10 else "10y" if years > 5 else "5y" if years > 2 else "2y"
        except Exception:
            range_str = "10y"
        range_param = f"range={range_str}"

    # 特殊代號不加後綴
    special = sid.startswith("^") or "=" in sid
    # 台指期嘗試多個 ticker 格式
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
            if not r.ok:
                continue
            j = r.json()
            result = j.get("chart", {}).get("result", [])
            if not result:
                continue
            result = result[0]
            timestamps = result.get("timestamp", [])
            quotes = result.get("indicators", {}).get("quote", [{}])[0]
            meta = result.get("meta", {})
            stock_name = meta.get("longName") or meta.get("shortName") or ticker
            if not timestamps or not quotes:
                continue
            data = []
            for i, ts in enumerate(timestamps):
                c = quotes.get("close", [])[i] if i < len(quotes.get("close", [])) else None
                o = quotes.get("open", [])[i] if i < len(quotes.get("open", [])) else None
                h = quotes.get("high", [])[i] if i < len(quotes.get("high", [])) else None
                l = quotes.get("low", [])[i] if i < len(quotes.get("low", [])) else None
                if c is None or (isinstance(c, float) and math.isnan(c)):
                    continue
                if is_60m:
                    # 60m 回傳 Unix timestamp（前端直接用）
                    data.append({"Date": ts, "Open": o, "High": h, "Low": l, "Close": c})
                else:
                    dt_str = datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
                    if dt_str < start:
                        continue
                    data.append({"Date": dt_str, "Open": o, "High": h, "Low": l, "Close": c})
            if data:
                return {"suffix": ticker, "data": data, "stock_name": stock_name, "interval": interval}
        except Exception:
            continue

    raise HTTPException(404, f"找不到 {sid} 的K線資料（interval={interval}）")

# ==========================================
# VIP 掃描結果
# ==========================================
_vip_cache: dict = {}  # {sheet: {"data": [...], "ts": timestamp}}
_VIP_CACHE_TTL = 300  # 5分鐘

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

# ==========================================
# 最愛清單 CRUD
# ==========================================
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

# ==========================================
# 工作組 CRUD
# ==========================================
@app.get("/api/working_group")
def get_working_group(user: dict = Depends(get_current_user)):
    return sheets_load("WorkingGroup", user["sub"])

@app.post("/api/working_group")
def save_working_group(req: WatchlistSaveRequest, user: dict = Depends(get_current_user)):
    ok = sheets_save("WorkingGroup", user["sub"], req.items)
    if not ok: raise HTTPException(500, "儲存失敗")
    return {"ok": True}

# ==========================================
# 健康檢查（cron-job.org 防冷啟動用）
# ==========================================
@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.datetime.now().isoformat()}

@app.on_event("startup")
async def warmup_cache():
    """啟動時預先暖機 VIP 快取，避免第一次請求太慢"""
    import asyncio
    async def _warm():
        await asyncio.sleep(3)  # 等 GSheets 連線初始化
        try:
            now = datetime.datetime.now().timestamp()
            for sheet in ["ScanResult", "ScanResult_W"]:
                ws = get_worksheet(sheet)
                if ws:
                    data = ws.get_all_records()
                    _vip_cache[sheet] = {"data": data, "ts": now}
                    print(f"[warmup] {sheet}: {len(data)} rows cached")
        except Exception as e:
            print(f"[warmup] failed: {e}")
    asyncio.create_task(_warm())
# ==========================================
# 管理員功能：一鍵重新載入券商清單
# ==========================================
@app.get("/api/admin/reload_brokers")
def reload_brokers():
    """手動觸發重新下載 Google Drive 券商資料，免重啟伺服器"""
    global _HQ_DATA, _RAW_BRANCH, UI_TREE, BROKER_MAP, GEO_MAP
    print("手動重新載入券商資料庫...")
    
    _HQ_DATA = _load_hq()
    _RAW_BRANCH = _load_branches()
    UI_TREE, BROKER_MAP = _build_broker_db(_RAW_BRANCH, _HQ_DATA)
    
    # 重新整理地緣券商
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
# ==========================================
# TAB5：TWSE YoY 年率分析
# 加在 main.py 最後面（@app.get("/") 之前）
# ==========================================

import math as _math

_yoy_cache: dict = {}
_YOY_CACHE_TTL = 3600  # 1小時更新一次（非即時）

@app.get("/api/yoy/twii")
def get_yoy_twii():
    """
    計算台股加權指數各週期 YoY 年率
    週期：年 / 月 / 週 / 日 / 60分鐘
    資料來源：Yahoo Finance ^TWII
    """
    now_ts = datetime.datetime.now().timestamp()
    cached = _yoy_cache.get("twii")
    if cached and (now_ts - cached["ts"]) < _YOY_CACHE_TTL:
        return cached["data"]

    result = {}

    # ── 抓資料 ──────────────────────────────────────
    try:
        # 月線 10年（供年/月週期用）
        mo = yf.download("^TWII", period="10y", interval="1mo", progress=False, auto_adjust=True)
        # 週線 3年
        wk = yf.download("^TWII", period="3y",  interval="1wk", progress=False, auto_adjust=True)
        # 日線 3年
        dy = yf.download("^TWII", period="3y",  interval="1d",  progress=False, auto_adjust=True)
        # 60分鐘 60天（Yahoo 60m 限制）
        hr = yf.download("^TWII", period="60d", interval="60m", progress=False, auto_adjust=True)
    except Exception as e:
        raise HTTPException(500, f"yfinance 下載失敗：{e}")

    # ── 工具函數 ──────────────────────────────────────
    def safe_float(v):
        try:
            f = float(v)
            return None if (_math.isnan(f) or _math.isinf(f)) else f
        except Exception:
            return None

    def group_avg(df, freq):
        """
        df: yfinance DataFrame (index=DatetimeIndex, Close 欄)
        freq: 'YE'/'ME'/'W'/'D'/'h' (pandas resample freq)
        回傳 {key_str: avg_close}
        """
        if df.empty:
            return {}
        # 處理 MultiIndex columns（yfinance multi-ticker 會有）
        close = df["Close"] if "Close" in df.columns else df.iloc[:, 0]
        # 若是 DataFrame 降成 Series
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        close = close.dropna()
        resampled = close.resample(freq).mean().dropna()
        out = {}
        for ts, val in resampled.items():
            v = safe_float(val)
            if v is None:
                continue
            # 產生 key
            if freq in ("YE", "Y", "A", "YS"):
                k = str(ts.year)
            elif freq in ("ME", "M", "MS"):
                k = ts.strftime("%Y-%m")
            elif freq in ("W", "W-SUN", "W-FRI"):
                k = ts.strftime("%Y-W%W")
            elif freq == "D":
                k = ts.strftime("%Y-%m-%d")
            else:  # hourly
                k = ts.strftime("%Y-%m-%dT%H")
            out[k] = v
        return out

    def prev_key(k, unit):
        """給定 key，回傳一年前對應的 key"""
        if unit == "year":
            return str(int(k) - 1)
        if unit == "month":
            y, m = k.split("-")
            return f"{int(y)-1}-{m}"
        if unit == "week":
            y, w = k.split("-W")
            return f"{int(y)-1}-W{w}"
        if unit == "day":
            dt = datetime.datetime.strptime(k, "%Y-%m-%d")
            dt2 = dt.replace(year=dt.year - 1)
            return dt2.strftime("%Y-%m-%d")
        if unit == "hour":
            dt = datetime.datetime.strptime(k, "%Y-%m-%dT%H")
            dt2 = dt.replace(year=dt.year - 1)
            return dt2.strftime("%Y-%m-%dT%H")
        return k

    def fuzzy_prev(avg_map, pk, unit):
        """找不到精確 prev_key 時，嘗試找同月份最近的 key"""
        if pk in avg_map:
            return avg_map[pk]
        prefix = pk[:7]  # YYYY-MM
        for k, v in avg_map.items():
            if k.startswith(prefix):
                return v
        return None

    def calc_yoy(avg_map, unit):
        """
        計算所有時間點的 YoY，回傳最新一筆
        {cur_avg, prev_avg, yoy_pct, key, prev_key}
        """
        keys = sorted(avg_map.keys(), reverse=True)
        for k in keys:
            pk = prev_key(k, unit)
            pv = fuzzy_prev(avg_map, pk, unit)
            if pv and avg_map[k]:
                yoy = (avg_map[k] / pv - 1) * 100
                return {
                    "key": k,
                    "prev_key": pk,
                    "cur_avg": round(avg_map[k], 2),
                    "prev_avg": round(pv, 2),
                    "yoy_pct": round(yoy, 2),
                    "signal": _signal(yoy),
                }
        return None

    def _signal(yoy):
        if yoy is None: return "—"
        if yoy > 20:  return "過熱謹慎"
        if yoy > 10:  return "動能強"
        if yoy > 2:   return "偏多"
        if yoy > -2:  return "中性"
        if yoy > -10: return "偏空"
        if yoy > -20: return "動能弱"
        return "極度超賣"

    # ── 計算各週期 ──────────────────────────────────────
    periods = [
        ("yearly",  mo, "YE",     "year"),
        ("monthly", mo, "ME",     "month"),
        ("weekly",  wk, "W-FRI",  "week"),
        ("daily",   dy, "D",      "day"),
        ("hourly",  hr, "h",      "hour"),
    ]
    labels = {
        "yearly":  "年",
        "monthly": "月",
        "weekly":  "週",
        "daily":   "日",
        "hourly":  "60分鐘",
    }

    period_results = []
    for pid, df, freq, unit in periods:
        avg_map = group_avg(df, freq)
        yoy_data = calc_yoy(avg_map, unit)
        period_results.append({
            "id": pid,
            "label": labels[pid],
            "data": yoy_data,
        })

    # ── 最新收盤價 ──────────────────────────────────────
    latest_price = None
    latest_change = None
    latest_pct = None
    try:
        close_col = dy["Close"] if "Close" in dy.columns else dy.iloc[:, 0]
        if isinstance(close_col, pd.DataFrame):
            close_col = close_col.iloc[:, 0]
        closes = close_col.dropna()
        if len(closes) >= 2:
            latest_price = round(float(closes.iloc[-1]), 2)
            prev_price   = round(float(closes.iloc[-2]), 2)
            latest_change = round(latest_price - prev_price, 2)
            latest_pct    = round((latest_change / prev_price) * 100, 2)
    except Exception:
        pass

    # ── 共識判斷 ──────────────────────────────────────
    yoys = [p["data"]["yoy_pct"] for p in period_results if p["data"]]
    pos = sum(1 for v in yoys if v > 1)
    neg = sum(1 for v in yoys if v < -1)
    total = len(yoys)
    if total and pos >= round(total * 0.7):
        consensus = "多頭"
    elif total and neg >= round(total * 0.7):
        consensus = "空頭"
    else:
        consensus = "分歧"

    data = {
        "latest_price":  latest_price,
        "latest_change": latest_change,
        "latest_pct":    latest_pct,
        "consensus":     consensus,
        "pos_count":     pos,
        "neg_count":     neg,
        "total_count":   total,
        "periods":       period_results,
        "updated_at":    datetime.datetime.now().isoformat(),
    }

    _yoy_cache["twii"] = {"data": data, "ts": now_ts}
    return data    
@app.get("/")
def root():
    return {"app": "stock-radar API", "version": "2.0"}

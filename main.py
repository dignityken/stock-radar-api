"""
stock-radar FastAPI 後端
對應 Streamlit app.py 的所有資料邏輯
"""
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
    dl_url = f"https://drive.google.com/uc?export=download&id={file_id}"
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
        res = requests.get(url, headers=HEADERS, verify=False, timeout=15)
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
        return df_all.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/stock/brokers")
def stock_brokers(sid: str, start: str, end: str):
    """TAB2：特定股票的所有買賣券商"""
    url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zco/zco.djhtm?a={sid}&e={start}&f={end}"
    try:
        res = requests.get(url, headers=HEADERS, verify=False, timeout=15)
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
@app.get("/api/stock/kline")
def stock_kline(sid: str, start: str = "2015-01-01"):
    """TAB4：K線資料（yfinance）"""
    import math
    end = (datetime.date.today() + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    for suffix in [".TW", ".TWO"]:
        ticker = f"{sid}{suffix}"
        try:
            df = yf.download(ticker, start=start, end=end, progress=False,
                             auto_adjust=False, repair=False)
            if df.empty: continue
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [col[0] for col in df.columns]
            df.reset_index(inplace=True)
            if "Date" not in df.columns: continue
            df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None).dt.strftime("%Y-%m-%d")
            df = df.dropna(subset=["Close"])
            if df.empty: continue
            # 清除 NaN/Inf
            for c in ["Open","High","Low","Close"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            df = df.replace([float('inf'), float('-inf')], None).dropna(subset=["Close"])
            cols = [c for c in ["Date","Open","High","Low","Close"] if c in df.columns]
            result = []
            for _, row in df[cols].iterrows():
                rec = {}
                for c in cols:
                    v = row[c]
                    rec[c] = None if (v is None or (isinstance(v, float) and math.isnan(v))) else v
                result.append(rec)
            if result:
                return {"suffix": suffix, "data": result}
        except Exception:
            continue
    raise HTTPException(404, f"找不到 {sid} 的K線資料")

# ==========================================
# VIP 掃描結果
# ==========================================
@app.get("/api/vip/scan")
def vip_scan(sheet: str = "ScanResult", user: dict = Depends(get_current_user)):
    if user.get("role") != "vip":
        raise HTTPException(403, "VIP 限定")
    ws = get_worksheet(sheet)
    if not ws: return []
    try:
        data = ws.get_all_records()
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

@app.get("/")
def root():
    return {"app": "stock-radar API", "version": "2.0"}

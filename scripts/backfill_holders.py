"""往前補歷史大戶持股率：從 money-104 抓每檔約一年的週資料，合併進
data/holders/{sid}.json（與 snapshot_holders.py 同格式，依日期去重）。

★ 本機手動執行。設計原則 = 正當禮貌爬取，不做規避封鎖：
    - 隨機長間隔（預設 3~10 分），像人在慢慢看
    - 瀏覽器式 header + 維持 session cookie
    - 出錯指數退避；連續失敗就停（視為被限流，請晚點再續跑）
    - 每檔抓完即存檔 → 可隨時中斷、重跑自動跳過已完成（resume）
    - 不用代理/IP輪替/破驗證碼（那是繞過防護，本腳本不做）
  全市場跑一次後就靠 snapshot_holders.py（TDCC）自己往後長，不再打擾 money-104。

用法：
    python scripts/backfill_holders.py 4147 1216            # 指定代號
    python scripts/backfill_holders.py --all                # 跑下拉全清單(約2565檔，很久)
    python scripts/backfill_holders.py --all --min-delay 60 --max-delay 180   # 縮短間隔
    python scripts/backfill_holders.py --all --limit 5 --min-delay 2 --max-delay 4   # 測試用
"""
import json, os, re, sys, time, random, argparse
import urllib.request, urllib.error, http.cookiejar, ssl

try:                                  # Windows cp950 終端機也能正常印中文
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTDIR = os.path.join(ROOT, "data", "holders")
BASE = "http://money-104.com/paper_qrystock_analysis_1.php?id={}"
ROW_RE = re.compile(r"id='row_(\d{8})'[^>]*>(.*?)</tr>", re.S)
TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S)
OPT_RE = re.compile(r'<option[^>]*value=["\']([0-9A-Z]{4,6})["\']')
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}
_ctx = ssl.create_default_context(); _ctx.check_hostname = False; _ctx.verify_mode = ssl.CERT_NONE
_opener = urllib.request.build_opener(
    urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()),
    urllib.request.HTTPSHandler(context=_ctx))


def get(url):
    req = urllib.request.Request(url, headers=HEADERS)
    raw = _opener.open(req, timeout=30).read()
    if raw[:3] == b"\xef\xbb\xbf":
        raw = raw[3:]
    return raw.decode("utf-8", "ignore")


def num(x):
    x = re.sub(r"<[^>]+>", "", x).replace("&nbsp;", "").replace(",", "").strip()
    try:
        return float(x)
    except ValueError:
        return 0.0


def parse(html):
    """[0]日期[1]集保總張數[2]總股東[8]>400%[9]>400人數[13]>1000人數[14]>1000%"""
    out = []
    for date, body in ROW_RE.findall(html):
        c = TD_RE.findall(body)
        if len(c) < 15:
            continue
        out.append({"date": date, "total_zhang": int(num(c[1])), "total_people": int(num(c[2])),
                    "b400_pct": round(num(c[8]), 2), "b400_people": int(num(c[9])),
                    "b1000_pct": round(num(c[14]), 2), "b1000_people": int(num(c[13]))})
    return out


def stock_list():
    return list(dict.fromkeys(OPT_RE.findall(get(BASE.format("1216")))))


def existing_weeks(sid):
    fp = os.path.join(OUTDIR, f"{sid}.json")
    if not os.path.exists(fp):
        return 0
    try:
        return len(json.load(open(fp, encoding="utf-8")).get("data", []))
    except Exception:
        return 0


def merge(sid, points):
    os.makedirs(OUTDIR, exist_ok=True)
    fp = os.path.join(OUTDIR, f"{sid}.json")
    rec = {"sid": sid, "data": []}
    if os.path.exists(fp):
        try:
            rec = json.load(open(fp, encoding="utf-8"))
        except Exception:
            pass
    by_date = {p["date"]: p for p in rec.get("data", [])}
    for p in points:
        by_date.setdefault(p["date"], p)   # 不覆蓋既有（TDCC 快照優先）
    rec["sid"] = sid
    rec["data"] = sorted(by_date.values(), key=lambda p: p["date"])
    json.dump(rec, open(fp, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sids", nargs="*")
    ap.add_argument("--file")
    ap.add_argument("--all", action="store_true", help="抓 money-104 下拉全清單")
    ap.add_argument("--limit", type=int, default=0, help="只跑前 N 檔（測試用）")
    ap.add_argument("--min-delay", type=float, default=180.0, help="每檔最短間隔秒(預設180)")
    ap.add_argument("--max-delay", type=float, default=600.0, help="每檔最長間隔秒(預設600)")
    ap.add_argument("--min-weeks", type=int, default=40, help="已有>=此週數視為完成、跳過")
    a = ap.parse_args()

    if a.all:
        print("抓下拉清單..."); sids = stock_list(); print(f"清單 {len(sids)} 檔")
    else:
        sids = list(a.sids)
        if a.file:
            sids += [l.strip() for l in open(a.file, encoding="utf-8") if l.strip()]
    if not sids:
        print("請給代號、--file 或 --all"); sys.exit(1)
    if a.limit:
        sids = sids[:a.limit]

    todo = [s for s in sids if existing_weeks(s) < a.min_weeks]
    skipped = len(sids) - len(todo)
    print(f"待處理 {len(todo)} 檔（已完成跳過 {skipped}）")
    avg = (a.min_delay + a.max_delay) / 2
    print(f"預估耗時 ~ {len(todo)*avg/3600:.1f} 小時（平均 {avg/60:.1f} 分/檔，可隨時中斷續跑）")

    fails = 0
    for i, sid in enumerate(todo, 1):
        try:
            pts = parse(get(BASE.format(sid)))
            if pts:
                merge(sid, pts); fails = 0
                print(f"[{i}/{len(todo)}] {sid}: {len(pts)} weeks OK")
            else:
                print(f"[{i}/{len(todo)}] {sid}: no data")
        except Exception as e:
            fails += 1
            wait = min(60 * fails, 600)
            print(f"[{i}/{len(todo)}] {sid}: FAIL {e} -> backoff {wait}s")
            if fails >= 5:
                print("連續 5 次失敗，停止（可能被限流）。稍後重跑會自動接續。"); break
            time.sleep(wait); continue
        if i < len(todo):
            d = random.uniform(a.min_delay, a.max_delay)
            print(f"    sleep {d/60:.1f} 分...")
            time.sleep(d)
    print("done.")


if __name__ == "__main__":
    main()

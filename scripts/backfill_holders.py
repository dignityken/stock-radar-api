"""往前補歷史大戶持股率：從 money-104 抓每檔約一年的週資料，合併進
data/holders/{sid}.json（與 snapshot_holders.py 同格式，依日期去重）。

★ 本機手動執行，禮貌限速、循序單線、出錯就停——不放 GitHub 自動跑。
   資料源頭仍是 TDCC，money-104 只是已累積好的中介；補完後就靠
   snapshot_holders.py 自己往後長，不再依賴它。

用法：
    python scripts/backfill_holders.py 4147 1216 2330        # 指定代號
    python scripts/backfill_holders.py --file sids.txt        # 一行一個代號
    python scripts/backfill_holders.py 4147 --delay 8         # 自訂延遲秒數(預設5)
"""
import json, os, re, sys, time, argparse
import urllib.request, ssl

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTDIR = os.path.join(ROOT, "data", "holders")
URL = "http://money-104.com/paper_qrystock_analysis_1.php?id={}"
ROW_RE = re.compile(r"id='row_(\d{8})'[^>]*>(.*?)</tr>", re.S)
TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S)


def num(x):
    x = re.sub(r"<[^>]+>", "", x).replace("&nbsp;", "").replace(",", "").strip()
    try:
        return float(x)
    except ValueError:
        return 0.0


def fetch(sid):
    ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(URL.format(sid), headers={"User-Agent": "Mozilla/5.0"})
    raw = urllib.request.urlopen(req, context=ctx, timeout=30).read()
    if raw[:3] == b"\xef\xbb\xbf":
        raw = raw[3:]
    return raw.decode("utf-8", "ignore")


def parse(html):
    """money-104 欄位：[0]日期[1]集保總張數[2]總股東[7]>400張數[8]>400%[9]>400人數[13]>1000人數[14]>1000%"""
    out = []
    for date, body in ROW_RE.findall(html):
        c = TD_RE.findall(body)
        if len(c) < 15:
            continue
        out.append({"date": date, "total_zhang": int(num(c[1])), "total_people": int(num(c[2])),
                    "b400_pct": round(num(c[8]), 2), "b400_people": int(num(c[9])),
                    "b1000_pct": round(num(c[14]), 2), "b1000_people": int(num(c[13]))})
    return out


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
    return len(points)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sids", nargs="*")
    ap.add_argument("--file")
    ap.add_argument("--delay", type=float, default=5.0, help="每檔間隔秒數（禮貌限速）")
    a = ap.parse_args()
    sids = list(a.sids)
    if a.file:
        sids += [l.strip() for l in open(a.file, encoding="utf-8") if l.strip()]
    if not sids:
        print("請給股票代號，或 --file sids.txt"); sys.exit(1)
    fails = 0
    for i, sid in enumerate(sids, 1):
        try:
            pts = parse(fetch(sid))
            if pts:
                merge(sid, pts)
                print(f"[{i}/{len(sids)}] {sid}: {len(pts)} weeks OK")
                fails = 0
            else:
                print(f"[{i}/{len(sids)}] {sid}: 查無資料")
        except Exception as e:
            fails += 1
            print(f"[{i}/{len(sids)}] {sid}: 失敗 {e}")
            if fails >= 3:
                print("連續 3 次失敗，停止（可能被限流，請稍後再試或加大 --delay）"); break
        if i < len(sids):
            time.sleep(a.delay)


if __name__ == "__main__":
    main()

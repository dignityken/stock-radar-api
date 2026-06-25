"""每週快照：抓 TDCC 集保股權分散(當週、全市場)，每檔算大戶率，
合併進 data/holders/{sid}.json（依日期去重）。給 GitHub Action 每週跑。
資料源頭＝TDCC 公開資料。

big400 = 分級>=12 (>400張)，big1000 = 分級15 (>1000張)。
"""
import csv, io, json, os, sys, datetime
import urllib.request, ssl

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTDIR = os.path.join(ROOT, "data", "holders")
URL = "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5"


def fetch():
    ctx = ssl.create_default_context(); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(URL, headers={"User-Agent": "Mozilla/5.0"})
    return urllib.request.urlopen(req, context=ctx, timeout=120).read().decode("utf-8-sig", "ignore")


def parse(text):
    """回傳 {sid: {date,total_zhang,total_people,b400_pct,b400_people,b1000_pct,b1000_people}}"""
    agg = {}
    rd = csv.reader(io.StringIO(text)); next(rd, None)
    for row in rd:
        if len(row) < 6:
            continue
        sid = row[1].strip()
        try:
            L = int(row[2].strip())
        except ValueError:
            continue
        if L < 1 or L > 15:
            continue
        people = int(row[3] or 0); shares = int(row[4] or 0); pct = float(row[5] or 0)
        a = agg.setdefault(sid, {"date": row[0].strip(), "shares": 0, "people": 0,
                                 "b400_pct": 0.0, "b400_people": 0, "b1000_pct": 0.0, "b1000_people": 0})
        a["shares"] += shares; a["people"] += people
        if L >= 12:
            a["b400_pct"] += pct; a["b400_people"] += people
        if L >= 15:
            a["b1000_pct"] += pct; a["b1000_people"] += people
    out = {}
    for sid, a in agg.items():
        out[sid] = {"date": a["date"], "total_zhang": round(a["shares"] / 1000),
                    "total_people": a["people"], "b400_pct": round(a["b400_pct"], 2),
                    "b400_people": a["b400_people"], "b1000_pct": round(a["b1000_pct"], 2),
                    "b1000_people": a["b1000_people"]}
    return out


def merge(sid, point):
    os.makedirs(OUTDIR, exist_ok=True)
    fp = os.path.join(OUTDIR, f"{sid}.json")
    rec = {"sid": sid, "data": []}
    if os.path.exists(fp):
        try:
            rec = json.load(open(fp, encoding="utf-8"))
        except Exception:
            pass
    by_date = {p["date"]: p for p in rec.get("data", [])}
    by_date[point["date"]] = point
    rec["sid"] = sid
    rec["data"] = sorted(by_date.values(), key=lambda p: p["date"])
    rec["updated"] = datetime.datetime.utcnow().isoformat() + "Z"
    json.dump(rec, open(fp, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))


def main():
    snap = parse(fetch())
    if not snap:
        print("no data"); sys.exit(1)
    date = next(iter(snap.values()))["date"]
    for sid, point in snap.items():
        merge(sid, point)
    print(f"snapshot {date}: {len(snap)} stocks updated")


if __name__ == "__main__":
    main()

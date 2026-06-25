"""每週快照：抓 TDCC 集保股權分散(當週、全市場)，每檔算大戶率，
合併進 data/holders/{sid}.json（依日期去重）。給 GitHub Action 每週跑。
資料源頭＝TDCC 公開資料。

big400 = 分級>=12 (>400張)，big1000 = 分級15 (>1000張)。
"""
import csv, io, json, os, sys
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
                                 "pct": [0.0] * 15, "ppl": [0] * 15})
        a["shares"] += shares; a["people"] += people
        a["pct"][L - 1] = pct; a["ppl"][L - 1] = people
    out = {}
    for sid, a in agg.items():
        pct = a["pct"]
        # b{N} = 分級門檻以上占比；t = 全15級占比(往後可任意門檻)
        out[sid] = {"date": a["date"], "total_zhang": round(a["shares"] / 1000),
                    "total_people": a["people"],
                    "b200_pct": round(sum(pct[10:]), 2), "b400_pct": round(sum(pct[11:]), 2),
                    "b1000_pct": round(pct[14], 2),
                    "b400_people": sum(a["ppl"][11:]), "b1000_people": a["ppl"][14],
                    "t": [round(p, 2) for p in pct]}
    return out


def merge(sid, point):
    """寫入該週點；若該日期已存在且資料相同則不動（idempotent，避免無謂 commit）。
    回傳 True 表示有寫入變更。"""
    os.makedirs(OUTDIR, exist_ok=True)
    fp = os.path.join(OUTDIR, f"{sid}.json")
    rec = {"sid": sid, "data": []}
    if os.path.exists(fp):
        try:
            rec = json.load(open(fp, encoding="utf-8"))
        except Exception:
            pass
    by_date = {p["date"]: p for p in rec.get("data", [])}
    if by_date.get(point["date"]) == point:
        return False
    by_date[point["date"]] = point
    rec["sid"] = sid
    rec["data"] = sorted(by_date.values(), key=lambda p: p["date"])
    json.dump(rec, open(fp, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    return True


def main():
    snap = parse(fetch())
    if not snap:
        print("no data"); sys.exit(1)
    date = next(iter(snap.values()))["date"]
    changed = sum(1 for sid, point in snap.items() if merge(sid, point))
    print(f"snapshot {date}: {len(snap)} stocks, {changed} changed")


if __name__ == "__main__":
    main()

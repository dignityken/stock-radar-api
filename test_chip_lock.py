"""籌碼鎖定率：對線上資料源的 smoke test。
獨立執行（不 import main，避免券商DB載入等副作用）：
    python test_chip_lock.py
驗證四個資料源可取得、排行/個股解析正確、鎖定率算得出合理值。
"""
import re, requests, urllib3
urllib3.disable_warnings()
H = {"User-Agent": "Mozilla/5.0"}


def num(x):
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return 0.0


def fetch_json(url):
    r = requests.get(url, headers=H, verify=False, timeout=30); r.encoding = "utf-8"
    return r.json()


def build_ref():
    ref = {}
    for row in fetch_json("https://openapi.twse.com.tw/v1/opendata/t187ap03_L"):
        sid = str(row.get("公司代號", "")).strip()
        ik = next((k for k in row if "已發行" in k), None)
        if sid and ik:
            ref[sid] = {"issued_z": num(row.get(ik)) / 1000, "dir_z": 0.0}
    for row in fetch_json("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"):
        sid = str(row.get("SecuritiesCompanyCode", "")).strip()
        if sid:
            ref[sid] = {"issued_z": num(row.get("IssueShares")) / 1000, "dir_z": 0.0}
    for url in ["https://openapi.twse.com.tw/v1/opendata/t187ap11_L",
                "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap11_O"]:
        for row in fetch_json(url):
            sid = str(row.get("公司代號", "")).strip()
            if sid in ref:
                ref[sid]["dir_z"] += num(row.get("目前持股")) / 1000
    return ref


def parse_ranking(market):
    url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zg/zg_F_{market}_1.djhtm"
    res = requests.get(url, headers=H, verify=False, timeout=25); res.encoding = "big5"
    parts = re.split(r"Link2Stk\('([^']+)'\)", res.text)
    out = {}
    for i in range(1, len(parts) - 1, 2):
        nums = re.findall(r't3n1">&nbsp;([\d,.]+)', parts[i + 1])
        if len(nums) >= 4:
            out[parts[i].strip()] = num(nums[-1])
    return out


if __name__ == "__main__":
    ref = build_ref()
    assert len(ref) > 1500, f"基本資料筆數過少: {len(ref)}"
    rank = parse_ranking("0")
    assert len(rank) >= 40, f"上市排行筆數異常: {len(rank)}"
    matched = [s for s in rank if s in ref]
    assert matched, "排行與基本資料無交集"
    sid = matched[0]
    info = ref[sid]; denom = info["issued_z"] - info["dir_z"]
    rate = rank[sid] / denom * 100
    print(f"ref={len(ref)} 檔, 上市排行={len(rank)} 檔, 配對={len(matched)}")
    print(f"範例 {sid}: 買超{int(rank[sid])}張 / 流通{int(denom)}張 = 鎖定率 {rate:.2f}%")
    assert 0 < rate < 100, f"鎖定率超出合理範圍: {rate}"
    print("OK")

"""
NQ Trading Calendar v2.4
- Monthly dedup (CPI 이틀 표시 버그 수정)
- FOMC Statement 제거
- ff_tz_offset 날짜 보정
- ADP blacklist
- Known Time + FF timezone auto-detection
- Dual alarms: 30min before + 8:30 AM ET
- Fed Chair future-proof
"""

import cloudscraper
from bs4 import BeautifulSoup
from ics import Calendar, Event
from ics.alarm import DisplayAlarm
from datetime import datetime, timedelta, time as dt_time, date
import pytz
import yfinance as yf
import pandas as pd
import re
import time as time_module

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
HKT = pytz.timezone('Asia/Hong_Kong')
ET  = pytz.timezone('US/Eastern')
OUTPUT_FILE = "trading_calendar.ics"

FUTURE_MONTHS = 3
MAX_TIER      = 2
MARKET_PREP_ET = dt_time(8, 30)

EARNINGS_CANDIDATES = ["AAPL", "NVDA", "MSFT", "GOOGL", "AMZN", "META", "TSLA"]
EARNINGS_TOP_N = 3

BLACKLIST = ["adp", "pce"]

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# NQ ESSENTIAL EVENTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EVENTS_DEF = [
    {"match": ["federal funds rate"],
     "group": "fomc", "display": "FOMC Rate Decision",
     "emoji": "🔴", "time_et": (14, 0), "tier": 1},

    {"match": ["fomc press conference"],
     "group": "fomc_pc", "display": "FOMC Press Conference",
     "emoji": "🎙️", "time_et": (14, 30), "tier": 1},

    {"match": ["cpi m/m", "cpi y/y", "core cpi"],
     "group": "cpi", "display": "CPI Release",
     "emoji": "🔥", "time_et": (8, 30), "tier": 1},

    {"match": ["non-farm employment change"],
     "group": "nfp", "display": "NFP + Unemployment",
     "emoji": "💼", "time_et": (8, 30), "tier": 1},

    {"match": ["unemployment rate"],
     "group": "nfp", "display": "NFP + Unemployment",
     "emoji": "💼", "time_et": (8, 30), "tier": 1},

    {"match": ["fed chair"],
     "also_require": ["speaks", "testifies"],
     "group": "fedchair", "display": "Fed Chair Speaks",
     "emoji": "🗣️", "time_et": None, "tier": 1},

    {"match": ["ism services pmi"],
     "group": "ism_svc", "display": "ISM Services PMI",
     "emoji": "⚡", "time_et": (10, 0), "tier": 2},
]

MONTH_MAP = {
    'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
    'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def match_event(name_lower: str):
    for cfg in EVENTS_DEF:
        if cfg["tier"] > MAX_TIER:
            continue
        if any(kw in name_lower for kw in cfg["match"]):
            if "also_require" in cfg:
                if any(kw in name_lower for kw in cfg["also_require"]):
                    return cfg
            else:
                return cfg
    return None


def parse_ff_time(time_str: str):
    s = time_str.strip().lower()
    if not s or 'day' in s or 'tentative' in s:
        return (10, 0, False)
    m = re.match(r'(\d{1,2}):(\d{2})(am|pm)', s)
    if not m:
        return (10, 0, False)
    h, mn = int(m.group(1)), int(m.group(2))
    if m.group(3) == 'pm' and h != 12:
        h += 12
    elif m.group(3) == 'am' and h == 12:
        h = 0
    return (h, mn, True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. FOREXFACTORY SCRAPER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def fetch_forex_events() -> list:
    print("\n🔍 [1] ForexFactory 경제 지표 수집...")
    scraper = cloudscraper.create_scraper()

    now = datetime.now()
    months = []
    cur = date(now.year, now.month, 1)
    end = now.date() + timedelta(days=FUTURE_MONTHS * 31)
    while cur <= end:
        months.append((cur.year, cur.month))
        nxt = date(cur.year, cur.month, 1) + timedelta(days=32)
        cur = date(nxt.year, nxt.month, 1)

    events_map = {}
    scanned = 0
    ff_tz_offset = None

    for page_year, page_month in months:
        label = date(page_year, page_month, 1).strftime("%b.%Y").lower()
        url = f"https://www.forexfactory.com/calendar?month={label}"
        print(f"   📡 {url}")

        try:
            resp = scraper.get(url, timeout=15)
            soup = BeautifulSoup(resp.text, 'html.parser')
            table = soup.find('table', class_='calendar__table')
            if not table:
                print(f"      ⚠️ 테이블 없음")
                continue

            cur_date = None

            for row in table.find_all('tr'):
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 날짜 파싱 (2단계)
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                date_found = False

                # Step 1: calendar__date cell (표준 이벤트 행)
                dc = row.find('td', class_='calendar__date')
                if dc:
                    dt_text = dc.get_text(strip=True)
                    if dt_text:
                        dm = re.search(r'([A-Z][a-z]{2})\s*(\d{1,2})', dt_text)
                        if dm:
                            mn = MONTH_MAP.get(dm.group(1).lower())
                            dn = int(dm.group(2))
                            if mn:
                                yr = page_year
                                if mn == 12 and page_month == 1:
                                    yr = page_year - 1
                                elif mn == 1 and page_month == 12:
                                    yr = page_year + 1
                                try:
                                    cur_date = date(yr, mn, dn)
                                    date_found = True
                                except ValueError:
                                    pass

                # Step 2: date breaker row (calendar__date 없는 날짜 구분 행)
                # 이벤트 데이터가 없는 행에서만 검사 → 오탐 방지
                if not date_found and not row.find('td', class_='calendar__event'):
                    row_text = row.get_text(strip=True)
                    if row_text:
                        dm = re.search(r'([A-Z][a-z]{2})\s*(\d{1,2})', row_text)
                        if dm:
                            mn = MONTH_MAP.get(dm.group(1).lower())
                            if mn:
                                dn = int(dm.group(2))
                                yr = page_year
                                if mn == 12 and page_month == 1:
                                    yr = page_year - 1
                                elif mn == 1 and page_month == 12:
                                    yr = page_year + 1
                                try:
                                    cur_date = date(yr, mn, dn)
                                except ValueError:
                                    pass

                if cur_date is None or cur_date < now.date():
                    continue

                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                # 이벤트 파싱 (이하 동일)
                # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                cc = row.find('td', class_='calendar__currency')
                ec = row.find('td', class_='calendar__event')
                tc = row.find('td', class_='calendar__time')

                if not (cc and ec):
                    continue
                if cc.get_text(strip=True) != 'USD':
                    continue

                event_name = ec.get_text(strip=True)
                event_lower = event_name.lower()

                if any(bl in event_lower for bl in BLACKLIST):
                    continue

                scanned += 1

                cfg = match_event(event_lower)
                if not cfg:
                    continue

                tc_text = tc.get_text(strip=True) if tc else ""
                ff_h, ff_m, ff_ok = parse_ff_time(tc_text)

                if ff_tz_offset is None and cfg["time_et"] is not None and ff_ok:
                    ff_tz_offset = (ff_h - cfg["time_et"][0]) % 24
                    if ff_tz_offset == 0:
                        print(f"   🕐 FF timezone = ET (offset 0h)")
                    else:
                        print(f"   🕐 FF timezone: ET+{ff_tz_offset}h")

                et_date = cur_date

                if cfg["time_et"] is not None:
                    et_h, et_m = cfg["time_et"]
                    if ff_ok:
                        diff = ff_h - et_h
                        if diff < -6:
                            et_date = cur_date - timedelta(days=1)
                    elif ff_tz_offset is not None:
                        if cfg["time_et"][0] + ff_tz_offset >= 24:
                            et_date = cur_date - timedelta(days=1)
                else:
                    tz_off = ff_tz_offset or 0
                    if ff_ok and tz_off > 0:
                        raw_h = ff_h - tz_off
                        et_m = ff_m
                        if raw_h < 0:
                            raw_h += 24
                            et_date = cur_date - timedelta(days=1)
                        et_h = raw_h
                    elif ff_ok:
                        et_h, et_m = ff_h, ff_m
                    else:
                        et_h, et_m = 10, 0

                if cfg["group"] == "fedchair":
                    dedup_key = (et_date, "fedchair")
                    if dedup_key in events_map:
                        continue
                else:
                    dedup_key = (et_date.year, et_date.month, cfg["group"])
                    if dedup_key in events_map:
                        if et_date > events_map[dedup_key]["_et_date"]:
                            del events_map[dedup_key]
                        else:
                            continue

                try:
                    naive = datetime.combine(et_date, dt_time(et_h, et_m))
                    dt_et  = ET.localize(naive)
                    dt_hkt = dt_et.astimezone(HKT)

                    events_map[dedup_key] = {
                        "_et_date": et_date,
                        "name": f"{cfg['emoji']} {cfg['display']}",
                        "begin_hkt": dt_hkt,
                        "begin_et":  dt_et,
                        "tier": cfg["tier"],
                        "ff_name": event_name,
                        "desc": (
                            f"📌 {cfg['display']}\n"
                            f"📋 FF: {event_name}\n"
                            f"⏰ ET: {dt_et.strftime('%Y-%m-%d %I:%M %p %Z')}\n"
                            f"🇭🇰 HKT: {dt_hkt.strftime('%Y-%m-%d %H:%M %Z')}\n"
                            f"📊 Tier {cfg['tier']}"
                        ),
                    }
                except Exception as e:
                    print(f"      ❌ {e}")

            time_module.sleep(0.3)

        except Exception as e:
            print(f"      ❌ {label}: {e}")

    result = sorted(events_map.values(), key=lambda x: x["begin_hkt"])
    print(f"   ✅ {scanned}개 USD 스캔 → {len(result)}개 NQ 핵심 이벤트\n")
    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. BIG TECH EARNINGS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_top_tickers(n=EARNINGS_TOP_N) -> list:
    print(f"🔍 [2] 시가총액 Top {n}...")
    data = []
    for sym in EARNINGS_CANDIDATES:
        try:
            cap = yf.Ticker(sym).fast_info.get('marketCap', 0)
            if cap:
                data.append((sym, cap))
        except Exception:
            pass
    data.sort(key=lambda x: x[1], reverse=True)
    top = [t[0] for t in data[:n]]
    print(f"   ✅ {top}")
    return top


def fetch_earnings(tickers: list) -> list:
    print(f"\n🔍 [3] 실적 발표일 수집... {tickers}")
    results = []

    for sym in tickers:
        try:
            stock = yf.Ticker(sym)
            earn_date = None

            try:
                cal = stock.calendar
                if isinstance(cal, dict) and 'Earnings Date' in cal:
                    dates = cal['Earnings Date']
                    if dates:
                        earn_date = dates[0]
                elif hasattr(cal, 'iloc') and not cal.empty:
                    for v in cal.values.flatten():
                        if isinstance(v, (datetime, pd.Timestamp, date)):
                            earn_date = v
                            break
            except Exception:
                pass

            if earn_date is None:
                try:
                    eds = stock.get_earnings_dates(limit=4)
                    if eds is not None and not eds.empty:
                        future = eds.index[eds.index > datetime.now(pytz.utc)]
                        if not future.empty:
                            earn_date = future[0]
                except Exception:
                    pass

            if earn_date is None:
                print(f"   ⚠️ {sym}: 발표일 없음")
                continue

            if isinstance(earn_date, pd.Timestamp):
                earn_date = earn_date.to_pydatetime()
            if isinstance(earn_date, date) and not isinstance(earn_date, datetime):
                earn_date = datetime.combine(earn_date, dt_time(0, 0))
            if earn_date.tzinfo is None:
                earn_date = pytz.utc.localize(earn_date)

            d = earn_date.astimezone(ET).date()
            dt_et  = ET.localize(datetime.combine(d, dt_time(9, 30)))
            dt_hkt = dt_et.astimezone(HKT)

            results.append({
                "name": f"💰 {sym} Earnings",
                "begin_hkt": dt_hkt,
                "begin_et":  dt_et,
                "tier": 1,
                "ff_name": f"{sym} Earnings",
                "desc": (
                    f"💰 {sym} Earnings\n"
                    f"⏰ ET: {dt_et.strftime('%Y-%m-%d %I:%M %p')}\n"
                    f"🇭🇰 HKT: {dt_hkt.strftime('%Y-%m-%d %H:%M')}"
                ),
            })
            print(f"   ✅ {sym}: {d}")

        except Exception as e:
            print(f"   ❌ {sym}: {e}")

    return results


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. ICS 생성
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def generate_ics(events: list):
    cal = Calendar()

    for evt in events:
        e = Event()
        e.name = evt["name"]
        e.begin = evt["begin_hkt"]
        e.duration = timedelta(minutes=30)
        e.description = evt["desc"]

        e.alarms.append(DisplayAlarm(trigger=timedelta(minutes=-30)))

        prep_et  = ET.localize(
            datetime.combine(evt["begin_et"].date(), MARKET_PREP_ET)
        )
        prep_hkt = prep_et.astimezone(HKT)
        offset   = prep_hkt - evt["begin_hkt"]

        if offset < timedelta(0):
            e.alarms.append(DisplayAlarm(trigger=offset))

        cal.events.add(e)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.writelines(cal.serialize_iter())

    print(f"\n🚀 '{OUTPUT_FILE}' 생성 완료 ({len(events)}개)")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def main():
    forex    = fetch_forex_events()
    top      = get_top_tickers()
    earnings = fetch_earnings(top)

    all_events = sorted(forex + earnings, key=lambda x: x["begin_hkt"])

    print("\n" + "=" * 110)
    print(f"📅 NQ TRADING CALENDAR — {len(all_events)} events")
    print("=" * 110)
    print(f"{'HKT Date':<12} {'HKT':<7} {'ET (date+time)':<17} {'Tier':<5} {'Event':<28} {'FF Name'}")
    print("-" * 110)

    for evt in all_events:
        hkt = evt['begin_hkt']
        et  = evt['begin_et']
        print(
            f"{hkt.strftime('%Y-%m-%d'):<12} "
            f"{hkt.strftime('%H:%M'):<7} "
            f"{et.strftime('%m/%d %I:%M%p'):<17} "
            f"T{evt['tier']:<4} "
            f"{evt['name']:<28} "
            f"{evt.get('ff_name', '')}"
        )

    print("=" * 110)

    generate_ics(all_events)

    fomc = [e for e in all_events if 'FOMC Rate' in e['name']]
    if fomc:
        fe = fomc[0]
        prep = ET.localize(
            datetime.combine(fe['begin_et'].date(), MARKET_PREP_ET)
        ).astimezone(HKT)
        a30 = fe['begin_hkt'] - timedelta(minutes=30)
        print(f"\n🔍 알람 검증 (첫 FOMC):")
        print(f"   이벤트:         {fe['begin_hkt'].strftime('%m/%d %H:%M HKT')}  ({fe['begin_et'].strftime('%m/%d %I:%M%p ET')})")
        print(f"   알람2 (장준비): {prep.strftime('%m/%d %H:%M HKT')}  (8:30AM ET)")
        print(f"   알람1 (30분전): {a30.strftime('%m/%d %H:%M HKT')}")

    print("\n💡 알람: 30분 전 + 8:30AM ET (CPI/NFP는 동시라 30분만)")
    print("⚠️  iPhone: 설정 → 캘린더 → 구독 캘린더 → '알림 제거' OFF")


if __name__ == "__main__":
    main()
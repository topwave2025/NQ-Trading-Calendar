import cloudscraper
from bs4 import BeautifulSoup
from ics import Calendar, Event
from ics.alarm import DisplayAlarm
from datetime import datetime, timedelta, time, date
import pytz
import yfinance as yf
import pandas as pd  # [수정] 누락된 pandas 추가
import re
import time as time_lib

# ==========================================
# 1. 설정 (Configuration)
# ==========================================
HKT = pytz.timezone('Asia/Hong_Kong')
NYC = pytz.timezone('US/Eastern')
OUTPUT_FILE = "trading_calendar.ics"

# [수정] 수집 시작일 지정
START_DATE = datetime(2025, 1, 1)
# 미래 몇 개월까지 수집할지 (오늘 기준)
FUTURE_MONTHS_BUFFER = 3

# ==========================================
# 2. 시가총액 Top 3 자동 선정 (유지)
# ==========================================
def get_top_3_tickers():
    candidates = ["AAPL", "NVDA", "MSFT", "GOOGL", "AMZN", "META", "TSLA"]
    ticker_data = []
    
    print(f"\n🔍 [1] 시가총액 Top 3 계산 중... (후보: {candidates})")
    for symbol in candidates:
        try:
            info = yf.Ticker(symbol).fast_info
            cap = info['marketCap']
            ticker_data.append((symbol, cap))
        except Exception:
            continue
    
    ticker_data.sort(key=lambda x: x[1], reverse=True)
    top_3 = [t[0] for t in ticker_data[:3]]
    print(f"   ✅ 확정된 Top 3: {top_3}")
    return top_3

# ==========================================
# 3. Forex Factory 크롤링 (2025.01 ~ 현재+3개월)
# ==========================================
def fetch_forex_events():
    print(f"\n🔍 [2] 경제 지표(ForexFactory) 수집 중... (2025.01 ~ Future)")
    
    # [핵심] 쿠키를 통해 ForexFactory 시간을 'New York'으로 고정 (Time Zone 3 = EST/EDT)
    # 이렇게 해야 스크립트가 가져오는 텍스트(예: 2:00pm)가 확실히 NY 시간임을 보장받음.
    scraper = cloudscraper.create_scraper()
    scraper.cookies.update({'preferences': 'time_zone=3'}) 
    
    events_list = []
    
    # 날짜 리스트 생성 (2025-01-01 ~ 오늘+3개월)
    target_months = []
    current_cursor = START_DATE
    end_date = datetime.now() + timedelta(days=FUTURE_MONTHS_BUFFER * 30)
    
    while current_cursor <= end_date:
        target_months.append(current_cursor)
        # 다음 달 1일로 이동
        if current_cursor.month == 12:
            current_cursor = current_cursor.replace(year=current_cursor.year + 1, month=1, day=1)
        else:
            current_cursor = current_cursor.replace(month=current_cursor.month + 1, day=1)

    total_checked = 0

    for m_date in target_months:
        month_str = m_date.strftime("%b.%Y").lower() # 예: jan.2025
        url = f"https://www.forexfactory.com/calendar?month={month_str}"
        print(f"   🔗 접속 중: {url} ...", end="\r")
        
        try:
            res = scraper.get(url)
            soup = BeautifulSoup(res.text, 'html.parser')
            table = soup.find('table', class_='calendar__table')
            
            if not table: continue

            rows = table.find_all('tr')
            current_year = m_date.year
            current_date_str = ""
            
            for row in rows:
                # 날짜 파싱
                date_cell = row.find('td', class_='calendar__date')
                if date_cell:
                    text = date_cell.get_text(strip=True)
                    match = re.search(r'([A-Za-z]{3})\s*([0-9]+)', text)
                    if not match:
                        match = re.search(r'[A-Za-z]{3}([A-Za-z]{3})\s*([0-9]+)', text)
                    if match:
                        month_text = match.group(1) if len(match.group(1)) == 3 else match.group(2)
                        day_text = match.group(2) if len(match.group(1)) == 3 else match.group(3)
                        current_date_str = f"{month_text} {day_text}"

                # 이벤트 정보 파싱
                currency_cell = row.find('td', class_='calendar__currency')
                event_cell = row.find('td', class_='calendar__event')
                time_cell = row.find('td', class_='calendar__time')
                
                if not (currency_cell and event_cell and time_cell):
                    continue
                    
                currency = currency_cell.get_text(strip=True)
                if currency != 'USD': continue 
                
                raw_event_name = event_cell.get_text(strip=True)
                event_lower = raw_event_name.lower()
                time_str = time_cell.get_text(strip=True)
                
                total_checked += 1
                
                # 필터링
                is_target = False
                emoji = "🇺🇸"

                # 삭제 대상
                if any(x in event_lower for x in ["minutes", "retail sales", "gdp", "pce", "adp", "manufacturing", "mortgage", "inventories", "bond", "note", "bill"]):
                    continue

                # 포함 대상
                if "fomc" in event_lower or "fed" in event_lower or "federal funds" in event_lower:
                    if "statement" in event_lower: is_target = True; emoji = "📜"
                    elif "federal funds rate" in event_lower: is_target = True; emoji = "📢"
                    elif "press conference" in event_lower: is_target = True; emoji = "🎙️"
                    elif "powell" in event_lower and ("speaks" in event_lower or "testifies" in event_lower):
                        is_target = True; emoji = "🗣️"
                elif "non-farm employment change" in event_lower: is_target = True; emoji = "💼"
                elif "unemployment rate" in event_lower: is_target = True; emoji = "📉"
                elif "cpi" in event_lower: is_target = True; emoji = "🔥"
                elif "ism" in event_lower and "services" in event_lower: is_target = True; emoji = "⚡"

                if not is_target: continue

                # 시간 변환
                if "Day" in time_str or time_str == "" or "Tentative" in time_str:
                    time_str = "8:30am" 
                
                try:
                    # 쿠키로 NY Time을 강제했으므로, 여기서 파싱되는 시간은 무조건 NY Time입니다.
                    dt_str = f"{current_date_str} {current_year} {time_str}"
                    dt_obj_naive = datetime.strptime(dt_str, "%b %d %Y %I:%M%p")
                    
                    # 1. NY Time으로 확정
                    dt_ny = NYC.localize(dt_obj_naive)
                    
                    # 2. HKT로 변환 (이벤트 실제 시간)
                    dt_hkt = dt_ny.astimezone(HKT)
                    
                    # 3. 알람 시간 계산 (이벤트 당일 NY 08:30)
                    # 예: Fed Rate가 NY 14:00 (HKT 익일 03:00)이라도, 알람은 NY 08:30 (HKT 21:30)에 울려야 함.
                    alarm_ny = dt_ny.replace(hour=8, minute=30, second=0, microsecond=0)
                    alarm_hkt = alarm_ny.astimezone(HKT)

                    events_list.append({
                        "name": f"{emoji} {raw_event_name}",
                        "begin": dt_hkt,
                        "begin_ny": dt_ny,
                        "alarm_hkt": alarm_hkt,
                        "alarm_ny": alarm_ny,
                        "description": f"Event: {raw_event_name}\nTime(NY): {dt_ny.strftime('%Y-%m-%d %H:%M')}"
                    })
                except Exception as e:
                    continue
            
            time_lib.sleep(0.2) # 차단 방지 딜레이
            
        except Exception as e:
            print(f"   ❌ 접속 실패 ({month_str}): {e}")
            continue

    print(f"\n   ✅ 총 {total_checked}개 항목 스캔 완료. {len(events_list)}개 정예 이벤트 선택됨.")
    return events_list

# ==========================================
# 4. Big Tech Earnings (에러 수정)
# ==========================================
def fetch_earnings(target_tickers):
    print(f"\n🔍 [3] 기업 실적발표(Earnings) 수집 중... {target_tickers}")
    earnings_list = []
    
    for ticker in target_tickers:
        try:
            stock = yf.Ticker(ticker)
            next_earnings_date = None
            
            # 1. Calendar 속성 확인
            try:
                cal = stock.calendar
                if isinstance(cal, dict) and 'Earnings Date' in cal:
                    dates = cal['Earnings Date']
                    if dates: next_earnings_date = dates[0]
                elif hasattr(cal, 'iloc') and not cal.empty:
                    # DataFrame 처리
                    vals = cal.values.flatten()
                    for v in vals:
                        # [수정] pd.Timestamp 체크 추가
                        if isinstance(v, (datetime, pd.Timestamp, date)):
                            next_earnings_date = v
                            break
            except: pass

            # 2. get_earnings_dates 메서드 확인
            if next_earnings_date is None:
                try:
                    dates = stock.get_earnings_dates(limit=3)
                    if dates is not None and not dates.empty:
                        future = dates.index[dates.index > datetime.now(pytz.utc)]
                        if not future.empty: next_earnings_date = future[0]
                except: pass

            if next_earnings_date:
                # [수정] date 객체일 경우 datetime으로 변환
                if isinstance(next_earnings_date, date) and not isinstance(next_earnings_date, datetime):
                    next_earnings_date = datetime.combine(next_earnings_date, time(0, 0))
                
                # Timestamp -> datetime 변환
                if hasattr(next_earnings_date, 'to_pydatetime'):
                    next_earnings_date = next_earnings_date.to_pydatetime()
                
                # Timezone 처리
                if next_earnings_date.tzinfo is None:
                    next_earnings_date = pytz.utc.localize(next_earnings_date)
                
                # NY 시간 기준 날짜로 변환
                date_ny = next_earnings_date.astimezone(NYC).date()
                
                # 1. 이벤트 시간: NY 11:30 AM (장 시작 2시간 뒤)
                event_ny = NYC.localize(datetime.combine(date_ny, time(11, 30)))
                event_hkt = event_ny.astimezone(HKT)
                
                # 2. 알람 시간: NY 08:30 AM (장 시작 1시간 전)
                alarm_ny = NYC.localize(datetime.combine(date_ny, time(8, 30)))
                alarm_hkt = alarm_ny.astimezone(HKT)

                earnings_list.append({
                    "name": f"💰 {ticker} Earnings",
                    "begin": event_hkt,
                    "begin_ny": event_ny,
                    "alarm_hkt": alarm_hkt,
                    "alarm_ny": alarm_ny,
                    "description": f"Earnings Release: {ticker}\n(Event time set to Market Open + 2h)"
                })
                print(f"   -> {ticker}: {event_hkt.strftime('%Y-%m-%d')}")
            else:
                print(f"   ⚠️ {ticker}: 예정된 발표일 없음")
                
        except Exception as e:
            print(f"   ❌ {ticker} 처리 중 에러: {e}")
            continue
            
    return earnings_list

# ==========================================
# 5. 메인 실행 & 파일 생성
# ==========================================
def main():
    # 1. 데이터 수집
    top_tickers = get_top_3_tickers()
    forex_events = fetch_forex_events()
    earnings_events = fetch_earnings(top_tickers)
    
    all_events = forex_events + earnings_events
    all_events.sort(key=lambda x: x['begin'])

    # 2. 터미널 출력
    print("\n" + "="*100)
    print(f"📅 [최종 확인] 캘린더 이벤트 리스트 ({len(all_events)}개)")
    print("="*100)
    print(f"{'이벤트 시간 (HKT)':<22} | {'알람 시간 (HKT)':<22} | {'이벤트명'}")
    print("-" * 100)
    
    for evt in all_events:
        event_time_str = evt['begin'].strftime("%Y-%m-%d %H:%M")
        alarm_time_str = evt['alarm_hkt'].strftime("%Y-%m-%d %H:%M")
        print(f"{event_time_str:<22} | {alarm_time_str:<22} | {evt['name']}")
        
    print("="*100)

    # 3. ICS 파일 생성
    c = Calendar()
    for item in all_events:
        e = Event()
        e.name = item['name']
        e.begin = item['begin']
        e.duration = timedelta(minutes=60)
        e.description = item['description']
        
        # 알람 트리거 계산 (알람시간 - 이벤트시간)
        trigger_offset = item['alarm_ny'] - item['begin_ny']
        e.alarms.append(DisplayAlarm(trigger=trigger_offset))
        c.events.add(e)
        
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.writelines(c.serialize_iter())
        
    print(f"\n🚀 '{OUTPUT_FILE}' 생성 완료.")
    print("   💡 알람은 미장 시작 1시간 전(NY 08:30)에 울립니다.")
    print("   💡 예: Fed Rate(새벽 3시) -> 알람(전날 저녁 21:30/20:30) 정상 작동.")

if __name__ == "__main__":
    main()
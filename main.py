import cloudscraper
from bs4 import BeautifulSoup
from ics import Calendar, Event
from datetime import datetime, timedelta
import pytz
import yfinance as yf
import re

# ==========================================
# 1. 설정 (Configuration)
# ==========================================
# 홍콩 시간대 설정
HKT = pytz.timezone('Asia/Hong_Kong')
# ForexFactory 기본 시간대 (뉴욕 시간 기준)
NYC = pytz.timezone('US/Eastern')

# 감시할 주식 티커 (유동적으로 변경 가능)
TARGET_TICKERS = ['NVDA', 'AAPL', 'MSFT']

# 캘린더 파일 이름
OUTPUT_FILE = "trading_calendar.ics"

# ==========================================
# 2. 유틸리티 함수
# ==========================================
def get_current_year(date_str):
    """
    날짜 문자열(예: Oct 15)을 받아 현재 시점 기준 적절한 연도를 붙임.
    12월에 내년 1월 데이터를 긁을 때 연도가 바뀌는 것을 처리.
    """
    today = datetime.now()
    try:
        dt = datetime.strptime(f"{date_str} {today.year}", "%b %d %Y")
        # 만약 긁어온 날짜가 오늘보다 10개월 이상 과거라면, 내년 날짜로 간주
        if (today - dt).days > 300:
            dt = dt.replace(year=today.year + 1)
        # 만약 긁어온 날짜가 오늘보다 10개월 이상 미래라면, 작년 날짜로 간주 (거의 없겠지만)
        elif (dt - today).days > 300:
            dt = dt.replace(year=today.year - 1)
        return dt.year
    except:
        return today.year

# ==========================================
# 3. Forex Factory 크롤링
# ==========================================
def fetch_forex_events():
    print("Fetching Forex Factory data...")
    scraper = cloudscraper.create_scraper()
    
    # URL 범위 설정: 오늘부터 +30일
    today = datetime.now()
    end_date = today + timedelta(days=30)
    
    # URL 포맷: range=oct11.2025-jan20.2026 (소문자 월 + 일 + 연도)
    start_str = today.strftime("%b%d.%Y").lower()
    end_str = end_date.strftime("%b%d.%Y").lower()
    url = f"https://www.forexfactory.com/calendar?range={start_str}-{end_str}"
    
    print(f"Target URL: {url}")
    
    try:
        res = scraper.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        table = soup.find('table', class_='calendar__table')
        
        events_list = []
        current_date_str = None
        
        if not table:
            print("Error: Could not find calendar table.")
            return []

        rows = table.find_all('tr')
        
        for row in rows:
            # 날짜 행 처리 (ForexFactory는 날짜가 병합되어 있음)
            date_cell = row.find('td', class_='calendar__date')
            if date_cell:
                text = date_cell.get_text(strip=True)
                if text:
                    # "FriOct 10" 같은 형식을 "Oct 10"으로 추출
                    match = re.search(r'([A-Za-z]{3})\s*([0-9]+)', text) # Month Day
                    if not match: 
                         # 요일+월+일 형태일 수 있음 (Fri Oct 10)
                         match = re.search(r'[A-Za-z]{3}([A-Za-z]{3})\s*([0-9]+)', text)
                    
                    if match:
                        # Oct 10 형태로 저장
                        if len(match.groups()) == 2:
                             # 예: Oct 10
                             month_part = match.group(1) if len(match.group(1)) == 3 else text[-6:-3]
                             day_part = match.group(2)
                             current_date_str = f"{month_part} {day_part}"
                        else:
                             # Fallback
                             current_date_str = text[-6:] # 대충 뒤에서 자름

            # 시간, 통화, 중요도, 이벤트명 추출
            time_cell = row.find('td', class_='calendar__time')
            currency_cell = row.find('td', class_='calendar__currency')
            impact_cell = row.find('td', class_='calendar__impact')
            event_cell = row.find('td', class_='calendar__event')
            
            if not (time_cell and currency_cell and impact_cell and event_cell):
                continue
                
            currency = currency_cell.get_text(strip=True)
            if currency != 'USD': continue # USD만 필터링
            
            event_name = event_cell.get_text(strip=True)
            time_str = time_cell.get_text(strip=True)
            
            # 중요도 판단 (색깔)
            impact_span = impact_cell.find('span')
            impact_class = impact_span['class'][0] if impact_span else ""
            is_high_impact = 'high' in impact_class or 'red' in impact_class
            
            # 필터링 로직
            # 1. Fed Chair는 색깔 상관없이 무조건 포함
            # 2. High Impact(빨강) 포함
            # 3. 특정 키워드 포함
            keywords = ["FOMC", "CPI", "PCE", "Non-Farm", "ISM Services", "GDP"]
            is_keyword_match = any(k in event_name for k in keywords)
            is_fed_chair = "Fed Chair" in event_name
            
            if not (is_high_impact or is_keyword_match or is_fed_chair):
                continue
                
            # 시간 파싱 (All Day 등 제외)
            if "Day" in time_str or time_str == "":
                continue
                
            # 날짜 + 시간 결합
            try:
                year = get_current_year(current_date_str)
                dt_str = f"{current_date_str} {year} {time_str}"
                # ForexFactory 시간은 보통 NY 시간 기준 (US/Eastern)으로 가정하고 파싱
                dt_obj = datetime.strptime(dt_str, "%b %d %Y %I:%M%p")
                dt_obj = NYC.localize(dt_obj) # 뉴욕 시간으로 설정
                dt_hkt = dt_obj.astimezone(HKT) # 홍콩 시간으로 변환
                
                events_list.append({
                    "name": f"🇺🇸 {event_name}",
                    "begin": dt_hkt,
                    "description": f"Impact: {'High' if is_high_impact else 'Medium/Low'}\nSource: ForexFactory"
                })
            except Exception as e:
                # 시간 파싱 에러 시 스킵
                continue
                
        print(f"Found {len(events_list)} Forex events.")
        return events_list
        
    except Exception as e:
        print(f"Error fetching Forex Factory: {e}")
        return []

# ==========================================
# 4. Big Tech Earnings 크롤링 (수정됨)
# ==========================================
def fetch_earnings():
    print("Fetching Earnings data...")
    earnings_list = []
    
    for ticker in TARGET_TICKERS:
        try:
            stock = yf.Ticker(ticker)
            cal = stock.calendar
            
            next_earnings = None

            # Case 1: 최신 yfinance (Dictionary로 반환)
            if isinstance(cal, dict):
                # 'Earnings Date' 키가 있는지 확인
                dates = cal.get('Earnings Date')
                if dates is not None:
                    # 리스트로 들어오면 첫 번째 날짜 선택
                    if isinstance(dates, list) and len(dates) > 0:
                        next_earnings = dates[0]
                    else:
                        next_earnings = dates

            # Case 2: 구버전 yfinance (DataFrame으로 반환)
            elif hasattr(cal, 'empty') and not cal.empty:
                # DataFrame 처리 로직 (혹시 모를 구버전 대비)
                if 'Earnings Date' in cal: # 컬럼에 있을 경우
                    next_earnings = cal['Earnings Date'].iloc[0]
                elif 'Earnings Date' in cal.index: # 인덱스에 있을 경우
                    next_earnings = cal.loc['Earnings Date']

            # 날짜를 찾았으면 이벤트 생성
            if next_earnings:
                # datetime 객체인지 확인 (가끔 date 객체일 수 있음)
                if not isinstance(next_earnings, datetime):
                    # date 객체라면 datetime으로 변환 (시간은 00:00)
                    next_earnings = datetime(next_earnings.year, next_earnings.month, next_earnings.day)

                # 타임존 정보가 없으면 UTC로 가정
                if next_earnings.tzinfo is None:
                    next_earnings = pytz.utc.localize(next_earnings)
                
                # 홍콩 시간 변환
                dt_hkt = next_earnings.astimezone(HKT)
                
                # 실적 발표는 시간이 불명확하므로, 캘린더에는 오전 6시로 고정해서 알림 받기 좋게 설정
                dt_hkt = dt_hkt.replace(hour=6, minute=0, second=0)

                earnings_list.append({
                    "name": f"📊 {ticker} Earnings",
                    "begin": dt_hkt,
                    "description": f"Big Tech Earnings: {ticker}\nCheck specific time (BMO/AMC)."
                })
                print(f"  -> Found {ticker}: {dt_hkt.strftime('%Y-%m-%d')}")
            else:
                print(f"  -> No earnings data found for {ticker}")

        except Exception as e:
            print(f"Error fetching {ticker}: {e}")
            continue
            
    print(f"Found {len(earnings_list)} Earnings events.")
    return earnings_list

# ==========================================
# 5. 메인 실행 및 ICS 생성
# ==========================================
def main():
    # 캘린더 객체 생성
    c = Calendar()
    
    # 데이터 수집
    forex_events = fetch_forex_events()
    earnings_events = fetch_earnings()
    
    all_events = forex_events + earnings_events
    
    # 이벤트 추가
    for item in all_events:
        e = Event()
        e.name = item['name']
        e.begin = item['begin']
        e.duration = timedelta(minutes=60) # 1시간짜리 이벤트로 표시
        e.description = item['description']
        c.events.add(e)
        
    # 파일 쓰기 (덮어쓰기 모드 'w')
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.writelines(c.serialize_iter())
        
    print(f"Successfully created {OUTPUT_FILE} with {len(all_events)} events.")

if __name__ == "__main__":
    main()

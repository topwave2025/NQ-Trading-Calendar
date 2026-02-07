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
# 홍콩 시간대 설정 (사용자 기준)
HKT = pytz.timezone('Asia/Hong_Kong')
# ForexFactory 기본 시간대 (뉴욕 시간 기준)
NYC = pytz.timezone('US/Eastern')

# 캘린더 파일 이름
OUTPUT_FILE = "trading_calendar.ics"

# 과거 데이터 보존 시작일 (History)
HISTORY_START_DATE = "2025-01-01"

# 미래 데이터 수집 기간 (일 단위, 3개월)
FUTURE_DAYS = 90

# ==========================================
# 2. 시가총액 Top 3 자동 선정 (Top 3 Strategy)
# ==========================================
def get_top_3_tickers():
    """
    나스닥 대장주 후보군(M7) 중 현재 시가총액 1, 2, 3위를 실시간으로 추출합니다.
    """
    candidates = ["AAPL", "NVDA", "MSFT", "GOOGL", "AMZN", "META", "TSLA"]
    ticker_data = []
    
    print("🔍 Calculating Market Cap Top 3...")
    for symbol in candidates:
        try:
            # fast_info를 사용하여 빠르게 시총 조회
            info = yf.Ticker(symbol).fast_info
            cap = info['marketCap']
            ticker_data.append((symbol, cap))
        except Exception as e:
            print(f"  Warning: Could not fetch data for {symbol}")
            continue
    
    # 시가총액 내림차순 정렬
    ticker_data.sort(key=lambda x: x[1], reverse=True)
    
    # 상위 3개 티커만 추출
    top_3 = [t[0] for t in ticker_data[:3]]
    print(f"✅ Current Top 3: {top_3}")
    return top_3

# ==========================================
# 3. Forex Factory 크롤링 (엄격한 필터링 적용)
# ==========================================
def fetch_forex_events():
    print("Fetching Forex Factory data...")
    scraper = cloudscraper.create_scraper()
    
    # 날짜 범위 설정: 2025-01-01 ~ 오늘+90일
    start_date_obj = datetime.strptime(HISTORY_START_DATE, "%Y-%m-%d")
    end_date_obj = datetime.now() + timedelta(days=FUTURE_DAYS)
    
    # URL 포맷 (ForexFactory: jan01.2025)
    start_str = start_date_obj.strftime("%b%d.%Y").lower()
    end_str = end_date_obj.strftime("%b%d.%Y").lower()
    
    url = f"https://www.forexfactory.com/calendar?range={start_str}-{end_str}"
    print(f"Target URL: {url}")
    
    try:
        res = scraper.get(url)
        soup = BeautifulSoup(res.text, 'html.parser')
        table = soup.find('table', class_='calendar__table')
        
        if not table:
            print("Error: Could not find calendar table.")
            return []

        events_list = []
        rows = table.find_all('tr')
        
        # 연도 추적을 위한 변수 (초기값은 시작일의 연도)
        current_year = start_date_obj.year
        last_month_idx = -1 # 월이 바뀌는 것을 감지하기 위함 (1~12)
        
        for row in rows:
            # 1. 날짜 파싱 및 연도 계산
            date_cell = row.find('td', class_='calendar__date')
            if date_cell:
                text = date_cell.get_text(strip=True)
                # "FriOct 10" 또는 "Oct 10" 형태
                match = re.search(r'([A-Za-z]{3})\s*([0-9]+)', text)
                if not match:
                    match = re.search(r'[A-Za-z]{3}([A-Za-z]{3})\s*([0-9]+)', text)
                
                if match:
                    month_str = match.group(1) if len(match.group(1)) == 3 else match.group(2) # Oct
                    day_str = match.group(2) if len(match.group(1)) == 3 else match.group(3) # 10
                    
                    # 월 문자열을 숫자로 변환 (Jan=1, Feb=2...)
                    month_idx = datetime.strptime(month_str, "%b").month
                    
                    # 연도 보정 로직:
                    # 12월(12)에서 1월(1)로 넘어가면 연도 +1
                    # (단, 시작 시점이 1월이고 데이터가 1월이면 그대로 유지)
                    if last_month_idx == 12 and month_idx == 1:
                        current_year += 1
                    
                    last_month_idx = month_idx
                    current_date_str = f"{month_str} {day_str}"

            # 2. 데이터 추출
            currency_cell = row.find('td', class_='calendar__currency')
            event_cell = row.find('td', class_='calendar__event')
            time_cell = row.find('td', class_='calendar__time')
            
            if not (currency_cell and event_cell and time_cell):
                continue
                
            currency = currency_cell.get_text(strip=True)
            if currency != 'USD': continue # USD만
            
            raw_event_name = event_cell.get_text(strip=True)
            event_name_lower = raw_event_name.lower()
            time_str = time_cell.get_text(strip=True)

            # 3. [NQ 트레이더 최종 합격 명단] 필터링 로직
            
            # (1) 블랙리스트 (Drop List) - 무조건 제외
            drop_keywords = [
                "adp", "ppi", "pce", "gdp", "minutes", 
                "consumer confidence", "sentiment", "bond auction", "bill auction"
            ]
            if any(bad in event_name_lower for bad in drop_keywords):
                continue

            # (2) 화이트리스트 (Accept List) - 조건부 포함
            is_accepted = False
            
            # A. FOMC Decision & Press Conference
            if "fomc" in event_name_lower:
                if "statement" in event_name_lower or "rate" in event_name_lower or "press conference" in event_name_lower:
                    is_accepted = True
            
            # B. CPI
            elif "cpi" in event_name_lower:
                is_accepted = True
                
            # C. Non-Farm & Unemployment (ADP는 위에서 이미 걸러짐)
            elif "non-farm employment change" in event_name_lower or "unemployment rate" in event_name_lower:
                is_accepted = True
                
            # D. ISM Services PMI
            elif "ism services pmi" in event_name_lower:
                is_accepted = True
                
            # E. Fed Chair Speaks (반드시 의장만)
            elif "fed chair" in event_name_lower and "speaks" in event_name_lower:
                is_accepted = True

            if not is_accepted:
                continue

            # 4. 시간 파싱 및 저장
            if "Day" in time_str or time_str == "":
                continue
                
            try:
                # 날짜 + 연도 + 시간 결합
                dt_str = f"{current_date_str} {current_year} {time_str}"
                dt_obj = datetime.strptime(dt_str, "%b %d %Y %I:%M%p")
                dt_obj = NYC.localize(dt_obj) # 뉴욕 시간
                dt_hkt = dt_obj.astimezone(HKT) # 홍콩 시간
                
                # 이모지 추가
                emoji = "🇺🇸"
                if "fomc" in event_name_lower: emoji = "🏦"
                elif "cpi" in event_name_lower: emoji = "🔥"
                elif "fed chair" in event_name_lower: emoji = "🗣️"
                
                events_list.append({
                    "name": f"{emoji} {raw_event_name}",
                    "begin": dt_hkt,
                    "description": f"Source: ForexFactory\nEvent: {raw_event_name}"
                })
            except Exception:
                continue
                
        print(f"Found {len(events_list)} Valid Economic events.")
        return events_list
        
    except Exception as e:
        print(f"Error fetching Forex Factory: {e}")
        return []

# ==========================================
# 4. Big Tech Earnings 크롤링 (Dynamic Top 3)
# ==========================================
def fetch_earnings(target_tickers):
    print("Fetching Earnings data...")
    earnings_list = []
    
    for ticker in target_tickers:
        try:
            stock = yf.Ticker(ticker)
            cal = stock.calendar
            
            next_earnings = None
            
            # yfinance 버전 호환성 처리
            if isinstance(cal, dict) and 'Earnings Date' in cal:
                dates = cal['Earnings Date']
                if dates:
                    next_earnings = dates[0]
            elif hasattr(cal, 'iloc'): # DataFrame
                 if 'Earnings Date' in cal.index:
                    next_earnings = cal.loc['Earnings Date'].iloc[0]

            # get_earnings_dates()로 재시도 (미래 날짜 탐색)
            if next_earnings is None:
                dates = stock.get_earnings_dates(limit=4)
                if dates is not None and not dates.empty:
                    future_dates = dates.index[dates.index > datetime.now(pytz.utc)]
                    if not future_dates.empty:
                        next_earnings = future_dates[-1] # 가장 가까운 미래

            if next_earnings:
                # 날짜 형식 보정
                if not isinstance(next_earnings, datetime):
                    next_earnings = datetime(next_earnings.year, next_earnings.month, next_earnings.day)
                
                if next_earnings.tzinfo is None:
                    next_earnings = pytz.utc.localize(next_earnings)
                
                dt_hkt = next_earnings.astimezone(HKT)
                # 실적발표는 시간 미정이 많으므로 오전 6시로 고정
                dt_hkt = dt_hkt.replace(hour=6, minute=0, second=0)

                earnings_list.append({
                    "name": f"💰 {ticker} Earnings",
                    "begin": dt_hkt,
                    "description": f"Market Cap Top 3 Earnings: {ticker}"
                })
                print(f"  -> {ticker}: {dt_hkt.strftime('%Y-%m-%d')}")
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")
            continue
            
    return earnings_list

# ==========================================
# 5. 메인 실행
# ==========================================
def main():
    c = Calendar()
    
    # 1. 시총 Top 3 선정
    top_tickers = get_top_3_tickers()
    
    # 2. 데이터 수집
    forex_events = fetch_forex_events()
    earnings_events = fetch_earnings(top_tickers)
    
    all_events = forex_events + earnings_events
    
    # 3. ICS 생성
    for item in all_events:
        e = Event()
        e.name = item['name']
        e.begin = item['begin']
        e.duration = timedelta(minutes=60)
        e.description = item['description']
        c.events.add(e)
        
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.writelines(c.serialize_iter())
        
    print(f"Successfully created {OUTPUT_FILE} with {len(all_events)} events.")

if __name__ == "__main__":
    main()
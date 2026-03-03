import time
from curl_cffi import requests
from datetime import datetime
import pandas as pd
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import random
import re
from bs4 import BeautifulSoup
import os
"""
Phần này dùng cho task 1, dùng để lấy data cần thiết của cafef
"""
# url và các mã sàn của cafef
URL = "https://cafef.vn/du-lieu/ajax/pagenew/databusiness/congtyniemyet.ashx"
EXCHANGES = {"1": "HOSE", "2": "HNX", "9": "UPCOM"}


# Hàm lấy tickers list cho mỗi exchange
def fetch_all_tickers_from_api():
    all_results = { "HOSE": [], "HNX": [], "UPCOM": [] }
    take = 50 
    session = create_session()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Referer": "https://cafef.vn/doanh-nghiep.chn",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest"
    }

    for center_id, exchange_name in EXCHANGES.items():
        skip = 0
        print(f"\n--- Đang lấy dữ liệu sàn {exchange_name} ---")
        
        while True:
            params = {"centerid": center_id, "skip": skip, "take": take, "major": 0}
            
            try:
                # Tăng timeout lên 30 giây để tránh lỗi mạng chậm
                response = session.get(URL, params=params, headers=headers, timeout=30)
                
                if response.status_code != 200:
                    print(f"  ! Lỗi Status {response.status_code}. Thử lại...")
                    continue
                
                result = response.json()
                data_list = result.get("Data", [])
                
                if not data_list:
                    break
                
                for item in data_list:
                    symbol = item.get("Symbol")
                    if symbol:
                        all_results[exchange_name].append(symbol.upper())
                
                print(f"  > Đã lấy được {skip + len(data_list)} mã...")
                
                if len(data_list) < take:
                    break
                    
                skip += take
                
                # NGHỈ NGẪU NHIÊN: Rất quan trọng để không bị coi là Bot
                # Nghỉ từ 1.5 đến 3 giây giữa mỗi request
                time.sleep(random.uniform(1.5, 3.0))

            except Exception as e:
                print(f"  ! Lỗi nghiêm trọng tại skip {skip}: {e}")
                print("  ! Đang tạm nghỉ 10s trước khi thử lại...")
                time.sleep(10)
                continue # Thử lại vị trí bị lỗi thay vì break hẳn
                
    return all_results


# Crawl data ban điều hành của các tickers
def get_board_data(ticker, exchange):
    # URL API của CaféF
    url = f"https://cafef.vn/du-lieu/Ajax/PageNew/ListCeo.ashx?Symbol={ticker}&PositionGroup=0"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://cafef.vn/"
    }

    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            return []

        json_data = response.json()
        results = []
        
        # Lấy thời gian scrape theo chuẩn ISO 8601
        scraped_at = datetime.now().isoformat()

        # DUYỆT QUA TẤT CẢ CÁC NHÓM (HĐQT, Ban Giám đốc, Ban kiểm soát...)
        if "Data" in json_data and json_data["Data"]:
            for group in json_data["Data"]:
                # DUYỆT QUA TẤT CẢ THÀNH VIÊN TRONG NHÓM ĐÓ
                for person in group.get("values", []):
                    # Tạo bản ghi theo đúng Schema yêu cầu
                    row = {
                        "ticker": ticker.upper(),
                        "exchange": exchange, # Vì API không trả về sàn, ta nên truyền vào từ danh sách có sẵn
                        "person_name": person.get("Name"),
                        "role": person.get("Position"),
                        "source": "cafef",
                        "scraped_at": scraped_at
                    }
                    results.append(row)
        
        return results

    except Exception as e:
        print(f"Lỗi khi crawl {ticker}: {e}")
        return []



"""
Phần này cho task 2, phục vụ cho crawl data trang vietstock
"""
# Tạo session để crawl
def create_session():
    session = requests.Session()
    # Cơ chế tự động thử lại nếu gặp lỗi kết nối hoặc mã lỗi 429, 500, 502, 503, 504
    retry = Retry(
        total=5, 
        backoff_factor=2, # Thời gian chờ tăng dần giữa các lần thử: 2s, 4s, 8s...
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# Hàm chuyển data ngày tháng theo format
def parse_microsoft_date(date_str):
    """Hàm bổ trợ để chuyển /Date mới nhất trong trang web vietstock/ sang dd/mm/yyyy"""
    if not date_str or not isinstance(date_str, str):
        return "N/A"
    try:
        # Dùng regex lấy các chữ số bên trong ngoặc ()
        timestamp_ms = int(re.search(r'\d+', date_str).group())
        # Chuyển từ miligiây sang datetime (unit='ms')
        return pd.to_datetime(timestamp_ms, unit='ms').strftime('%d/%m/%Y')
    except:
        return "N/A"
    


# Hàm để lấy data ban điều hành cập nhật mới nhất
def crawl_latest_board(ticker, exchange="HOSE"):
    url_main = f"https://finance.vietstock.vn/{ticker}/ban-lanh-dao.htm"
    url_api = "https://finance.vietstock.vn/data/boarddetails"
    
    session = requests.Session()
    scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_year = datetime.now().year
    
    results = [] # Danh sách chứa các record trả về

    print(f"--- Đang lấy dữ liệu {ticker} ---")
    
    try:
        # Bước 1: Lấy Token
        r_main = session.get(url_main, impersonate="chrome110")
        soup = BeautifulSoup(r_main.text, 'html.parser')
        token_element = soup.find('input', {'name': '__RequestVerificationToken'})
        
        if not token_element:
            print(f"Không lấy được token cho {ticker}")
            return []
        
        token = token_element['value']

        # Bước 2: Gọi API
        payload = {
            'code': ticker,
            'page': 1,
            '__RequestVerificationToken': token
        }
        headers = {
            'User-Agent': 'Mozilla/5.0...',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': url_main,
        }

        response = session.post(url_api, data=payload, headers=headers, impersonate="chrome110")

        if response.status_code == 200:
            json_data = response.json()
            if not json_data:
                return []

            # Lấy kỳ báo cáo gần nhất
            latest_period = json_data[0]
            details = latest_period.get('Details', [])
            
            for person in details:
                # Tính tuổi
                yob = person.get("YearOfBirth")
                age = (current_year - int(yob)) if (yob and str(yob).isdigit()) else None
                
                # Tạo bản ghi theo yêu cầu
                row = {
                    "ticker": ticker.upper(),
                    "exchange": exchange,
                    "person_name": person.get("Name"),
                    "role": person.get("PositionText"),
                    "age": age,
                    "total_shares": person.get("TotalShares"),
                    "time_sticking": person.get("TimeSticking"),
                    "source": "vietstock", 
                    "scraped_at": scraped_at,
                    "closed_date": parse_microsoft_date(latest_period.get('ClosedDate'))
                }
                results.append(row)
                
            print(f"Thành công: Lấy được {len(results)} lãnh đạo.")
            return results
        else:
            print(f"Lỗi API {response.status_code}")
            return []

    except Exception as e:
        print(f"Lỗi khi crawl {ticker}: {e}")
        return []


# Lưu vào thành file parquet
def save_to_parquet(new_data_list, file_path):
    if not new_data_list:
        print("Không có dữ liệu mới để lưu.")
        return

    # 1. Chuyển dữ liệu mới thành DataFrame
    df_new = pd.DataFrame(new_data_list)

    # 2. Kiểm tra nếu file đã tồn tại thì load lên để gộp
    if os.path.exists(file_path):
        df_old = pd.read_parquet(file_path)
        # Gộp dữ liệu cũ và mới
        df_final = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_final = df_new

    # 3. Xử lý trùng lặp
    # Định nghĩa các cột dùng để xác định 1 bản ghi là duy nhất
    subset_cols = ['ticker', 'person_name', 'role', 'closed_date']
    
    # Giữ lại bản ghi đầu tiên xuất hiện (keep='first')
    before_count = len(df_final)
    df_final = df_final.drop_duplicates(subset=subset_cols, keep='first')
    after_count = len(df_final)

    # 4. Tạo thư mục nếu chưa có
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # 5. Lưu xuống file Parquet
    df_final.to_parquet(file_path, index=False, engine='pyarrow')
    
    print(f"--- Kết quả lưu file ---")
    print(f"Tổng số bản ghi mới nhận được: {len(df_new)}")
    print(f"Số bản ghi trùng bị loại bỏ: {before_count - after_count}")
    print(f"Tổng số bản ghi hiện có trong file: {after_count}")
    print(f"Đã lưu tại: {file_path}")

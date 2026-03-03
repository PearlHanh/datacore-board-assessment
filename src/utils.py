import time
import requests
from datetime import datetime
import pandas as pd
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import random
URL = "https://cafef.vn/du-lieu/ajax/pagenew/databusiness/congtyniemyet.ashx"
EXCHANGES = {"1": "HOSE", "2": "HNX", "9": "UPCOM"}


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

# Hàm lấy tickers cho mỗi exchange
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


# Crawl data các tickers 
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



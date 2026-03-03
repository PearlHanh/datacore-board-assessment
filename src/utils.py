import time
import logging
import random
import re
import os
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from unidecode import unidecode
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from curl_cffi import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# --- CẤU HÌNH LOGGING ---
# Ghi đồng thời ra file crawl_process.log và màn hình Console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("../crawl_process.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- HẰNG SỐ ---
URL_CAFEF_TICKERS = "https://cafef.vn/du-lieu/ajax/pagenew/databusiness/congtyniemyet.ashx"
EXCHANGES: Dict[str, str] = {"1": "HOSE", "2": "HNX", "9": "UPCOM"}


def create_session() -> requests.Session:
    """
    Tạo một session với cơ chế tự động thử lại (Retry) khi gặp lỗi kết nối.

    Returns:
        requests.Session: Session đã được cấu hình HTTPAdapter và Retry.
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


"""
Phần này dùng cho task 1, dùng để lấy data cần thiết của cafef
"""

def fetch_all_tickers_from_api() -> Dict[str, List[str]]:
    """
    Lấy danh sách tất cả mã chứng khoán (tickers) từ API của CaféF theo từng sàn.

    Returns:
        Dict[str, List[str]]: Dictionary chứa danh sách mã theo sàn (HOSE, HNX, UPCOM).
    """
    all_results: Dict[str, List[str]] = {"HOSE": [], "HNX": [], "UPCOM": []}
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
        logger.info(f"--- Đang lấy dữ liệu sàn {exchange_name} ---")
        
        while True:
            params = {"centerid": center_id, "skip": skip, "take": take, "major": 0}
            
            try:
                response = session.get(URL_CAFEF_TICKERS, params=params, headers=headers, timeout=30)
                
                if response.status_code != 200:
                    logger.warning(f"Lỗi Status {response.status_code}. Đang thử lại tại skip {skip}...")
                    continue
                
                result = response.json()
                data_list = result.get("Data", [])
                
                if not data_list:
                    break
                
                for item in data_list:
                    symbol = item.get("Symbol")
                    if symbol:
                        all_results[exchange_name].append(symbol.upper())
                
                logger.info(f"  > {exchange_name}: Đã lấy được {skip + len(data_list)} mã...")
                
                if len(data_list) < take:
                    break
                    
                skip += take
                time.sleep(random.uniform(1.5, 3.0))

            except Exception as e:
                logger.error(f"Lỗi nghiêm trọng tại skip {skip}: {e}", exc_info=True)
                logger.info("Đang tạm nghỉ 10s trước khi thử lại...")
                time.sleep(10)
                continue 
                
    return all_results


def get_board_data(ticker: str, exchange: str) -> List[Dict[str, Any]]:
    """
    Crawl danh sách ban lãnh đạo của một công ty từ CaféF.

    Args:
        ticker (str): Mã chứng khoán (VD: 'VNM').
        exchange (str): Tên sàn tương ứng.

    Returns:
        List[Dict[str, Any]]: Danh sách các bản ghi thông tin lãnh đạo.
    """
    url = f"https://cafef.vn/du-lieu/Ajax/PageNew/ListCeo.ashx?Symbol={ticker}&PositionGroup=0"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://cafef.vn/"
    }

    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logger.warning(f"Caféf {ticker} - Lỗi Status {response.status_code}")
            return []

        json_data = response.json()
        results: List[Dict[str, Any]] = []
        scraped_at = datetime.now().isoformat()

        if "Data" in json_data and json_data["Data"]:
            for group in json_data["Data"]:
                for person in group.get("values", []):
                    results.append({
                        "ticker": ticker.upper(),
                        "exchange": exchange,
                        "person_name": person.get("Name"),
                        "role": person.get("Position"),
                        "source": "cafef",
                        "scraped_at": scraped_at
                    })
        return results

    except Exception as e:
        logger.error(f"Lỗi khi crawl CaféF cho {ticker}: {e}")
        return []


"""
Phần này cho task 2, phục vụ cho crawl data trang vietstock
"""

def parse_microsoft_date(date_str: Optional[str]) -> str:
    """
    Chuyển đổi chuỗi ngày tháng dạng Microsoft JSON (/Date(...)/) sang dd/mm/yyyy.

    Args:
        date_str (Optional[str]): Chuỗi ngày thô từ API Vietstock.

    Returns:
        str: Ngày định dạng dd/mm/yyyy hoặc "N/A" nếu lỗi.
    """
    if not date_str or not isinstance(date_str, str):
        return "N/A"
    try:
        timestamp_ms = int(re.search(r'\d+', date_str).group())
        return pd.to_datetime(timestamp_ms, unit='ms').strftime('%d/%m/%Y')
    except Exception:
        return "N/A"


def crawl_latest_board(ticker: str, exchange: str = "HOSE") -> List[Dict[str, Any]]:
    """
    Crawl thông tin ban lãnh đạo chi tiết từ Vietstock.

    Args:
        ticker (str): Mã chứng khoán.
        exchange (str): Tên sàn (mặc định HOSE).

    Returns:
        List[Dict[str, Any]]: Danh sách thông tin lãnh đạo bao gồm cả tuổi và tỷ lệ sở hữu.
    """
    url_main = f"https://finance.vietstock.vn/{ticker}/ban-lanh-dao.htm"
    url_api = "https://finance.vietstock.vn/data/boarddetails"
    
    session = requests.Session()
    scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_year = datetime.now().year
    results: List[Dict[str, Any]] = []

    logger.info(f"--- Đang lấy dữ liệu Vietstock: {ticker} ---")
    
    try:
        r_main = session.get(url_main, impersonate="chrome110")
        soup = BeautifulSoup(r_main.text, 'html.parser')
        token_element = soup.find('input', {'name': '__RequestVerificationToken'})
        
        if not token_element:
            logger.error(f"Không lấy được token Vietstock cho {ticker}")
            return []
        
        token = token_element['value']
        payload = {'code': ticker, 'page': 1, '__RequestVerificationToken': token}
        headers = {'X-Requested-With': 'XMLHttpRequest', 'Referer': url_main}

        response = session.post(url_api, data=payload, headers=headers, impersonate="chrome110")

        if response.status_code == 200:
            json_data = response.json()
            if not json_data:
                logger.warning(f"Vietstock {ticker}: Không có dữ liệu.")
                return []

            latest_period = json_data[0]
            details = latest_period.get('Details', [])
            
            for person in details:
                yob = person.get("YearOfBirth")
                age = (current_year - int(yob)) if (yob and str(yob).isdigit()) else None
                
                results.append({
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
                })
            logger.info(f"Thành công {ticker}: Lấy được {len(results)} lãnh đạo.")
            return results
        else:
            logger.error(f"Lỗi API Vietstock {ticker}: Status {response.status_code}")
            return []

    except Exception as e:
        logger.error(f"Lỗi khi crawl Vietstock {ticker}: {e}", exc_info=True)
        return []


def save_to_parquet(new_data_list: List[Dict[str, Any]], file_path: str) -> None:
    """
    Lưu dữ liệu vào file Parquet, tự động gộp với dữ liệu cũ và loại bỏ trùng lặp.

    Args:
        new_data_list (List[Dict[str, Any]]): Dữ liệu mới crawl được.
        file_path (str): Đường dẫn đến file Parquet.
    """
    if not new_data_list:
        logger.warning("Không có dữ liệu mới để lưu.")
        return

    df_new = pd.DataFrame(new_data_list)

    if os.path.exists(file_path):
        df_old = pd.read_parquet(file_path)
        df_final = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_final = df_new

    # Xử lý trùng lặp dựa trên các cột định danh
    subset_cols = ['ticker', 'person_name', 'role', 'closed_date']
    actual_subset = [c for c in subset_cols if c in df_final.columns]
    
    before_count = len(df_final)
    df_final = df_final.drop_duplicates(subset=actual_subset, keep='first')
    after_count = len(df_final)

    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df_final.to_parquet(file_path, index=False, engine='pyarrow')
    
    logger.info("--- KẾT QUẢ LƯU FILE ---")
    logger.info(f"Mới: {len(df_new)} | Loại trùng: {before_count - after_count} | Tổng file: {after_count}")
    logger.info(f"Đường dẫn: {file_path}")


"""
Phần cho task 3, merge 2 dataset
"""

def clean_honorific_only(name: Optional[str]) -> Optional[str]:
    """
    Loại bỏ các danh xưng 'Ông' hoặc 'Bà' ở đầu tên.

    Args:
        name (Optional[str]): Tên đầy đủ kèm danh xưng.

    Returns:
        Optional[str]: Tên đã được loại bỏ danh xưng.
    """
    if not name or not isinstance(name, str):
        return name
    pattern = r'^(?:(?:[Ôô]ng|[Bb]à)[\s\.]+)+'
    return re.sub(pattern, '', name).strip()


def remap_role_vietstock(role_vs: Optional[str]) -> str:
    """
    Chuyển đổi các chức danh viết tắt từ Vietstock sang dạng đầy đủ.
    Ví dụ: "TVHĐQT/TGĐ" -> "Thành viên HĐQT / Tổng Giám đốc"
    
    Args:
        role_vs (Optional[str]): Chuỗi chức danh viết tắt.
        
    Returns:
        str: Chuỗi chức danh đầy đủ.
    """
    if not role_vs or role_vs == '***':
        return "Khác"

    # 1. Dictionary mapping các từ viết tắt
    component_map = {
        "CTHĐQT": "Chủ tịch HĐQT",
        "Phó CTHĐQT": "Phó Chủ tịch HĐQT",
        "TVHĐQT": "Thành viên HĐQT",
        "TGĐ": "Tổng Giám đốc",
        "Phó TGĐ": "Phó Tổng GĐ",
        "GĐ": "Giám đốc",
        "Phó GĐ": "Phó Giám đốc",
        "KTT": "Kế toán trưởng",
        "Thường trực": "thường trực",
        "UBKTNB": "Ban kiểm toán nội bộ",
        "TCKT": "Tài chính Kế toán",
        "TV": "Thành viên",
        "UB": "Ủy ban",
        "Phụ trách Quản trị": "Phụ trách quản trị",
        "Thư ký Công ty": "Thư ký công ty"
    }

    # Sắp xếp các key theo độ dài giảm dần (quan trọng!)
    # Để tránh việc thay thế "TV" trước khi thay thế "TVHĐQT"
    sorted_abbrs = sorted(component_map.keys(), key=len, reverse=True)

    # 2. Xử lý các chức danh ghép (Ngăn cách bởi dấu /)
    parts = role_vs.split('/')
    mapped_parts = []

    for part in parts:
        temp_part = part.strip()
        
        # Duyệt qua danh sách viết tắt để thay thế
        for abbr in sorted_abbrs:
            # Sử dụng re.sub với \b (word boundary) để khớp chính xác cụm từ
            # Ví dụ: chỉ thay "TV" khi nó đứng độc lập, không nằm trong "TVHĐQT"
            full_text = component_map[abbr]
            temp_part = re.sub(rf'\b{abbr}\b', full_text, temp_part)
            
        mapped_parts.append(temp_part)

    # 3. Kết nối lại bằng dấu " / " cho thoáng hoặc " kiêm "
    # Vietstock thường dùng / để liệt kê nhiều chức danh cùng lúc
    return " / ".join(mapped_parts)





# Join 2 bảng
def normalize_string(text: Optional[str]) -> str:
    if not text or not isinstance(text, str):
        return ""
    text_norm = unidecode(text).lower()
    titles = [r'\bts\b', r'\bths\b', r'\bgs\b', r'\bpgs\b', r'\bks\b', r'\bdr\b']
    for t in titles:
        text_norm = re.sub(t, '', text_norm)
    return re.sub(r'[^a-z0-9]', '', text_norm)

def assess_data_quality(row: Dict[str, Any]) -> str:
    """
    Đánh giá chất lượng dữ liệu (Data Quality) dựa trên mức độ đầy đủ của thông tin.
    """
    essential_fields = ['ticker', 'exchange', 'person_name', 'role']
    missing_essentials = [f for f in essential_fields if not row.get(f)]
    
    if row['source_agreement'] == 'conflict':
        return "Needs Verification (Conflict)"
    
    if len(missing_essentials) > 0:
        return "Low (Missing Essentials)"
    
    # Kiểm tra các trường bổ trợ (chỉ có ở VS)
    supplementary_fields = ['age', 'total_shares', 'time_sticking']
    missing_supps = [f for f in supplementary_fields if pd.isna(row.get(f)) or row.get(f) is None]
    
    if row['source_agreement'] == 'both' and not missing_supps:
        return "Perfect (Full Data)"
    elif row['source_agreement'] == 'both':
        return "Good (Matched but Partial supplementary)"
    
    return "Acceptable (Single Source)"

def merge_to_golden_dataset(df_vs: pd.DataFrame, df_cf: pd.DataFrame) -> pd.DataFrame:
    logger.info("Bắt đầu quy trình xây dựng Golden Dataset...")

    df_vs = df_vs.copy()
    df_cf = df_cf.copy()

    for df in [df_vs, df_cf]:
        df['ticker'] = df['ticker'].str.strip().str.upper()
        df['exchange'] = df['exchange'].str.strip().str.upper()

    # 2. Tạo Join Key
    df_vs['join_key'] = df_vs['ticker'] + "_" + df_vs['exchange'] + "_" + df_vs['person_name'].apply(normalize_string)
    df_cf['join_key'] = df_cf['ticker'] + "_" + df_cf['exchange'] + "_" + df_cf['person_name'].apply(normalize_string)

    # 3. Outer Join
    merged = pd.merge(df_vs, df_cf, on='join_key', how='outer', suffixes=('_vs', '_cf'))

    # 4. Xử lý logic Golden và Data Quality
    golden_list: List[Dict[str, Any]] = []

    for _, row in merged.iterrows():
        has_vs = pd.notna(row.get('ticker_vs'))
        has_cf = pd.notna(row.get('ticker_cf'))
        
        # 3a. Select best-available
        res = {
            "ticker": row['ticker_cf'] if has_cf else row['ticker_vs'],
            "exchange": row['exchange_cf'] if has_cf else row['exchange_vs'],
            "person_name": row['person_name_cf'] if has_cf else row['person_name_vs'],
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        res['total_shares'] = row.get('total_shares') if has_vs else None
        res['age'] = row.get('age') if has_vs else None
        res['time_sticking'] = row.get('time_sticking') if has_vs else None

        # 3b. Conflict Resolution & Confidence Score
        if has_vs and has_cf:
            res['role'] = row['role_cf'] if pd.notna(row['role_cf']) else row['role_vs']
            role_vs = str(row.get('role_vs', '')).strip().lower()
            role_cf = str(row.get('role_cf', '')).strip().lower()
            
            if role_vs != role_cf:
                res['source_agreement'] = "conflict"
                res['confidence_score'] = 0.85
            else:
                res['source_agreement'] = "both"
                res['confidence_score'] = 1.0
        elif has_cf:
            res['role'] = row['role_cf']
            res['source_agreement'] = "cafef_only"
            res['confidence_score'] = 0.75
        else:
            res['role'] = row['role_vs']
            res['source_agreement'] = "vietstock_only"
            res['confidence_score'] = 0.8

        # --- BỔ SUNG DATA QUALITY COLUMN ---
        res['data_quality'] = assess_data_quality(res)
        golden_list.append(res)

    df_golden = pd.DataFrame(golden_list)
    
    # Sắp xếp cột
    ordered_cols = [
        'ticker', 'exchange', 'person_name', 'role', 'age', 
        'total_shares', 'time_sticking', 'source_agreement', 
        'confidence_score', 'data_quality', 'scraped_at'
    ]
    return df_golden[ordered_cols]

# --- 3c. OUTPUT REPORTS ---

def generate_reports(df: pd.DataFrame, vs_len: int, cf_len: int):
    """
    Xuất tập dữ liệu Golden và tự động tạo báo cáo Quality Report & Data Dictionary.
    Hệ thống tự động xử lý các giá trị 'NaN' giả để tính toán Null Rate chính xác.
    """
    # --- BƯỚC 0: CHUẨN HÓA GIÁ TRỊ NULL THỰC TẾ ---
    # Chuyển tất cả các dạng chuỗi "NaN", "None", hoặc trống về giá trị Null của hệ thống (np.nan)
    # Điều này cực kỳ quan trọng để hàm .isnull() đếm chính xác
    df = df.replace(['NaN', 'nan', 'None', 'none', '', None], np.nan)

    # --- BƯỚC 1: LƯU FILE PARQUET (Requirement 3c) ---
    parquet_path = "../data/final/golden_board.parquet"
    df.to_parquet(parquet_path, index=False)
    logger.info(f"Đã lưu Golden Parquet tại: {parquet_path}")
    
    # --- BƯỚC 2: PHÂN TÍCH UNMATCHED & CONFLICTS ---
    unmatched_df = df[df['source_agreement'].isin(['vietstock_only', 'cafef_only'])]
    common_unmatched = unmatched_df['person_name'].value_counts().head(10).to_markdown()

    short_names_count = len(unmatched_df[unmatched_df['person_name'].str.len() < 5])
    special_char_names = len(unmatched_df[unmatched_df['person_name'].str.contains(r'[^\w\s]', regex=True, na=False)])

    conflict_df = df[df['source_agreement'] == 'conflict']
    common_role_conflicts = conflict_df['role'].value_counts().head(5).to_markdown() if not conflict_df.empty else "No major conflicts."

    # --- BƯỚC 3: TẠO NỘI DUNG DATA QUALITY REPORT (Requirement 3c) ---
    match_count = len(df[df['source_agreement'].isin(['both', 'conflict'])])
    total_golden = len(df)
    conflict_rate = (len(conflict_df) / match_count * 100) if match_count > 0 else 0
    
    report_md = f"""# Data Quality Report: Board Golden Dataset
**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 1. Summary Metrics
- **Total Golden Records:** {total_golden}
- **Vietstock Raw Count:** {vs_len}
- **CafeF Raw Count:** {cf_len}
- **Match Rate:** {(match_count / max(vs_len, cf_len) * 100):.2f}%
- **Conflict Rate:** {conflict_rate:.2f}% (Role disagreement)

## 2. Agreement Distribution
{df['source_agreement'].value_counts().to_markdown()}

## 3. Data Quality Breakdown
{df['data_quality'].value_counts().to_markdown()}

## 4. Most Common Unmatched Names
*Những tên xuất hiện trong một nguồn nhưng không tìm thấy ở nguồn kia (Top 10):*
{common_unmatched}

## 5. Observed Patterns & Issues
### A. Unmatched Patterns
- **Short Names:** Có {short_names_count} tên không khớp có độ dài dưới 5 ký tự (Dấu hiệu viết tắt hoặc dữ liệu rác).
- **Special Characters:** Có {special_char_names} tên không khớp chứa ký tự đặc biệt.
- **Coverage Gap:** Nguồn CafeF cập nhật Ban Điều Hành hàng ngày, trong khi Vietstock cập nhật định kỳ (thường theo BCTC 6 tháng/năm), dẫn đến độ trễ trong việc khớp các bổ nhiệm mới.

### B. Conflict Patterns
*Các chức danh bị xung đột dữ liệu:*
{common_role_conflicts}
"""
    with open("../docs/data_quality_report.md", "w", encoding="utf-8") as f:
        f.write(report_md)

    # --- BƯỚC 4: TẠO DATA DICTIONARY VỚI NULL RATE THỰC TẾ (Requirement 3d) ---
    descriptions = {
        'ticker': 'Mã chứng khoán niêm yết.',
        'exchange': 'Sàn giao dịch (HOSE, HNX, UPCOM).',
        'person_name': 'Họ tên lãnh đạo (Đã làm sạch danh xưng Ông/Bà).',
        'role': 'Chức vụ hiện tại (Ưu tiên dữ liệu từ CafeF).',
        'age': 'Tuổi của lãnh đạo (Nguồn: Vietstock).',
        'total_shares': 'Số cổ phiếu nắm giữ cá nhân (Nguồn: Vietstock).',
        'time_sticking': 'Thời gian đảm nhiệm/Thâm niên (Nguồn: Vietstock).',
        'source_agreement': 'Trạng thái đồng nhất: both, conflict, cafef_only, vietstock_only.',
        'confidence_score': 'Điểm tin cậy dữ liệu (0.7 - 1.0).',
        'data_quality': 'Phân loại mức độ đầy đủ của bản ghi.',
        'scraped_at': 'Thời điểm tạo bản ghi Golden.'
    }

    dict_md = f"# Data Dictionary: Board Golden Dataset\n\n"
    dict_md += "| Field | Type | Description | Null Rate | Caveats |\n"
    dict_md += "| :--- | :--- | :--- | :--- | :--- |\n"

    for col in df.columns:
        dtype = str(df[col].dtype)
        
        # TÍNH TOÁN NULL RATE THỰC TẾ
        null_count = df[col].isnull().sum()
        rate_val = (null_count / total_golden * 100) if total_golden > 0 else 0
        null_rate_str = f"{rate_val:.1f}%"
        
        desc = descriptions.get(col, "-")
        
        # Tự động đưa ra cảnh báo dựa trên Null Rate
        caveat = "-"
        if rate_val > 40:
            caveat = "⚠️ Tỷ lệ trống cao do thông tin này chỉ có ở một nguồn (Vietstock)."
        elif col == 'role':
            caveat = "Dữ liệu conflict sẽ ưu tiên giá trị của CafeF."

        dict_md += f"| **{col}** | `{dtype}` | {desc} | {null_rate_str} | {caveat} |\n"

    with open("../docs/data_dictionary.md", "w", encoding="utf-8") as f:
        f.write(dict_md)
    
    logger.info("Đã hoàn tất toàn bộ báo cáo và từ điển dữ liệu.")
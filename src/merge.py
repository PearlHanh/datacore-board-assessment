import pandas as pd
import os
import logging
from utils import (
    clean_honorific_only, 
    remap_role_vietstock, 
    merge_to_golden_dataset, 
    generate_reports
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Lữu dữu liệu vào processed
def save_incremental_processed(df_new: pd.DataFrame, file_path: str):
    """
    Lưu dữ liệu vào file Parquet theo phương thức cộng dồn (incremental).
    Chỉ giữ lại các bản ghi mới không trùng lặp dựa trên Ticker, Exchange, Name và Role.
    """
    # Các cột định danh để xác định trùng lặp
    subset_cols = ['ticker', 'exchange', 'person_name', 'role']
    
    if os.path.exists(file_path):
        logger.info(f"Đang đọc dữ liệu cũ để gộp: {file_path}")
        df_old = pd.read_parquet(file_path)
        
        # Gộp dữ liệu cũ và mới (đưa cái mới lên đầu để ưu tiên nếu có logic khác)
        df_final = pd.concat([df_new, df_old], ignore_index=True)
        
        # Loại bỏ trùng lặp
        before = len(df_final)
        df_final = df_final.drop_duplicates(subset=subset_cols, keep='first')
        after = len(df_final)
        
        logger.info(f"Đã loại bỏ {before - after} bản ghi trùng. Thêm mới {after - len(df_old)} bản ghi.")
    else:
        logger.info(f"Tạo mới file dữ liệu processed tại: {file_path}")
        df_final = df_new

    # Đảm bảo thư mục tồn tại và lưu file
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df_final.to_parquet(file_path, index=False, engine='pyarrow')


# --- LUỒNG CHÍNH ---

# 1. Load data raw
logger.info("Đang tải dữ liệu thô từ folder raw...")
data_cafef = pd.read_parquet("../data/raw/cafef_board.parquet")
data_vietstock = pd.read_parquet("../data/raw/vietstock_board.parquet")

# 2. Xử lý làm sạch dữ liệu
logger.info("Đang thực hiện làm sạch tên và chuẩn hóa chức vụ...")
data_cafef["person_name"] = data_cafef["person_name"].apply(clean_honorific_only)
data_vietstock["person_name"] = data_vietstock["person_name"].apply(clean_honorific_only)

# Mapping lại role của vietstock do viết tắt
data_vietstock["role"] = data_vietstock["role"].apply(remap_role_vietstock)

# 3. Lưu dữ liệu đã xử lý (Incremental) trước khi merge
logger.info("Đang lưu dữ liệu đã xử lý vào folder processed...")
save_incremental_processed(data_cafef, "../data/processed/processed_cafef.parquet")
save_incremental_processed(data_vietstock, "../data/processed/processed_vietstock.parquet")

# 4. Merge 2 bảng thành golden data
logger.info("Đang tiến hành gộp dữ liệu thành Golden Dataset...")
df_golden = merge_to_golden_dataset(data_vietstock, data_cafef)

# 5. Xuất báo cáo và lưu file Golden cuối cùng
generate_reports(df_golden, len(data_vietstock), len(data_cafef))

logger.info("Hoàn tất toàn bộ quy trình.")
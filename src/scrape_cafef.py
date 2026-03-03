# import requests
# from datetime import datetime
# import pandas as pd
# import json
# import time
# from utils import get_board_data
# import os
# # Đọc dữ liệu tickers từ file json đã lưu
# json_path = "../data/raw/all_tickers.json"

# with open(json_path, "r", encoding="utf-8") as f:
#     all_tickers = json.load(f)

# result = []
# for exchange, ticker_list in all_tickers.items():
#     for ticker in ticker_list[:30]:
#         result.append(get_board_data(ticker, exchange))
#         print(f"Đã xong ", ticker)
#         time.sleep(1)                   


# new_data = pd.DataFrame(result)
# parquet_path = "../data/raw/cafef_board2.parquet"
# os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

# if os.path.exists(parquet_path):
#         # Đọc dữ liệu cũ
#     old_parquet = pd.read_parquet(parquet_path)
        
#         # Gộp dữ liệu cũ và mới
#     combined = pd.concat([old_parquet, new_data], ignore_index=True)
        
#         # Xóa trùng: 
#         # Lưu ý: 'scraped_at' thường khác nhau giữa các lần chạy, 
#         # nên ta xóa trùng dựa trên các cột định danh cố định
#     subset_cols = ['ticker', 'exchange', 'person_name', 'role']
#     combined = combined.drop_duplicates(subset=subset_cols, keep='last')
        
#     print(f"Đã gộp dữ liệu. Tổng số dòng hiện tại: {len(combined)}")
# else:
#     combined = new_data.drop_duplicates()
#     print(f"Tạo file mới. Tổng số dòng: {len(combined)}")

#     # 5. Ghi file Parquet
# combined.to_parquet(parquet_path, index=False, engine="pyarrow")
# print(f"Đã lưu dữ liệu vào: {parquet_path}")


import requests
from datetime import datetime
import pandas as pd
import json
import time
from utils import fetch_all_tickers_from_api, get_board_data  # Đảm bảo hàm này trả về một LIST các dict
import os

# Nơi lưu 
JSON_PATH_TICKER = "../data/raw/all_tickers.json"
# Đọc dữ liệu tickers từ file json đã lưu
# final_tickers = fetch_all_tickers_from_api()
    
# for exchange in final_tickers:
#     final_tickers[exchange] = sorted(list(set(final_tickers[exchange])))
    
# os.makedirs(os.path.dirname(JSON_PATH_TICKER), exist_ok=True)
# with open(JSON_PATH_TICKER, "w", encoding="utf-8") as f:
#     json.dump(final_tickers, f, ensure_ascii=False, indent=4)
    
# total = sum(len(v) for v in final_tickers.values())
# print(f"\n--- THÀNH CÔNG ---")
# print(f"Tổng số mã: {total}")
# print(f"Lưu tại: {JSON_PATH_TICKER}")


with open(JSON_PATH_TICKER, "r", encoding="utf-8") as f:
    all_tickers = json.load(f)

result = []
for exchange, ticker_list in all_tickers.items():
    # Thử nghiệm với 30 mã mỗi sàn
    for ticker in ticker_list[:30]:
        data_list = get_board_data(ticker, exchange)
        
        if data_list: # Kiểm tra nếu có dữ liệu
            # DÙNG EXTEND để làm phẳng danh sách
            result.extend(data_list) 
            
        print(f"Đã xong {ticker}")
        time.sleep(1)                   

# Bây giờ mỗi phần tử trong result là 1 dict (1 nhân sự), pd.DataFrame sẽ ra đúng cột
new_data = pd.DataFrame(result)

parquet_path = "../data/raw/cafef_board.parquet"
os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

# Định nghĩa các cột dùng để xác định trùng lặp (không bao gồm scraped_at)
subset_cols = ['ticker', 'exchange', 'person_name', 'role']

if os.path.exists(parquet_path):
    old_parquet = pd.read_parquet(parquet_path)
    combined = pd.concat([old_parquet, new_data], ignore_index=True)
    
    # Xóa trùng dựa trên các cột định danh
    combined = combined.drop_duplicates(subset=subset_cols, keep='last')
    print(f"Đã gộp dữ liệu. Tổng số dòng hiện tại: {len(combined)}")
else:
    # Ngay cả file mới cũng nên drop_duplicates theo subset để đảm bảo sạch
    combined = new_data.drop_duplicates(subset=subset_cols, keep='last')
    print(f"Tạo file mới. Tổng số dòng: {len(combined)}")

# Lưu file Parquet
combined.to_parquet(parquet_path, index=False, engine="pyarrow")
print(f"Đã lưu dữ liệu vào: {parquet_path}")

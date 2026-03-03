import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
from datetime import datetime
from utils import get_data, crawl_cafef_tickers
import pyarrow
import json
import os

'''
UPCOM có ID: 9
HSX có ID: 1 (HOSE)
HNX có ID: 2
'''

#Crawl tất cả dữ liệu tickers của mỗi sàn
# upcom_tickers = crawl_cafef_tickers("9")
# hose_tickers = crawl_cafef_tickers("1")
# hnx_tickers = crawl_cafef_tickers("2")

# new_all_tickers = {
#     "upcom": upcom_tickers,
#     "hose": hose_tickers,
#     "hnx": hnx_tickers
# }

# json_path = "../data/raw/all_tickers.json"

# # Nếu file tồn tại thì merge
# if os.path.exists(json_path):
#     with open(json_path, "r", encoding="utf-8") as f:
#         old_data = json.load(f)
# else:
#     old_data = {}

# old_data.update(new_all_tickers)

# with open(json_path, "w", encoding="utf-8") as f:
#     json.dump(old_data, f, ensure_ascii=False, indent=4)

# all_tickers = old_data


with open("../data/raw/all_tickers.json", "r", encoding="utf-8") as f:
    all_tickers = json.load(f)
 

# Crawl dữ liệu ban lãnh đạo, dùng 30 mã chứng khoán mỗi sàn
result = []
for exchange, ticker_list in all_tickers.items():
    for ticker in ticker_list[:30]:
        url = f"https://cafef.vn/du-lieu/{exchange}/{ticker}-ban-lanh-dao-so-huu.chn"
        result = get_data(url, ticker, exchange, result)
        print(f"Successfully process ticker {ticker}")
        time.sleep(1)

print(f"Tìm thấy: {len(result)} người")

new_data = pd.DataFrame(result)

# Xóa trùng trong batch hiện tại
new_data = new_data.drop_duplicates()

parquet_path = "../data/raw/cafef_board.parquet"
os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

if os.path.exists(parquet_path):
        # Đọc dữ liệu cũ
    old_parquet = pd.read_parquet(parquet_path)
        
        # Gộp dữ liệu cũ và mới
    combined = pd.concat([old_parquet, new_data], ignore_index=True)
        
        # Xóa trùng: 
        # Lưu ý: 'scraped_at' thường khác nhau giữa các lần chạy, 
        # nên ta xóa trùng dựa trên các cột định danh cố định
    subset_cols = ['ticker', 'exchange', 'person_name', 'role']
    combined = combined.drop_duplicates(subset=subset_cols, keep='last')
        
    print(f"Đã gộp dữ liệu. Tổng số dòng hiện tại: {len(combined)}")
else:
    combined = new_data.drop_duplicates()
    print(f"Tạo file mới. Tổng số dòng: {len(combined)}")

    # 5. Ghi file Parquet
combined.to_parquet(parquet_path, index=False, engine="pyarrow")
print(f"Đã lưu dữ liệu vào: {parquet_path}")



# import pandas as pd
# import time
# import requests
# from bs4 import BeautifulSoup



# url_root = "https://cafef.vn/du-lieu"

# exchange = "upcom"
# ticker = "vin"

# url = f"{url_root}/{exchange}/{ticker}-ban-lanh-dao-so-huu.chn"
# print(url)
# # Thêm các header để thay thế khi gửi yêu cầu HTTP tránh bị chặn bởi server
# headers = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
# }



# response = requests.get(url, headers=headers)
# response.raise_for_status()  # Kiểm tra nếu có lỗi HTTP
# soup = BeautifulSoup(response.content, "html.parser")
# # Tìm bảng chứa thông tin ban lãnh đạo sở hữu
# director_and_owner_nodes = soup.find_all("div", class_="directorandonwer_body-directory-topperson")
# result = []
# print(f"Tìm thấy {len(director_and_owner_nodes)} nhân sự cấp cao.")
# for node in director_and_owner_nodes:
#     name_tag = node.find("div", class_="directorandonwer_name-top")
#     if name_tag:
#         name_tag = name_tag.find("a")
#         name = name_tag.text.strip() if name_tag else "N/A"
#     else:
#         name = "N/A"
#     result.append({'name': name})


# print(result)

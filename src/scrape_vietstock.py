from utils import crawl_latest_board, save_to_parquet
import json
import time
# file path lưu data được lưu

file_path = "../data/raw/vietstock_board.parquet"

# Nơi lưu các tickers của mỗi exchange
file_json_tickers ="../data/raw/all_tickers.json"

result = []

with open(file_json_tickers, "r", encoding=("utf-8")) as f:
    all_tickers = json.load(f)

for exchange, tickers_list in all_tickers.items():
    for ticker in tickers_list[:30]:
        result.extend(crawl_latest_board(ticker, exchange))
        print(f"Xử lý thành công {ticker}")
        time.sleep(1)

# Lưu vào parquet
save_to_parquet(result, "../data/raw/vietstock_board.parquet")
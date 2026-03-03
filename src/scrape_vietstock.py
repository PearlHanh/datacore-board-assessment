from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

options = Options()
options.add_argument('--remote-allow-origins=*')

# WebDriverManager sẽ tự tìm bản Chrome bạn đang dùng và tải Driver tương ứng
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

driver.get("https://finance.vietstock.vn/A32/ban-lanh-dao.htm")

page_html = driver.page_source
if "Nguyễn Thế Anh" in page_html:
    print("Dữ liệu nằm trong HTML, không cần tìm API!")
else:
    print("Dữ liệu không có trong HTML, chắc chắn có API hoặc iframe.")



# Đọc dữ liệu các mã tickers đã được lưu
json_path = "../data/raw/all_tickers.json"

with open()
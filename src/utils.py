import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
from datetime import datetime



# Crawl data các tickers 

def crawl_cafef_tickers(exchange_index):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    chrome_options.add_argument("--log-level=3")
    
    driver = webdriver.Chrome(options=chrome_options)
    url = "https://cafef.vn/du-lieu/du-lieu-doanh-nghiep.chn"
    driver.get(url)
    wait = WebDriverWait(driver, 15)

    all_tickers = []

    try:
        # --- Ghi nhớ mã đầu tiên của bảng để biết bảng có đổi  ---
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-data-business tbody tr td.col-1 a")))
            old_first_ticker = driver.find_element(By.CSS_SELECTOR, "table.table-data-business tbody tr td.col-1 a").text.strip()
        except:
            old_first_ticker = "EMPTY"

        # --- Chọn sàn trong dropdown ---
        select_element = wait.until(EC.presence_of_element_located((By.ID, "inp-data-business-center")))
        select = Select(select_element)
        select.select_by_value(exchange_index) 
        time.sleep(0.5)

        # --- Bấm nút tìm kiếm ---
        # Tìm nút div có onclick chứa handleSearchCompany
        try:
            search_btn_xpath = "//div[contains(@onclick, 'handleSearchCompany')]"
            search_button = wait.until(EC.element_to_be_clickable((By.XPATH, search_btn_xpath)))
            driver.execute_script("arguments[0].click();", search_button)
            print(f"Đã bấm nút Tìm kiếm cho sàn {exchange_index}")
        except Exception as e:
            print(f"Không tìm thấy nút Tìm kiếm: {e}")

        # --- Đợi load bảng mới ---
        def table_has_changed(d):
            try:
                current_first = d.find_element(By.CSS_SELECTOR, "table.table-data-business tbody tr td.col-1 a").text.strip()
                return current_first != old_first_ticker
            except:
                return False
        
        try:
            # Đợi tối đa 10s để dữ liệu sàn mới hiện ra
            wait.until(table_has_changed)
        except:
            print("Lưu ý: Bảng có thể không thay đổi nội dung hoặc sàn này không có dữ liệu.")

        time.sleep(1) # Nghỉ thêm 1s cho ổn định

        # --- Lặp qua từng trang để lấy mã ---
        page_num = 1
        last_first_ticker = ""

        while True:
            # print(f"Đang xử lý Sàn [{exchange_index}] - Trang {page_num}...")
            
            # Đợi bảng ổn định
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "table-data-business")))
            rows = driver.find_elements(By.CSS_SELECTOR, "table.table-data-business tbody tr")
            
            if not rows:
                break

            current_page_tickers = []
            for row in rows:
                try:
                    ticker = row.find_element(By.CSS_SELECTOR, "td.col-1 a").text.strip()
                    if ticker:
                        current_page_tickers.append(ticker)
                except:
                    continue

            # Kiểm tra dừng nếu trang bị lặp (hết dữ liệu)
            if current_page_tickers and current_page_tickers[0] == last_first_ticker:
                break
            
            if current_page_tickers:
                last_first_ticker = current_page_tickers[0]
                all_tickers.extend(current_page_tickers)

            # Bấm nút sang trang ---
            try:
                next_btn_xpath = "//div[contains(@onclick, 'pageIndex + 1')]"
                next_buttons = driver.find_elements(By.XPATH, next_btn_xpath)

                if next_buttons and next_buttons[0].is_displayed():
                    current_old_ticker = current_page_tickers[0]
                    driver.execute_script("arguments[0].click();", next_buttons[0])
                    
                    # Đợi trang tiếp theo load xong
                    def page_has_changed(d):
                        try:
                            return d.find_element(By.CSS_SELECTOR, "table.table-data-business tbody tr td.col-1 a").text.strip() != current_old_ticker
                        except:
                            return False
                    
                    wait.until(page_has_changed)
                    page_num += 1
                else:
                    break
            except:
                break

    finally:
        driver.quit()
    
    return all_tickers


# Lấy dữ liệu các directors và onwers
# def get_data(url, ticker, exchange, result):
#     # Chạy ở chế độ ẩn danh
#     chrome_options = Options()
#     chrome_options.add_argument("--headless")


#     driver = webdriver.Chrome(options=chrome_options)
#     driver.get(url) 


#     time.sleep(1)
#     soup = BeautifulSoup(driver.page_source, "html.parser")
#     driver.quit()

#     # Kiểu 1: Gồm top các directory top people
#     nodes_1 = soup.find_all("div", class_="directorandonwer_body-directory-topperson")
#     for node in nodes_1:
#         name_div = node.find("div", class_="directorandonwer_name-top")
#         # Cấu trúc tên director được lưu trong thẻ <a> bên trong div
#         name = name_div.find("a").text.strip() if name_div and name_div.find("a") else "N/A"

#         # Cấu trúc role director được lưu ở directorandonwer_position-top
#         pos_div = node.find("div", class_="directorandonwer_position-top")
#         position = pos_div.text.strip() if pos_div else "N/A"
#         scraped_at = datetime.now().replace(microsecond=0).isoformat()

#         result.append({'ticker': ticker, 'exchange': exchange,'person_name': name, 'role': position, 'source': 'cafef', 'scraped_at:': scraped_at})


#     # Kiểu 2: Gồm các another directory people

#     nodes_2 = soup.find_all("div", class_="directorandonwer_body-directory-person")
#     for node in nodes_2:
#         name_tag = node.find("a")
#         name = name_tag.text.strip() if name_tag else "N/A"
#         pos_div = node.find("div", style_="")
#         position = pos_div.get_text(strip=True) if pos_div else "N/A"
#         scraped_at = datetime.now().replace(microsecond=0).isoformat()
#         result.append({'ticker': ticker, 'exchange': exchange,'person_name': name, 'role': position, 'source': 'cafef', 'scraped_at:': scraped_at})

#     # Kiểu 3: Các thành viên ban kiểm toán

#     nodes_3 = soup.find_all("div", class_="directorandonwer_body-audit-list")

#     for node in nodes_3:
#         name_tag = node.find("a")
#         name = name_tag.text.strip() if name_tag else "N/A"
#         pos_div = node.find("div", style_="")
#         position = pos_div.text.strip() if pos_div else "N/A"
#         scraped_at = datetime.now().replace(microsecond=0).isoformat()
#         result.append({'ticker': ticker, 'exchange': exchange,'person_name': name, 'role': position, 'source': 'cafef', 'scraped_at:': scraped_at})

#     # Kiểu 4: Các thành viên vị trí khác
#     nodes_4 = soup.find_all("div", class_="directorandonwer_body-different-list")
#     for node in nodes_4:
#         name_tag = node.find("a")
#         name = name_tag.text.strip() if name_tag else "N/A"
#         pos_div = node.find("div", style_="")
#         position = pos_div.text.strip() if pos_div else "N/A"
#         scraped_at = datetime.now().replace(microsecond=0).isoformat()
#         result.append({'ticker': ticker, 'exchange': exchange,'person_name': name, 'role': position, 'source': 'cafef', 'scraped_at:': scraped_at})


#     return result

def get_data(url, ticker, exchange, result):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    chrome_options.add_argument("--log-level=3")

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url) 

    time.sleep(1)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    scraped_at = datetime.now().replace(microsecond=0).isoformat()

    # --- Kiểu 1: Top People (Dùng class định danh vì cấu trúc này ổn định) ---
    nodes_1 = soup.find_all("div", class_="directorandonwer_body-directory-topperson")
    for node in nodes_1:
        name_tag = node.find("div", class_="directorandonwer_name-top")
        name = name_tag.find("a").get_text(strip=True) if name_tag and name_tag.find("a") else "N/A"
        
        pos_tag = node.find("div", class_="directorandonwer_position-top")
        position = pos_tag.get_text(strip=True) if pos_tag else "N/A"
        
        if name != "N/A":
            result.append({'ticker': ticker, 'exchange': exchange, 'person_name': name, 'role': position, 'source': 'cafef', 'scraped_at': scraped_at})

    # --- Kiểu 2, 3: Các thành viên khác (Sử dụng danh sách chuỗi văn bản) ---
    # Danh sách các class tương ứng với Kiểu 2, 3
    other_node_classes = [
        "directorandonwer_body-directory-person", # Kiểu 2
        "directorandonwer_body-column-six"       # Kiểu 3
    ]

    for css_class in other_node_classes:
        nodes = soup.find_all("div", class_=css_class)
        for node in nodes:
            # stripped_strings lấy ra list các chữ: [Tên, Chức vụ, Tuổi]
            content_list = list(node.stripped_strings)
            
            if len(content_list) >= 1:
                name = content_list[0] # Phần tử đầu là Tên
                
                # Phần tử thứ 2 là Chức vụ. Nếu không có thì để N/A
                position = content_list[1] if len(content_list) > 1 else "N/A"
                
                # Loại bỏ các trường hợp rác hoặc hàng trống
                if name != "N/A" and name != "":
                    result.append({
                        'ticker': ticker, 
                        'exchange': exchange,
                        'person_name': name, 
                        'role': position, 
                        'source': 'cafef', 
                        'scraped_at': scraped_at
                    })

    return result
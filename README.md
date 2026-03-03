Đây là nội dung file README.md được soạn thảo chuyên nghiệp, phản ánh đúng các kỹ thuật và quyết định thiết kế mà chúng ta đã thực hiện xuyên suốt dự án.

#Vietnam Stock Market Board of Directors - Data Pipeline

Dự án này là một hệ thống ETL (Extract, Transform, Load) chuyên sâu, thu thập dữ liệu về Ban lãnh đạo từ hai nguồn tài chính hàng đầu Việt Nam: Vietstock và CafeF. Hệ thống xử lý các xung đột dữ liệu để tạo ra một tập dữ liệu "Golden" duy nhất, chất lượng cao.

##🚀 1. Setup Instructions

###Cài đặt chương trình
```bash
git clone https://github.com/PearlHanh/datacore-board-assessment
```

###Yêu cầu hệ thống

Python 3.9+

Cài đặt các thư viện cần thiết:


```bash
cd datacore-board-assesment
pip install requirements.txt
```

###Cấu trúc thư mục
```
datacore-board-assessment/
├── README.md                  # Setup, how to run, your approach
├── requirements.txt            # Python dependencies (pinned versions)
├── config.yaml                 # All configurable parameters
├── src/
│   ├── scrape_cafef.py        # Task 1 scraper
│   ├── scrape_vietstock.py    # Task 2 scraper
│   ├── merge.py               # Task 3 merge logic
│   └── utils.py               # Shared utilities (name normalization, etc.)
├── data/
│   ├── raw/                   # Raw scraped outputs
│   ├── processed/             # Cleaned individual source data
│   └── final/                 # Merged golden dataset
├── docs/
│   ├── data_dictionary.md     # Field definitions and metadata
│   └── data_quality_report.md # Quality analysis
├── notebooks/
│   ├── main.ipynb             # EDA
│   └── observe.md             # review table of data        
└── tests/                         # Unit tests
```
##🛠️ 2. How to Run

Hệ thống được thiết kế chạy theo thứ tự các Task:

###Bước 1: Crawl dữ liệu từ CafeF

```Bash
python src/scrape_cafef.py
```
###Bước 2: Crawl dữ liệu từ Vietstock (Vượt rào cản CSRF/TLS)

```Bash
python src/scrape_vietstock.py
```
###Bước 3: Hợp nhất dữ liệu và tạo Golden Dataset

```Bash
python src/merge.py
```

##🧠 3. Technical Approach & Decisions
###3.1. Chiến lược Thu thập Ticker list (Từ CafeF)
Thay vì cào dữ liệu (Scraping) từ giao diện người dùng (DOM) vốn chậm và dễ lỗi khi giao diện thay đổi, dự án này tập trung vào việc truy tìm "Data Root" – các Endpoint API ẩn mà CafeF sử dụng để đổ dữ liệu vào trang Hồ sơ doanh nghiệp. Điều này cải thiện rất nhiều về mặt thời gian so với việc crawl từ giao diện.

- Sử dụng Chrome DevTools để truy vết bằng cách tìm vị trí trang web gọi API sau khi load lại dữ liệu trong trang https://cafef.vn/du-lieu/du-lieu-doanh-nghiep.chn

- Phát hiện các yêu cầu đều được gửi đến tập tin https://cafef.vn/du-lieu/ajax/pagenew/databusiness/congtyniemyet.ashx. Đây chính là gốc - nơi mà dữ liệu trang web nhận được.

Hàm fetch_all_tickers_from_api thực hiện việc truy vấn nguồn này theo cách sau:
- Phân loại theo sàn (Exchange): Nó duyệt qua danh sách các sàn (EXCHANGES) với các ID tương ứng mà CafeF quy định:
    - 1: HOSE
    - 2: HNX
    - 9: UPCOM
- Cơ chế Phân trang (Pagination): Vì API không trả về hàng nghìn mã cùng lúc, nên cần sử dụng cơ chế skip và take: 
    - take=50: Mỗi lần yêu cầu lấy 50 mã.
    - skip: Tăng dần (0, 50, 100...) cho đến khi không còn dữ liệu trả về (if not data_list: break).
- Tham số truy vấn (Parameters):
    - centerid: ID của sàn chứng khoán.
    - major=0: Lấy tất cả ngành nghề (không lọc theo ngành cụ thể).


###3.2. Chiến lược thu thập ban điều hành (CafeF)
Tương tự như cách đi tìm data gốc của ticker list.

- Phát hiện các yêu cầu đều được gửi đến tập tin https://cafef.vn/du-lieu/Ajax/PageNew/ListCeo.ashx?Symbol={ticker}&PositionGroup=0. Đây chính là gốc - nơi mà dữ liệu trang web nhận được.

- Với ticker là mã doanh nghiệp, nơi đây lưu TOÀN BỘ dữ liệu ban điều hành của các công ty.

Hàm get_board_data thực hiện việc thu thập nguồn này theo cách sau:
- Gửi request lên url để nhận về file json chứa data của ban điều hành.
- Thu thập theo đúng trường thuộc tính.

*Chú ý: Hệ thống hiện tại đang lấy dữ liệu của 30 tickers mỗi sàn giao dịch, người dùng muốn thay đổi số lượng có thể sửa tại vị trí dòng code thứ 31 của src/scrape_cafef.py*


###3.3. Lấy dữ liệu ban điều hành (Vietstock)
Khác với CafeF cho phép truy cập API khá cởi mở, Vietstock thiết kế hệ thống theo mô hình bảo mật nhiều lớp. Để lấy được dữ liệu "gốc", hệ thống phải thực hiện một quy trình giả lập trình duyệt hoàn chỉnh.

Dữ liệu Ban lãnh đạo của Vietstock không nằm trong mã nguồn HTML. Nó được tải động từ một Internal API Endpoint: https://finance.vietstock.vn/data/boarddetails

- Vượt rào CSRF (Cross-Site Request Forgery):
    - Vietstock sử dụng cơ chế Synchronizer Token Pattern. Server chỉ trả dữ liệu nếu request đính kèm một mã __RequestVerificationToken hợp lệ.
    - Giải pháp: Code thực hiện 2 bước. Bước 1 truy cập trang HTML (url_main) để "lấy trộm" mã Token đang ẩn trong các thẻ <input type="hidden">. Bước 2 mới dùng Token này để "mở khóa" API.
- Giả lập dấu vân tay trình duyệt (TLS Fingerprinting):
    - Đây là phần cao cấp nhất. Vietstock sử dụng tường lửa (WAF) để nhận diện các thư viện Python thông thường (như requests hay urllib). Các thư viện này có cách "bắt tay" TLS khác con người, dẫn đến việc bị chặn ngay lập tức.
    - Giải pháp: Sử dụng curl_cffi với tham số impersonate="chrome110". Kỹ thuật này giả lập chính xác cách trình duyệt Chrome thật sự kết nối mạng, khiến hệ thống bảo mật của Vietstock coi script là một người dùng bình thường.
- Xác thực yêu cầu AJAX:
    - Hệ thống bắt buộc phải có Header X-Requested-With: XMLHttpRequest. Điều này khẳng định request được gửi đi từ một ứng dụng Web (AJAX), giúp vượt qua các kiểm tra tính hợp lệ của request từ phía Server ASP.NET.

API Vietstock trả về các trường thông tin mà CafeF không có: YearOfBirth (năm sinh để tính tuổi), TimeSticking (thâm niên), và TotalShares (số lượng cổ phiếu chính xác).

Do đặc thù của web cập nhật dữ liệu ban điều hành mỗi 6 tháng một lần, nên hệ thống chỉ thu thập các dữ liệu cập nhật gần nhất.

*Chú ý: Hệ thống hiện tại đang lấy dữ liệu của 30 tickers mỗi sàn giao dịch, người dùng muốn thay đổi số lượng có thể sửa tại vị trí dòng code thứ 18 của src/scrape_vietstock.py*

###3.4. Lưu dữ liệu  
Sử dụng Apache Parquet thay vì CSV để đảm bảo bảo toàn kiểu dữ liệu (Schema), nén dữ liệu tốt và hỗ trợ xử lý hiệu năng cao cho các công cụ BI hoặc Data Science.

###3.5. Merge dữ liệu thành golden board
Hệ thống sử dụng phương pháp Composite Join Key kết hợp với Rule-based Conflict Resolution để đảm bảo dữ liệu "Golden" đạt chất lượng cao nhất.

- Composite Key (Khóa phức hợp): Hệ thống không chỉ join theo tên, mà dùng bộ ba ticker + exchange + normalized_name. Điều này giúp phân biệt chính xác một người nếu họ cùng tên nhưng ở các sàn khác nhau hoặc mã chứng khoán khác nhau.
- Xử lý "Bẫy Unicode" tiếng Việt: Đây là thách thức lớn nhất. Tiếng Việt có hai bảng mã (Dựng sẵn và Tổ hợp) trông giống hệt nhau nhưng máy tính hiểu khác nhau. Code sử dụng unidecode để đưa tất cả về dạng ASCII không dấu (truonggiabinh) trước khi tạo Key.
- Lọc nhiễu danh xưng: Sử dụng hàm clean_honorific_only để bóc tách "Ông/Bà" và normalize_string để loại bỏ học vị (TS, ThS). Điều này đảm bảo "TS. Trương Gia Bình" và "Ông Trương Gia Bình" sẽ được nhận diện là một thực thể duy nhất.

Hệ thống giải quyết xung đột như sau:

- Tính cập nhật (Freshness): Ưu tiên CafeF cho cột role (chức vụ). Vì CafeF là trang tin tức, họ cập nhật các biến động nhân sự (bổ nhiệm/miễn nhiệm) theo ngày, trong khi Vietstock cập nhật theo kỳ báo cáo (6 tháng).
- Độ sâu dữ liệu (Enrichment): Vietstock cung cấp các trường dữ liệu mà CafeF không có (age, total_shares, time_sticking). Hệ thống sẽ tự động "đắp" các thông tin này vào bản ghi của CafeF để tạo ra một dòng dữ liệu giàu thông tin nhất.
- Gắn cờ trạng thái (Agreement Flagging):
    - both: Hai nguồn khớp nhau hoàn toàn.
    - conflict: Hai nguồn tìm thấy cùng một người nhưng lệch chức danh (Cần con người kiểm tra).
    - only: Bản ghi chỉ xuất hiện ở một nguồn (Báo hiệu sự thiếu sót của nguồn còn lại).

Quản trị chất lượng (Data Governance):

- Confidence Score (0.7 - 1.0): Mỗi dòng dữ liệu được gán một điểm số tin cậy. Dữ liệu có sự xác nhận từ cả hai nguồn (both) sẽ có điểm tuyệt đối (1.0).
- Data Quality Category: Phân loại bản ghi (Perfect, Good, Low) giúp người dùng cuối (Data Scientist/Analyst) biết được mức độ đầy đủ của dữ liệu trước khi đưa vào mô hình phân tích.

###3.6. Công việc cần cải thiện khi có thêm thời gian
Hệ thống cần chú trọng hơn vào việc xử lý dữ liệu, với trường dữ liệu "role" bên dữ liệu Vietstock, cần chuẩn hóa toàn bộ để đồng bộ hơn với dữ liệu CafeF.

Cấu trúc lại file log cho từng tác vụ.


###3.7. Các hạn chế đã biết
Các mã cổ phiếu có thể lỗi:
- Mã mới niêm yết hoặc OTC: Các mã cổ phiếu vừa lên sàn trong vòng 24-48h hoặc các mã thuộc sàn OTC có thể chưa được cập nhật đồng bộ trên API của Vietstock, dẫn đến lỗi 404 Not Found hoặc trả về mảng dữ liệu trống.
- Mã bị tạm ngừng giao dịch/Hủy niêm yết: Một số mã cổ phiếu cũ vẫn tồn tại trong danh sách của CafeF nhưng trang hồ sơ doanh nghiệp đã bị gỡ bỏ, gây lỗi khi truy vấn API chi tiết.

Các Edge Cases chưa xử lý (Trường hợp ngoại lệ):
- Lãnh đạo là người nước ngoài thường có cấu trúc tên đảo ngược (Họ trước hoặc Tên trước) tùy theo nguồn tin, dẫn đến việc tạo join_key bị lệch và không thể gộp dòng (thành 2 bản ghi only).
- Trùng tên trong cùng một công ty: Trong trường hợp hiếm hoi có hai lãnh đạo trùng tên hoàn toàn trong cùng một mã chứng khoán, hệ thống sẽ coi họ là một thực thể duy nhất do join_key bị trùng.
- Kiêm nhiệm phức tạp: Một số lãnh đạo giữ 4-5 chức vụ cùng lúc. Vietstock dùng dấu / để phân tách, CafeF có thể tách thành các dòng riêng lẻ. Việc chuẩn hóa chuỗi role đôi khi vẫn chưa phủ hết 100% các biến thể viết tắt cực đoan.

Những hạn chế kỹ thuật hiện tại:
- Độ trễ dữ liệu (Data Latency): Dữ liệu số lượng cổ phiếu (total_shares) từ Vietstock thường dựa trên báo cáo định kỳ (6 tháng/lần). Các giao dịch mua bán nội bộ diễn ra hàng ngày trên CafeF có thể chưa được phản ánh kịp thời vào con số này.
- Xử lý tuần tự (Sequential Processing): Pipeline hiện tại đang chạy tuần tự từng mã. Với danh sách khoảng 1600 mã, tổng thời gian xử lý có thể kéo dài. Chưa áp dụng lập trình đa luồng (Multi-threading) để tối ưu tốc độ do lo ngại bị sàn chặn IP sớm.

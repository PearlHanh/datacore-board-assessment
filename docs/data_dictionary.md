# Data Dictionary: Board Golden Dataset

| Field | Type | Description | Null Rate | Caveats |
| :--- | :--- | :--- | :--- | :--- |
| **ticker** | `object` | Mã chứng khoán niêm yết. | 0.0% | - |
| **exchange** | `object` | Sàn giao dịch (HOSE, HNX, UPCOM). | 0.0% | - |
| **person_name** | `object` | Họ tên lãnh đạo (Đã làm sạch danh xưng Ông/Bà). | 0.0% | - |
| **role** | `object` | Chức vụ hiện tại (Ưu tiên dữ liệu từ CafeF). | 0.0% | Dữ liệu conflict sẽ ưu tiên giá trị của CafeF. |
| **age** | `float64` | Tuổi của lãnh đạo (Nguồn: Vietstock). | 34.1% | - |
| **total_shares** | `float64` | Số cổ phiếu nắm giữ cá nhân (Nguồn: Vietstock). | 28.5% | - |
| **time_sticking** | `object` | Thời gian đảm nhiệm/Thâm niên (Nguồn: Vietstock). | 28.5% | - |
| **source_agreement** | `object` | Trạng thái đồng nhất: both, conflict, cafef_only, vietstock_only. | 0.0% | - |
| **confidence_score** | `float64` | Điểm tin cậy dữ liệu (0.7 - 1.0). | 0.0% | - |
| **data_quality** | `object` | Phân loại mức độ đầy đủ của bản ghi. | 0.0% | - |
| **scraped_at** | `object` | Thời điểm tạo bản ghi Golden. | 0.0% | - |

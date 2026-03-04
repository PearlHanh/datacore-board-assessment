# Data Quality Report: Board Golden Dataset
**Date:** 2026-03-04 10:05:08

## 1. Summary Metrics
- **Total Golden Records:** 1433
- **Vietstock Raw Count:** 857
- **CafeF Raw Count:** 1330
- **Match Rate:** 69.62%
- **Conflict Rate:** 47.62% (Role disagreement)

## 2. Agreement Distribution
| source_agreement   |   count |
|:-------------------|--------:|
| both               |     485 |
| conflict           |     441 |
| cafef_only         |     409 |
| vietstock_only     |      98 |

## 3. Data Quality Breakdown
| data_quality                             |   count |
|:-----------------------------------------|--------:|
| Acceptable (Single Source)               |     507 |
| Perfect (Full Data)                      |     444 |
| Needs Verification (Conflict)            |     441 |
| Good (Matched but Partial supplementary) |      41 |

## 4. Most Common Unmatched Names
*Những tên xuất hiện trong một nguồn nhưng không tìm thấy ở nguồn kia (Top 10):*
| person_name        |   count |
|:-------------------|--------:|
| ***                |      11 |
| Lương Quốc Quyền   |       4 |
| Nguyễn Khoa Tuyển  |       3 |
| Nguyễn Văn Vũ      |       3 |
| Nguyễn Minh Hiền   |       3 |
| Nguyễn Trường Sơn  |       3 |
| Hoàng Mạnh Cường   |       2 |
| Thái Hồng Ngọc     |       2 |
| Lê Thị Hiền        |       2 |
| Phạm Thị Ngọc Diễm |       2 |

## 5. Observed Patterns & Issues
### A. Unmatched Patterns
- **Short Names:** Có 11 tên không khớp có độ dài dưới 5 ký tự (Dấu hiệu viết tắt hoặc dữ liệu rác).
- **Special Characters:** Có 11 tên không khớp chứa ký tự đặc biệt.
- **Coverage Gap:** Nguồn CafeF cập nhật Ban Điều Hành hàng ngày, trong khi Vietstock cập nhật định kỳ (thường theo BCTC 6 tháng/năm), dẫn đến độ trễ trong việc khớp các bổ nhiệm mới.

### B. Conflict Patterns
*Các chức danh bị xung đột dữ liệu:*
| role                    |   count |
|:------------------------|--------:|
| Phó Tổng GĐ             |     116 |
| Thành viên HĐQT         |      69 |
| Tổng Giám đốc           |      40 |
| Thành viên HĐQT độc lập |      34 |
| Phụ trách quản trị      |      21 |

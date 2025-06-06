Yêu cầu hệ thống
Python 3.7 trở lên

Hệ điều hành: Windows / Linux / macOS


Mỗi node là một tiến trình chạy độc lập trên một port.

Dữ liệu được phân phối giữa các node bằng cách hash key → node.

Dữ liệu có một bản chính (primary) và một bản phụ (replica) ở node kế tiếp.

Cách chạy hệ thống
1. Mở 3 terminal / cửa sổ mới
2. Chạy từng node: python node.py

nhập vào port: 5000

nhập vào port: 5001

nhập vào port: 5002

Lưu ý: mỗi node nên được chạy trong một terminal riêng biệt.

rồi chạy python client.py ở terminal mới và test
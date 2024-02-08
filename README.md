# [IE212.O11.Group11] - Real-time Stress Detection on Reddit Posts, using Kafka and Spark streaming

* Trường Đại học Công nghệ Thông tin, Đại học Quốc gia Thành phố Hồ Chí Minh (ĐHQG-HCM).
* Khoa: Khoa học và kỹ thuật thông tin (KTTT).
* GVHD: TS. Đỗ Trọng Hợp.
* Nhóm sinh viên thực hiện: Nhóm 11.

## Danh sách thành viên
|STT | Họ tên | MSSV| Vai trò |
|:---:|:-------------:|:-----:|:-----:|
|1.  | Phan Nguyễn Hải Yến | 21521698 | Trưởng nhóm |
|2. 	| Lý Phi Lân		|	21520319 | Thành viên |
|3. 	| Lê Đức Mạnh		| 21521116 | Thành viên |

##  Giới thiệu
Với sự phát triển nhanh chóng của công nghệ thông tin, các trang mạng xã hội đã trở thành một phần không thể thiếu trong cuộc sống của nhiều người hiện nay. Theo báo cáo thống kê của Statista, tính đến tháng 10 năm 2023, có hơn 60% dân số thế giới sử dụng mạng xã hội [1]. Số lượng người sử dụng lớn kéo theo đó là số lượng bài viết khổng lồ được chia sẻ mỗi ngày, cùng với nhịp sống hiện đại có nhiều áp lực từ công việc, học tập, gia đình, xã hội… khiến tình trạng căng thẳng trở nên phổ biến và dễ dàng được quan sát hơn bao giờ hết.

Trong phạm vi đồ án môn học, nhóm tập trung vào việc xây dựng hệ thống có thể tiếp nhận các thông tin là các bài đăng trên trang mạng xã hội Reddit và phát hiện được tình trạng căng thẳng của bài viết một cách tự động. Từ đó làm nền tảng để các trang mạng xã hội đưa ra sự hỗ trợ kịp thời đến người dùng.

## Kiến trúc hệ thống
Nhóm chia kiến trúc hệ thống thành 2 thành phần chính.
* Thành phần ngoại tuyến (Offline): Được nhóm xây dựng và phát triển để huấn luyện và kiểm tra các mô hình, nhằm tìm ra mô hình tối ưu nhất áp dụng trong pipeline phát hiện căng thẳng trực tuyến theo thời gian thực. Nhóm sử dụng các mô hình Machine Learning trong Spark MLlib bao gồm: Word2Vec, SVM, RandomForest, LogisticRegression, GradientBoosted, DecisionTrees và các mô hình Deep Learning bao gồm: BERT, ROBERTA, DistilBERT, XLNet, Electra.
* Thành phần trực tuyến (Online): Là một pipeline phát hiện cảm xúc căng thẳng ở các bài đăng trên Reddit trong thời gian thực. Ba công việc chính trong phần này là thu thập dữ liệu trực tuyến trên mạng xã hội Reddit, truyền và xử lý dữ liệu theo thời gian thực, và phát hiện cảm xúc căng thẳng của dữ liệu này theo thời gian thực.
![Hệ thống (final)](https://github.com/namtuthien/IE212.O11.Group11/assets/96688782/b30b8431-781d-4f8e-b74d-38413b5ea787)

## Công nghệ và môi trường sử dụng
* Ubuntu 22.04.3 LTS.
* Apache Spark 3.5.0.
* Apache Kafka 3.6.0.
* Apache Airflow 2.8.0.
* Streamlit 1.30.0.

## Hướng dẫn cài đặt và thực thi
### Bước 1. Cài đặt Apache Spark
* Cập nhật các gói.
```
$ sudo apt update
```
* Tải Apache Spark.
```
$ curl "https://downloads.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz" -o ~/Downloads/spark.tgz
```
* Giải nén Apache Spark.
```
$ mkdir ~/spark && cd ~/spark
$ tar -xvzf ~/Downloads/spark.tgz --strip 1
```
### Bước 2. Cài đặt Apache Kafka
* Cập nhật các gói. Cài đặt default-jre.
```
$ sudo apt update
$ sudo apt-get install default-jre
```
* Tải Apache Kafka.
```
$ curl "https://downloads.apache.org/kafka/3.6.0/kafka_2.12-3.6.0.tgz" -o ~/Downloads/kafka.tgz
```
* Giải nén Apache Kafka.
```
$ mkdir ~/kafka && cd ~/kafka
$ tar -xvzf ~/Downloads/kafka.tgz --strip 1
```
### Bước 3. Cài đặt Apache Airflow
* Cập nhật các gói. Cài đặt pip.
```
$ sudo apt-get update
$ sudo apt-get install pip
```
* Tải Apache Airflow.
```
$ sudo pip install apache-airflow
```
### Bước 4. Clone Github repositoru
```
$ git clone https://github.com/namtuthien/IE212.O11.Group11.git
```
### Bước 5. Cấu hình Apache Airflow
* Truy cập vào thư mục vừa clone, khởi tạo airflow.db và tạo tài khoản đăng nhập.
```
$ export AIRFLOW_HOME="$(pwd)"
$ airflow db init
$ airflow users create --username your-user-name --firstname your-first-name --lastname your-last-name --role Admin --email your-email@gmail.com
```
* Tại airflow.cfg, thay đổi executor = LocalExecutor để cấu hình Apache Airflow có thể chạy nhiều tác vụ đồng thời.
![airflow-1](https://github.com/namtuthien/IE212.O11.Group11/assets/96688782/4968624d-f667-4d89-93ff-48d3f2b06916)
Xem thêm: https://medium.com/international-school-of-ai-data-science/local-executor-in-apache-airflow-f8ce6773f1da
### Bước 6. Cấu hình Apache Spark
* Truy cập vào thư mục cài đặt Apache Spark, cấu hình (như hình) để có thể thực thi Spark Standalone.
![spark-1](https://github.com/namtuthien/IE212.O11.Group11/assets/96688782/c9d3f93c-b3a7-49a8-9548-48b708c91485)
Xem thêm: https://spark.apache.org/docs/latest/spark-standalone.html
### Bước 7. Cài đặt các thư viện cần thiết
```
$ pip install -r requirements.txt
```
### Bước 8. Chạy Apache Spark
* Khởi tạo Spark Master và các Spark Worker.
```
$ cd spark
$ sbin/start-master.sh
$ sbin/start-worker.sh spark://127.0.0.1:1909
```
* Truy cập vào localhost:8080 để xem giao diện Spark Master.
![spark-2](https://github.com/namtuthien/IE212.O11.Group11/assets/96688782/f56d225a-e00e-43a1-adb5-3075a58cf752)
* Lệnh dừng Spark Master và các Spark Worker.
```
$ cd spark
$ sbin/stop-master.sh
$ sbin/stop-worker.sh spark://127.0.0.1:1909
```
### Bước 9. Chạy Apache Kafka
* Chạy Zookeeper.
```
$ cd kafka
$ bin/zookeeper-server-start.sh config/zookeeper.properties
```
* Chạy Kafka Server.
```
$ cd kafka
$ bin/kafka-server-start.sh config/server.properties
```
### Bước 10. Chạy Apache Airflow
* Chạy Airlfow Web Server.
```
$ cd your/path/to/IE212.O11.Group11
$ export AIRFLOW_HOME="$(pwd)"
$ airflow webserver --port 8888
```
* Chạy Airflow Scheduler.
```
$ export AIRFLOW_HOME="$(pwd)"
$ airflow scheduler
```
* Truy cập vào localhost:8888 và đăng nhập để xem giao diện quản lý của Airflow.
![Airflow-2](https://github.com/namtuthien/IE212.O11.Group11/assets/96688782/746d92fc-4f6e-4f3a-8f2e-d25764dfa823)
* Bấm vào nút "chạy" để thực thi offline_dag. Sau khi offline_dag thực thi thành công, tiếp tục "chạy" online_dag. Đồng thời, khi online_dag đang thực thi, sử dụng terminal để mở ứng dụng visualize.
```
$ cd your/path/to/IE212.O11.Group11/visualize
$ streamlit run crawled-data.py
$ streamlit run detected-result.py
```
* Truy cập vào localhost:8051 hoặc localhost:8052 để xem kết quả thực thi.
![visualize-1](https://github.com/namtuthien/IE212.O11.Group11/assets/96688782/7de89b09-aaa1-4773-b80f-687d5f4abecc)
![visualize-2](https://github.com/namtuthien/IE212.O11.Group11/assets/96688782/cdface1d-0e0c-4e91-a868-1cb0a760baeb)

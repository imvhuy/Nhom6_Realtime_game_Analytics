# Dockerfile để tạo Spark image với PyMongo
FROM bitnamilegacy/spark:3.5.5

USER root

# Cài đặt pymongo - package duy nhất cần thiết cho MongoDB
RUN pip install --no-cache-dir pymongo==4.6.1

# Tạo thư mục cho checkpoint
RUN mkdir -p /tmp/spark_checkpoint_ccu && chmod 777 /tmp/spark_checkpoint_ccu

USER 1001
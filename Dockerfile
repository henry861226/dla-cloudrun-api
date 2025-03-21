# 使用官方 Python 基礎映像
FROM python:3.9-slim

# env variable
# ENV LANG=C.UTF-8
# ENV PYTHONDONTWRITEBYTECODE=1
# ENV PYTHONUNBUFFERED=1
# ENV PIP_DISABLE_PIP_VERSION_CHECK=1
# ENV PYTHONPATH=/app

WORKDIR /app
# 複製程式碼和依賴
COPY . /app
# 安裝依賴
RUN pip install --no-cache-dir -r requirements.txt

# 指定執行命令
CMD ["python", "main.py"]

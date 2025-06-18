# 使用精簡版 Python 映像檔
FROM python:3.10-slim

# 設定工作目錄
WORKDIR /app

# 複製本地所有檔案進容器
COPY . .

# 安裝依賴套件
RUN pip install --no-cache-dir -r requirements.txt

# 預設執行主程式
CMD ["python", "fuel_detection/event_comparator.py"]





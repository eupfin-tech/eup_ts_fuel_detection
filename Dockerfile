# 使用精簡版 Python 映像檔
FROM python:3.10-slim

# 設定工作目錄
WORKDIR /app

# 安裝系統依賴（用於資料庫連線）
RUN apt-get update && apt-get install -y \
    freetds-dev \
    freetds-bin \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# 複製 requirements.txt 並安裝 Python 依賴
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製所有程式檔案
COPY . .

# 確保 CSV 檔案存在
RUN ls -la *.csv

# 設定環境變數（可選）
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
EXPOSE 8080

# 選擇啟動方式：

# 1. 執行完整的事件比對邏輯 + API 服務（推薦）
CMD ["sh", "-c", "echo 'Container started'; ls -la; python -c 'import os; print(\"Current dir:\", os.getcwd()); print(\"Files:\", os.listdir(\".\"))'; python event_comparator.py"]

# 2. 只啟動 API 服務（不執行事件比對邏輯）
# CMD ["uvicorn", "event_comparator:app", "--host", "0.0.0.0", "--port", "8080"]

# 3. 只啟動 API 服務 + OpenTelemetry
# CMD ["opentelemetry-instrument", "uvicorn", "event_comparator:app", "--host", "0.0.0.0", "--port", "8080"]
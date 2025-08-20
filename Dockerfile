FROM python:3.13.6-slim

# 防止生成 .pyc，并确保日志立即输出
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# 常用调试工具（ping/curl/vim）
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        iputils-ping \
        vim \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 安装依赖（若存在 requirements.txt 则安装其中依赖，否则安装 Flask 与 Gunicorn）
COPY . /app
RUN python -m pip install --upgrade pip \
    && pip install -r requirements.txt

EXPOSE 8082

# # 默认使用 Gunicorn 启动，期望存在 `app.py` 且包含 `app = Flask(__name__)`
CMD ["gunicorn", "-b", "0.0.0.0:8082", "app:app"]



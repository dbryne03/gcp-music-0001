FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
RUN useradd -r -u 1000 -s /bin/false appuser
ENV HF_HOME=/tmp/hf_cache
USER appuser
CMD ["python", "main.py"]

FROM python:3.11-slim
WORKDIR /app
ARG SERVICE_DIR
ARG PORT
ENV PORT=${PORT}
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/ ./shared/
COPY ${SERVICE_DIR}/app/ ./app/
ENV PYTHONPATH=/app
EXPOSE ${PORT}
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]
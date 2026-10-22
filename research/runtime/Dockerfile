FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /worker
COPY research/runtime/bounded_worker.py /worker/bounded_worker.py
RUN useradd --create-home --uid 10001 runner
USER runner
CMD ["python", "/worker/bounded_worker.py"]

FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
WORKDIR /worker
COPY research/runtime/bounded_worker.py /worker/bounded_worker.py
COPY research/runtime/model_runner.py /worker/model_runner.py
RUN pip install --no-cache-dir numpy==2.2.6 scipy==1.15.3 scikit-learn==1.6.1 joblib==1.5.1 threadpoolctl==3.6.0
RUN useradd --create-home --uid 10001 runner
USER runner
CMD ["python", "/worker/bounded_worker.py"]

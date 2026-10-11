ARG BASE_IMAGE
FROM ${BASE_IMAGE}
COPY requirements.txt /requirements.txt
RUN python -m pip install --no-cache-dir -r /requirements.txt && python -m pip freeze > /dependency-freeze.txt
# Application code, host credentials and the production Dockerfile are excluded.

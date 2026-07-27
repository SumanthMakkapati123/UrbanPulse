FROM python:3.12-slim

WORKDIR /app

COPY python/requirements.txt /app/python/requirements.txt
RUN pip install --no-cache-dir --disable-pip-version-check \
    -r /app/python/requirements.txt

COPY python /app/python
COPY reference-data /app/reference-data

ENV PYTHONPATH=/app/python \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "-m"]
CMD ["urbanpulse.bus_gps_producer", "--help"]

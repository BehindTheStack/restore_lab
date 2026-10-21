FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY lab.py schema_evolution.py source_catalog.py sqlserver_fixture.py canonical_contract.py mssql_sync.py monitor.py startup.py ./
ENTRYPOINT ["python", "-u", "/app/lab.py"]

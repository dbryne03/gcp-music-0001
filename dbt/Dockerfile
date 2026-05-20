FROM python:3.12-slim
WORKDIR /dbt

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dbt_project.yml .
COPY packages.yml .
COPY profiles.yml .
COPY models/ models/

RUN dbt deps --profiles-dir .

RUN useradd -r -u 1000 -s /bin/false dbtuser \
    && chown -R dbtuser /dbt

USER dbtuser

ENTRYPOINT ["dbt", "--profiles-dir", "."]

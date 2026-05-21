FROM python:3.12-slim
WORKDIR /dbt

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dbt_project.yml .
COPY packages.yml .
COPY profiles.yml .
COPY models/ models/
COPY macros/ macros/

ENV DBT_PROFILES_DIR=/dbt

RUN useradd -r -u 1000 -s /bin/false dbtuser \
    && chown -R dbtuser /dbt

USER dbtuser

RUN dbt deps

ENTRYPOINT ["dbt"]

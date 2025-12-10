FROM python:3.11-slim

WORKDIR /usr/app/dbt

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/usr/app/dbt/profiles

RUN pip install --no-cache-dir \
    dbt-core==1.9.0 \
    dbt-postgres==1.9.0 \
    elementary-data==0.13.0 \
    psycopg2-binary

COPY . /usr/app/dbt

CMD ["bash"]



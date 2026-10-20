FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /opt/drct

COPY requirements/app.txt requirements/dbt.txt /tmp/requirements/
RUN pip install -r /tmp/requirements/app.txt

# dbt lives in its own venv so its pins cannot conflict with the app requirements;
# only the `dbt` entrypoint is exposed on PATH.
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install -r /tmp/requirements/dbt.txt \
    && ln -s /opt/dbt-venv/bin/dbt /usr/local/bin/dbt

CMD ["python"]

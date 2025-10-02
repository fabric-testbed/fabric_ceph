FROM python:3.13.0-slim

LABEL maintainer="Komal Thareja <komal.thareja@gmail.com>"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /usr/src/app

# OS deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends cron && \
    rm -rf /var/lib/apt/lists/*

# 1) Install Python deps first (cache-friendly)
COPY requirements.txt /usr/src/app/requirements.txt
RUN python -m pip install --upgrade pip && \
    python -m pip install --no-cache-dir -r requirements.txt

# 2) Copy project and install package
COPY fabric_ceph /usr/src/app/fabric_ceph
COPY pyproject.toml README.md LICENSE /usr/src/app/
RUN python -m pip install --no-cache-dir /usr/src/app

# 3) Entrypoint
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 11000 8700

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["fabric_ceph"]

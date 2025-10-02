FROM python:3.13.0
MAINTAINER Komal Thareja<komal.thareja@gmail.com>

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
VOLUME ["/usr/src/app"]

EXPOSE 11000
EXPOSE 8700

RUN apt-get update
RUN apt-get install cron -y

COPY docker-entrypoint.sh /usr/src/app/
COPY fabric_ceph /usr/src/app/fabric_ceph
COPY pyproject.toml /usr/src/app/
COPY ./README.md /usr/src/app/
COPY LICENSE /usr/src/app/

RUN pip3 install .
RUN mkdir -p "/etc/fabric/ceph/config/"
RUN mkdir -p "/var/log/ceph-mgr"

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
CMD ["fabric_ceph"]

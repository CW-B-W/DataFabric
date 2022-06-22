FROM jupyter/pyspark-notebook:spark-3.2.1

USER root

RUN apt-get update && DEBIAN_FRONTEND=noninteractive && apt-get install -y --no-install-recommends \
      build-essential \
      libkrb5-dev \
      net-tools \
      curl \
      netcat \
      gnupg \
      libsnappy-dev \
      iputils-ping \
      wget \
    && rm -rf /var/lib/apt/lists/*

USER ${NB_UID}

RUN pip3 install setuptools wheel
ADD requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

ADD run.sh ${HOME}/run.sh
USER root
RUN chmod a+x ${HOME}/run.sh
USER ${NB_UID}
WORKDIR "${HOME}"
ENTRYPOINT ["tini", "-g", "--"]
CMD ["./run.sh"]
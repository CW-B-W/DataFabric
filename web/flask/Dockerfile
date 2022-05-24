FROM ubuntu:18.04

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

RUN apt-get update && DEBIAN_FRONTEND=noninteractive && apt-get install -y --no-install-recommends \
      python3 \
      python3-pip \
      python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install setuptools wheel
ADD requirements.txt /requirements.txt
RUN pip3 install -r requirements.txt

ADD entrypoint-flask.sh /entrypoint-flask.sh
RUN chmod a+x /entrypoint-flask.sh
ADD run.sh /run.sh
RUN chmod a+x /run.sh
CMD ["/run.sh"]

FROM ubuntu:18.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential \
      net-tools \
      iputils-ping \
      curl \
      netcat \
      gnupg \
      libsnappy-dev \
      libkrb5-dev \
      wget \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update && DEBIAN_FRONTEND=noninteractive && apt-get install -y --no-install-recommends \
      python3.7 \
      python3.7-dev \
      python3.7-distutils \
      python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 1
RUN update-alternatives --set python3 /usr/bin/python3.7

RUN pip3 install --upgrade pip

RUN apt-get update && DEBIAN_FRONTEND=noninteractive && apt-get install -y --no-install-recommends \
      alien \
      libaio1 \
      freetds-dev \
      freetds-bin \
    && rm -rf /var/lib/apt/lists/*
RUN wget -O /tmp/oracle-instantclient-basic-21.4.0.0.0-1.x86_64.rpm https://download.oracle.com/otn_software/linux/instantclient/214000/oracle-instantclient-basic-21.4.0.0.0-1.x86_64.rpm
RUN alien -i /tmp/oracle-instantclient-basic-21.4.0.0.0-1.x86_64.rpm

RUN pip3 install setuptools wheel
ADD requirements.txt /requirements.txt
RUN pip3 install -r requirements.txt

ADD run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]
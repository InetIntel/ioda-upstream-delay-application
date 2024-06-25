FROM ubuntu:jammy

ENV DEBIAN_FRONTEND=noninteractive
ENV APP_ENV=docker

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    build-essential \
    libpcap-dev \
    git \
    autoconf \
    automake \
    libtool \
    zlib1g-dev \
    wget \
    tcpdump \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Yarrp
RUN git clone https://github.com/cmand/yarrp.git /yarrp
WORKDIR /yarrp
RUN ./bootstrap && ./configure && make

WORKDIR /

# IODA-UPSTREAM-DELAY
RUN git clone https://github.com/InetIntel/ioda-upstream-delay-new.git /ioda-upstream-delay
RUN mkdir -p /project/intermediate_result /project/result
# RUN python3 -m pip install asyncio


WORKDIR /project
CMD ["python3", "main.py"]

FROM ubuntu:jammy

ENV DEBIAN_FRONTEND=noninteractive
ENV APP_ENV=docker

ARG GITHUB_TOKEN

RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    autoconf \
    automake \
    libtool \
    zlib1g-dev \
    libpcap-dev \
    curl \
    wget \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

#TODO app nees some other python libs

# Yarrp
RUN git clone https://github.com/cmand/yarrp.git /yarrp
WORKDIR /yarrp
RUN ./bootstrap && ./configure && make

WORKDIR /

# IODA-UPSTREAM-DELAY
RUN echo "machine github.com login $GITHUB_TOKEN" > /root/.netrc && \
    git clone https://github.com/InetIntel/ioda-upstream-delay-new.git /ioda-upstream-delay-new && \
    rm /root/.netrc

RUN mkdir -p /ioda-upstream-delay-new/intermediate_result /ioda-upstream-delay-new/result

# Python libs
WORKDIR /ioda-upstream-delay-new
RUN python3 -m pip install -r requirements.txt

WORKDIR /ioda-upstream-delay-new
CMD ["python3", "main.py"]

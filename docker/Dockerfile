FROM ubuntu:jammy

ENV DEBIAN_FRONTEND=noninteractive
ENV APP_ENV=docker

ARG GITHUB_TOKEN

# Install necessary packages
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
    python3-setuptools \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Scamper
RUN curl -L https://www.caida.org/catalog/software/scamper/code/scamper-cvs-20240725.tar.gz -o scamper.tar.gz && \
    tar xzf scamper.tar.gz && \
    cd scamper-cvs-20240725 && \
    ./configure && \
    make && \
    make install && \
    cd .. && \
    rm -rf scamper-cvs-20240725 scamper.tar.gz

# wandio
RUN mkdir -p /src && cd /src && \
    curl -LO https://github.com/LibtraceTeam/wandio/archive/refs/tags/4.2.6-1.tar.gz && \
    tar zxf 4.2.6-1.tar.gz && \
    cd wandio-4.2.6-1 && \
    ./bootstrap.sh && \
    ./configure && \
    make && \
    make install && \
    ldconfig

# libipmeta
RUN git clone https://github.com/CAIDA/libipmeta.git /libipmeta && \
    cd /libipmeta && \
    git submodule init && \
    git submodule update && \
    ./autogen.sh && \
    ./configure && \
    make && \
    make install && \
    ldconfig

# PyIPMeta
RUN git clone https://github.com/CAIDA/pyipmeta.git /pyipmeta && \
    cd /pyipmeta && \
    python3 setup.py build_ext && \
    python3 setup.py install

# Yarrp
RUN git clone https://github.com/cmand/yarrp.git /yarrp
WORKDIR /yarrp
RUN ./bootstrap && ./configure && make

WORKDIR /

# Clone IODA-UPSTREAM-DELAY repository
RUN echo "machine github.com login $GITHUB_TOKEN" > /root/.netrc && \
    git clone https://github.com/Ghoood/iupd /ioda-upstream-delay-new && \
    rm /root/.netrc

# Create necessary directories
RUN mkdir -p /ioda-upstream-delay-new/intermediate_result /ioda-upstream-delay-new/result

# Build the parser executable
RUN cd /ioda-upstream-delay-new/parser && \
    make

# Install Python libraries
WORKDIR /ioda-upstream-delay-new
RUN python3 -m pip install -r requirements.txt

# Set the working directory back to the project root
WORKDIR /ioda-upstream-delay-new

# Default command to run your application
CMD ["python3", "main.py"]

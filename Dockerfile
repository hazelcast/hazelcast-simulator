# syntax=docker/dockerfile:1.4

##################################################
# Builder stage: compile Hazelcast Simulator
##################################################
FROM ubuntu:24.04 AS builder
WORKDIR /simulator

# Install repo tools, add PPAs, install build deps
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      software-properties-common wget gnupg ca-certificates \
 && add-apt-repository ppa:deadsnakes/ppa -y \
 && wget -qO- https://apt.releases.hashicorp.com/gpg \
      | gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
      https://apt.releases.hashicorp.com $(lsb_release -cs) main" \
      > /etc/apt/sources.list.d/hashicorp.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-21-jdk \
      maven \
      python3.11 python3.11-venv python3.11-dev python3-pip \
      libffi-dev build-essential \
      terraform \
      rsync git ssh \
 && rm -rf /var/lib/apt/lists/*

# Fetch code & set up Python venv
RUN git clone --depth=1 https://github.com/fcannizzohz/hazelcast-simulator.git . \
 && python3.11 -m venv venv

# Cache pip downloads across builds
RUN --mount=type=cache,target=/root/.cache/pip \
    . venv/bin/activate && \
    pip install --upgrade pip && \
    pip install -r requirements.txt

# Build Java & gather runtime JARs
RUN ./build \
 && mvn -f java/simulator \
        dependency:copy-dependencies \
        -DincludeScope=runtime \
        -DoutputDirectory=lib

##################################################
# Runtime stage: minimal image to run Simulator
##################################################
FROM ubuntu:24.04 AS runtime
WORKDIR /simulator

# Add PPAs & install only runtime deps (with GPG key dearmored)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      software-properties-common wget gnupg ca-certificates \
 && add-apt-repository ppa:deadsnakes/ppa -y \
 && wget -qO- https://apt.releases.hashicorp.com/gpg \
      | gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
      https://apt.releases.hashicorp.com $(lsb_release -cs) main" \
      | tee /etc/apt/sources.list.d/hashicorp.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-21-jre-headless \
      python3.11 \
      terraform \
      rsync git ssh \
 && rm -rf /var/lib/apt/lists/*

# Copy over built simulator and venv
COPY --from=builder /simulator /simulator

# Clean up Git metadata & expose CLI
RUN rm -rf /simulator/.git
ENV PATH="/simulator/venv/bin:/simulator/bin:${PATH}"

ENTRYPOINT ["perftest"]
CMD ["--help"]

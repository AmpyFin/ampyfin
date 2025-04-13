# This Dockerfile is designed for production, not development
# docker build -t ampyfin .
#
# Set environment variables with -e ENV_KEY=<value>
# Keep bash Docker container running.
# docker run -d -t --name ampyfin-cli ampyfin
# docker exec -it ampyfin-cli /bin/bash

ARG PYTHON_VERSION=3.13
FROM docker.io/library/python:$PYTHON_VERSION-slim AS base

WORKDIR /ampyfin

# Install base packages
RUN apt-get update -qq && \
    apt-get install --no-install-recommends -y nano && \
    rm -rf /var/lib/apt/lists /var/cache/apt/archives

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1 \
    # Turn off stdout and stderr buffering, for easier container logging
    PYTHONUNBUFFERED=1 \
    # Disable pip caching to reduce image size
    PIP_NO_CACHE_DIR=1

# Discard build stage to reduce size of final image
FROM base AS build

# Install packages needed to build requirements
RUN apt-get update -qq && \
    apt-get install --no-install-recommends -y curl build-essential git pkg-config && \
    rm -rf /var/lib/apt/lists /var/cache/apt/archives

# To use TA-Lib for python, you need to have the TA-Lib already installed
ARG TA_LIB_VERSION=0.6.4
RUN curl -L https://github.com/ta-lib/ta-lib/releases/download/v$TA_LIB_VERSION/ta-lib-$TA_LIB_VERSION-src.tar.gz > ta-lib-src.tar.gz && \
    tar -xzf ta-lib-src.tar.gz && \
    cd ta-lib-$TA_LIB_VERSION/ && \
    ./configure --prefix=/usr/local && \
    make && \
    make install && \
    cd .. && \
    rm -rf ta-lib-$TA_LIB_VERSION ta-lib-src.tar.gz

# Install app package requirements
COPY requirements.txt .
RUN python -m pip install -r requirements.txt

# Copy app code
COPY . .

# Final stage for app image
FROM base

# Create the final image and copy artifacts: libta, pip, app
COPY --from=build /usr/local/lib/libta-lib.so /usr/local/lib/libta-lib.so
COPY --from=build /usr/local/lib/libta-lib.so.0 /usr/local/lib/libta-lib.so.0
COPY --from=build /usr/local/lib/libta-lib.so.0.0.0 /usr/local/lib/libta-lib.so.0.0.0
COPY --from=build /usr/local/lib/python${PYTHON_VERSION%.*}/site-packages /usr/local/lib/python${PYTHON_VERSION%.*}/site-packages
COPY --from=build /ampyfin /ampyfin

# Run and own only the runtime files as a non-root user for security
RUN groupadd --system --gid 1000 ampyfin && \
    useradd ampyfin --uid 1000 --gid 1000 --create-home --shell /bin/bash && \
    chown -R ampyfin:ampyfin control.py log wandb
USER 1000:1000

# This can be overwritten at runtime
CMD ["/bin/bash"]

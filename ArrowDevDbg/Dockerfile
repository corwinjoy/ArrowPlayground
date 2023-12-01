# Use the specified base image
FROM apache/arrow-dev:amd64-ubuntu-22.04-python-3


# Install necessary dependencies
RUN apt-get update && \
    apt-get install -y \
    autoconf \
    automake \
    binutils \
    build-essential \
    clang \
    cmake \
    dos2unix \
    g++ \
    gcc \
    gdb \
    git \
    htop \
    iputils-ping \
    libtool \
    linux-tools-generic \
    linux-tools-6.2.0-36-generic \
    locales-all \
    make \
    mc \
    nano \
    net-tools \
    ninja-build \
    python3-dbg \
    python3-dev \
    rsync \
    strace \
    sudo \
    tar \
    valgrind \
    wget \
#Clean apt cache
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/* \
	&& echo
	
# Allow perf to run as non-root
RUN echo kernel.perf_event_paranoid=-1 >> /etc/sysctl.conf
RUN echo kernel.kptr_restrict=0 >> /etc/sysctl.conf
RUN sysctl --system

	
# Add a builder user to handle file sharing in Linux
ARG UID=1000
RUN useradd -m -u ${UID} -s /bin/bash builder
RUN usermod -aG sudo builder
RUN echo "builder ALL=(ALL:ALL) NOPASSWD: ALL" | sudo tee "/etc/sudoers.d/dont-prompt-builder-for-sudo-password"

# Copy our arrow build script
COPY --chown=builder:builder install_arrow.sh /install_arrow.sh

USER builder

# Command to keep the container running
CMD ["/bin/bash"]

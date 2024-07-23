# This Dockerfile is meant to create containers that replicate what the OVH nodes look like, *after* being provisioned and setup
FROM ubuntu:24.04

# Avoid prompts from apt, can always leave on since it doesn't contribute to base image size
ENV DEBIAN_FRONTEND=noninteractive

# install openssh
RUN apt-get update && \
    apt-get install -y openssh-server wget tar && \
    ## Remove package lists downloaded by apt to reduce image size
    rm -rf /var/lib/apt/lists/

# Create ipfs user, with the ssh files needed
RUN useradd -m -s /bin/bash ipfs && \
    mkdir -p /home/ipfs/.ssh && \
    # Set correct permissions on SSH files
    # 700 ensures only ipfs can read, write, and access the .ssh directory
    # 600 ensures only ipfs can read and write the authorized_keys file
    # These permissions are required by SSH for security reasons
    chmod 700 /home/ipfs/.ssh && \
    touch /home/ipfs/.ssh/authorized_keys && \
    chmod 600 /home/ipfs/.ssh/authorized_keys && \
    chown -R ipfs:ipfs /home/ipfs/.ssh

# Configure ssh with public key of the user building this image
ARG SSH_PUBLIC_KEY
RUN  mkdir -p /run/sshd && \
    ssh-keygen -A && \
    echo "PasswordAuthentication no" >> /etc/ssh/sshd_config && \
    echo "PubkeyAuthentication yes" >> /etc/ssh/sshd_config && \
    echo $SSH_PUBLIC_KEY > /home/ipfs/.ssh/authorized_keys


# Define the build platform explicitly since for downloading the appropriate kubo and ipfs-cluster-service binary, the other commonly used option would be x86_64
ARG TARGETARCH=arm64

# Download and install kubo
ENV KUBO_VERSION=0.29.0
ENV KUBO_FILE="kubo_v${KUBO_VERSION}_linux-${TARGETARCH}.tar.gz"
RUN wget https://dist.ipfs.tech/kubo/v${KUBO_VERSION}/kubo_v${KUBO_VERSION}_linux-${TARGETARCH}.tar.gz && \
    tar -xvzf "${KUBO_FILE}" && \
    cd kubo && \
    sh install.sh && \
    cd .. && \
    rm -rf "${KUBO_FILE}" kubo


# Install ipfs-cluster-service, for full service as opposed to just a follower client
ENV IPFS_CLUSTER_SERVICE_VERSION=1.1.1
ENV IPFS_CLUSTER_SERVICE_FILE="ipfs-cluster-service_v${IPFS_CLUSTER_SERVICE_VERSION}_linux-${TARGETARCH}.tar.gz"
RUN wget https://dist.ipfs.tech/ipfs-cluster-service/v${IPFS_CLUSTER_SERVICE_VERSION}/ipfs-cluster-service_v${IPFS_CLUSTER_SERVICE_VERSION}_linux-${TARGETARCH}.tar.gz && \
    tar -xvzf "${IPFS_CLUSTER_SERVICE_FILE}" && \
    mv ipfs-cluster-service/ipfs-cluster-service /usr/local/bin && \
    rm -rf "${IPFS_CLUSTER_SERVICE_FILE}" ipfs-cluster-service

# ENV CLUSTER_IPFSHTTP_NODEMULTIADDRESS=/dns4/ipfs0/tcp/5001
# ENV CLUSTER_CRDT_TRUSTEDPEERS='*'
# ENV CLUSTER_MONITORPINGINTERVAL=2s

# port for sshing in
EXPOSE 22
EXPOSE 5001
# 4001 8080 5001 9096

COPY startup.sh /usr/local/bin/startup.sh
RUN chmod +x /usr/local/bin/startup.sh
ENTRYPOINT [ "/usr/local/bin/startup.sh" ]
CMD ["/usr/sbin/sshd", "-D"]

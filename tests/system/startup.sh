#!/bin/sh
set -e

# Initialize IPFS if not already initialized
if [ ! -f /home/ipfs/.ipfs/config ]; then
    echo "Initializing IPFS..."
    su - ipfs -c "ipfs init --profile server"

    # Configure IPFS to allow external API access, so that we can see it from our host machine web browser
    su - ipfs -c 'ipfs config --json API.HTTPHeaders.Access-Control-Allow-Origin "[\"*\"]"'
    su - ipfs -c 'ipfs config --json API.HTTPHeaders.Access-Control-Allow-Methods "[\"PUT\", \"POST\", \"GET\"]"'
    # Configure IPFS API to listen on all interfaces
    su - ipfs -c 'ipfs config Addresses.API "/ip4/0.0.0.0/tcp/5001"'
    su - ipfs -c 'ipfs config Addresses.Gateway "/ip4/0.0.0.0/tcp/8080"'
else
    echo "IPFS already initialized, skipping initialization"
fi

# Start IPFS daemon
su - ipfs -c "ipfs daemon --enable-gc &"

# Initialize IPFS Cluster if not already initialized
if [ ! -f /home/ipfs/.ipfs-cluster/service.json ]; then
    echo "Initializing IPFS Cluster..."
    su - ipfs -c "ipfs-cluster-service init"
else
    echo "IPFS Cluster already initialized, skipping initialization"
fi

# Start IPFS Cluster daemon
su - ipfs -c "ipfs-cluster-service daemon &"

# Start SSH daemon
/usr/sbin/sshd -D

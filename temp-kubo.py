import json
import os
import random
import socket
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Literal

import click

from etl_scripts.grabbag import eprint

word_pool = [
    "sky",
    "blue",
    "run",
    "fast",
    "ocean",
    "deep",
    "tree",
    "green",
    "sing",
    "loud",
    "book",
    "read",
    "soft",
    "chair",
    "bright",
    "sun",
    "moon",
    "star",
    "dream",
    "fly",
    "high",
    "warm",
    "coat",
    "happy",
    "smile",
    "laugh",
    "quiet",
    "night",
    "dark",
    "light",
    "water",
    "flow",
    "stone",
    "hard",
    "path",
    "walk",
    "friend",
    "kind",
    "music",
    "play",
    "sweet",
    "cake",
    "river",
    "bend",
    "cloud",
    "white",
    "sleepy",
    "cat",
    "window",
    "open",
    "door",
    "key",
    "lock",
    "secret",
    "whisper",
    "garden",
    "grow",
    "flower",
    "red",
    "wind",
    "blow",
    "paper",
    "write",
    "story",
    "tell",
    "magic",
    "spark",
    "shine",
    "gold",
    "silver",
    "road",
    "travel",
    "ship",
    "sail",
    "bird",
    "song",
    "echo",
    "cave",
    "mountain",
    "peak",
    "snow",
    "cold",
    "fire",
    "dance",
    "jump",
    "quick",
    "shadow",
    "follow",
    "lost",
    "find",
    "map",
    "guide",
    "truth",
    "speak",
    "heart",
    "beat",
    "love",
    "share",
    "gift",
    "give",
    "time",
    "watch",
    "clock",
    "tick",
    "past",
    "future",
    "now",
    "always",
    "never",
    "often",
    "again",
    "place",
    "home",
    "away",
    "near",
    "far",
]

scratchspace: Path = (Path(__file__).parent / "scratchspace" / "temp-kubo").absolute()
os.makedirs(scratchspace, exist_ok=True)


def find_free_port() -> int:
    """Find a free port that is not the default ipfs ports of 4001, 5001, and 8080."""
    while True:
        with socket.socket() as s:
            s.bind(("", 0))  # Bind to a free port provided by the host.
            port = int(s.getsockname()[1])
            match port:
                case 4001 | 5001 | 8080:
                    continue
                case _:
                    return port


def get_ports_from_config(config_path: Path) -> tuple[int, int, int]:
    """
    Return a tuple of the ports in the format (swarm, rpc, gateway) by reading the config of an ipfs daemon.
    """
    config: dict
    with open(config_path, "r") as f:
        config = json.load(f)

    # The first swarm address usually looks like "/ip4/0.0.0.0/tcp/4001"
    swarm = int(Path(config["Addresses"]["Swarm"][0]).name)
    rpc = int(Path(config["Addresses"]["API"]).name)
    gateway = int(Path(config["Addresses"]["Gateway"]).name)

    return (swarm, rpc, gateway)


@click.command
@click.argument("kind", type=click.Choice(["swarm", "rpc", "gateway"]))
@click.argument(
    "ipfs-dir",
    type=click.Path(
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        resolve_path=True,
        path_type=Path,
    ),
)
def port(kind: Literal["swarm", "rpc", "gateway"], ipfs_dir: Path):
    """
    Get a swarm/rpc/gateway port number, given the path to an ipfs directory.
    """
    swarm, rpc, gateway = get_ports_from_config(ipfs_dir / "config")
    match kind:
        case "swarm":
            print(swarm)
        case "rpc":
            print(rpc)
        case "gateway":
            print(gateway)


@click.command
def create():
    """
    Create a temporary kubo daemon with a memorable path for its data within the scratchspace, assign it random available ports, then print its IPFS Path to stdout, as well as a space separated string representing its swarm port, rpc port, and gateway port, in that order.

     After creation of a properly configured temporary kubo location, it is up to the user to instantiate and manage the kubo daemons, as well as delete the ipfs repository once their usage is done.

     Common use will look like `export IPFS_PATH=$(uv run temp-kubo.py create)` and then running that ipfs daemon.
    """
    # Create temporary directory, set it as the IPFS Path
    # Theoretical limit is len(word_pool)**3 which should suffice for most use cases
    ipfs_path: Path
    while True:
        random_name = f"{random.choice(word_pool)}-{random.choice(word_pool)}-{random.choice(word_pool)}"
        random_path = scratchspace / random_name
        if not random_path.exists():
            ipfs_path = random_path
            break

    custom_env = os.environ.copy()
    custom_env["IPFS_PATH"] = str(ipfs_path)

    # Find free ports that are free on both the OS and also not used by any other temporarily created ipfs daemons. Scan configs for this, so that we don't conflict even if the daemon is already running
    # This has to be done before initializing so we don't get the new defaults caught up in this
    used_temp_kubo_ports: set[int] = set()
    for other_ipfs_path in scratchspace.iterdir():
        config_path = other_ipfs_path / "config"

        # The first swarm address usually looks like "/ip4/0.0.0.0/tcp/4001"
        used_swarm_port, used_rpc_port, used_gateway_port = get_ports_from_config(
            config_path
        )

        used_temp_kubo_ports.add(used_swarm_port)
        used_temp_kubo_ports.add(used_rpc_port)
        used_temp_kubo_ports.add(used_gateway_port)

    # IPFS init
    # if we don't redirect ipfs init's stdout to this python process's stderr, then programs utilizing this script will not be able to pipe this command's output directly to a IPFS_PATH variable for use
    subprocess.run(
        ["ipfs", "init", "--profile", "pebbleds"],
        check=True,
        env=custom_env,
        stdout=sys.stderr,
        stderr=sys.stderr,
    )

    swarm_port: int
    while True:
        swarm_port = find_free_port()
        if swarm_port not in used_temp_kubo_ports:
            break

    rpc_port: int
    while True:
        rpc_port = find_free_port()
        if rpc_port not in used_temp_kubo_ports:
            break

    gateway_port: int
    while True:
        gateway_port = find_free_port()
        if gateway_port not in used_temp_kubo_ports:
            break

    config_path = ipfs_path / "config"
    config: dict
    with open(config_path, "r") as f:
        config = json.load(f)

    swarm_addrs: list[str] = config["Addresses"]["Swarm"]
    new_port_swarm_addrs = [s.replace("4001", str(swarm_port)) for s in swarm_addrs]
    config["Addresses"]["Swarm"] = new_port_swarm_addrs

    rpc_multiaddr = config["Addresses"]["API"]
    gateway_multiaddr = config["Addresses"]["Gateway"]

    config["Addresses"]["API"] = rpc_multiaddr.replace("5001", str(rpc_port))
    config["Addresses"]["Gateway"] = gateway_multiaddr.replace(
        "8080", str(gateway_port)
    )

    # Add dClimate data infra nodes as bootstrap nodes
    bootstrap_addresses = config["Bootstrap"]
    bootstrap_addresses.append(
        "/ip4/15.235.14.184/udp/4001/quic-v1/p2p/12D3KooWHdZM98wcuyGorE184exFrPEJWv2btXWWSHLQaqwZXuPe"
    )
    bootstrap_addresses.append(
        "/ip4/15.235.86.198/udp/4001/quic-v1/p2p/12D3KooWGX5HDDjbdiJL2QYf2f7Kjp1Bj6QAXR5vFvLQniTKwoBR"
    )
    bootstrap_addresses.append(
        "/ip4/40.160.21.102/udp/4001/quic-v1/p2p/12D3KooWEaVCpKd2MgZeLugvwCWRSQAMYWdu6wNG6SySQsgox8k5"
    )

    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)

    print(ipfs_path)


@click.group
def cli():
    """Create and get information about temporary kubo daemons."""
    pass


cli.add_command(create)
cli.add_command(port)

if __name__ == "__main__":
    cli()

"""Offline-capable LAN service advertisement shared by both server runners."""

import asyncio
import hashlib
import ipaddress
import logging
import os
import socket

import ifaddr
from zeroconf import ServiceInfo
from zeroconf.asyncio import AsyncZeroconf


def lan_ipv4_addresses(host='0.0.0.0'):
    """Return private IPv4 interfaces without needing Internet or a DNS server."""
    networks = tuple(ipaddress.ip_network(value) for value in
                     ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16'))
    addresses = set()
    for adapter in ifaddr.get_adapters():
        for interface in adapter.ips:
            if not isinstance(interface.ip, str):
                continue
            try:
                address = ipaddress.ip_address(interface.ip)
            except ValueError:
                continue
            if address.version == 4 and any(address in network for network in networks):
                addresses.add(str(address))
    if host not in {'0.0.0.0', ''}:
        addresses.intersection_update({host})
    return sorted(addresses)


def service_info(addresses, port):
    identifier = hashlib.sha256((','.join(addresses) + ':' + str(port)).encode()).hexdigest()[:10]
    return ServiceInfo(
        '_etrakyan._tcp.local.',
        f'etrakyan-{identifier}._etrakyan._tcp.local.',
        addresses=[socket.inet_aton(address) for address in addresses],
        port=port,
        properties={b'path': b'/ping', b'name': b'etrakyan', b'ver': b'1', b'scheme': b'http'},
        server=f'etrakyan-{identifier}.local.',
    )


async def _close_registration(zeroconf, info):
    if zeroconf is None:
        return
    if info is not None:
        try:
            await zeroconf.async_unregister_service(info)
        except Exception as error:
            logging.warning('LAN service unregister failed (%s).', type(error).__name__)
    try:
        await zeroconf.async_close()
    except Exception as error:
        logging.warning('LAN discovery cleanup failed (%s).', type(error).__name__)


async def advertise_server(host=None, port=None, refresh_seconds=15):
    """Advertise the actual bind port; refresh after Wi-Fi/IP changes."""
    host = os.getenv('HOST', '0.0.0.0') if host is None else host
    port = int(os.getenv('PORT', '8000')) if port is None else port
    zeroconf = None
    info = None
    advertised = None
    try:
        while True:
            try:
                addresses = lan_ipv4_addresses(host)
                if addresses != advertised:
                    await _close_registration(zeroconf, info)
                    zeroconf = None
                    info = None
                    advertised = None
                    if addresses:
                        info = service_info(addresses, port)
                        zeroconf = AsyncZeroconf(interfaces=addresses)
                        await zeroconf.async_register_service(info, allow_name_change=True)
                        print('eTrak LAN discovery: ' + ', '.join(
                            f'http://{address}:{port}' for address in addresses), flush=True)
                    else:
                        logging.info('LAN discovery waiting for a private IPv4 interface (bind=%s).', host)
                    advertised = addresses
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logging.warning('LAN discovery unavailable (%s); will retry.', type(error).__name__)
                advertised = None
            await asyncio.sleep(refresh_seconds)
    finally:
        await _close_registration(zeroconf, info)

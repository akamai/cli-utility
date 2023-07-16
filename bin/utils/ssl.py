# https://www.pyopenssl.org/en/latest/api/crypto.html#OpenSSL.crypto.X509
# https://chromium.googlesource.com/chromium/src/+/refs/heads/main/net/tools/print_certificates.py
# https://www.sslshopper.com/certificate-decoder.html
# https://crashcourse.housegordon.org/python-subprocess.html
# https://github.com/python/cpython/blob/90f1d777177e28b6c7b8d9ba751550e373d61b0a/Lib/ssl.py#L1436
from __future__ import annotations

import socket
import ssl
import time

from cryptography import x509
from cryptography.x509.oid import NameOID
from utils import _logging as lg

logger = lg.setup_logger()


def cert_decode_pem(pem_data):
    pem_data_bytes = pem_data.encode('utf-8')
    cert = x509.load_pem_x509_certificate(pem_data_bytes)
    return cert


def get_cert(hostname: str, port: int, sni: bool, retries=2, delay=1):
    count = 0
    for attempt in range(1, retries + 1):
        try:
            conn = ssl.create_connection((hostname, port), timeout=5)
            context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
            if sni:
                sock = context.wrap_socket(conn, server_hostname=hostname)
            else:
                sock = context.wrap_socket(conn)
            der_certificate = sock.getpeercert(True)
            try:
                pem_certificate = ssl.DER_cert_to_PEM_cert(der_certificate)
            except:
                pem_certificate = ''
            try:
                cert = cert_decode_pem(pem_certificate)
                expired_dt = cert.not_valid_after.strftime('%Y-%m-%d %H:%M:%S')
                common_name = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
                logger.debug(common_name)
            except:
                expired_dt = ''
                common_name = ''
            return expired_dt, common_name, pem_certificate
        except socket.timeout as e:
            logger.debug(f'{hostname:<80} Connection {attempt}/{retries} timed out: {str(e)}')
            count += 1
            time.sleep(delay)
        except Exception as e:
            logger.error(f'{hostname:<80} Error occurred during connection: {str(e)}')
            break
    if count > 0:
        logger.error(f'{hostname:<80} Failed to establish a connection')
    return None, None, None


if __name__ == '__main__':
    pass

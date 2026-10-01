import logging
import os


def determine_client_version() -> str:
    from importlib.metadata import version, PackageNotFoundError

    try:
        version = version("driftwood")
    except PackageNotFoundError:
        try:
            version = version("feldera")
        except PackageNotFoundError:
            version = "unknown"

    return version


def requests_verify_from_env() -> str | bool:
    env_tls_insecure = os.environ.get("DRIFTWOOD_TLS_INSECURE") or os.environ.get("FELDERA_TLS_INSECURE")
    https_tls_cert = os.environ.get("DRIFTWOOD_HTTPS_TLS_CERT") or os.environ.get("FELDERA_HTTPS_TLS_CERT")

    if env_tls_insecure is not None and https_tls_cert is not None:
        logging.warning(
            "environment variables DRIFTWOOD_HTTPS_TLS_CERT and "
            + "DRIFTWOOD_TLS_INSECURE both are set."
            + "\nDRIFTWOOD_HTTPS_TLS_CERT takes priority."
        )

    if env_tls_insecure is None:
        tls_insecure = False
    else:
        tls_insecure = env_tls_insecure.strip().lower() in (
            "1",
            "true",
            "yes",
        )

    requests_verify = not tls_insecure
    if https_tls_cert is not None:
        requests_verify = https_tls_cert

    return requests_verify

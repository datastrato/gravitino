"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import re

from gravitino.constants import TIMEOUT
from gravitino.service import Service


class GravitinoClient:
    def __init__(
        self,
        host: str,
        port: int = 8090,
        prefix: str = "/api",
        timeout: int = TIMEOUT,
        debug: bool = False,
    ) -> None:
        if re.search(r"^https?:\/\/", host):
            _host = host.rstrip("/")
        else:
            _host = f"http://{host.rstrip('/')}"

        if not re.search(r"[0-9]{2,5}$", _host):
            _host = f"{_host}:{port}"

        _base_url = f"{_host}/{prefix.strip('/')}"
        self.service = Service(_base_url, timeout)
        self.debug = debug

    @property
    def version(self):
        return self.service.get_version()

    def __getattr__(self, metalake):
        return self.service.get_metalake(metalake)

    def __dir__(self):
        return ['the_first_metalake', 'metalake_demo']

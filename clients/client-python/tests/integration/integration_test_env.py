"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import logging
import os
import unittest
import subprocess
import time
import sys

import requests

from gravitino.exceptions.gravitino_runtime_exception import GravitinoRuntimeException

logger = logging.getLogger(__name__)


def get_gravitino_server_version():
    try:
        response = requests.get("http://localhost:8090/api/version")
        response.raise_for_status()  # raise an exception for bad status codes
        response.close()
        return True
    except requests.exceptions.RequestException:
        logger.warning("Failed to access the Gravitino server")
        return False


def check_gravitino_server_status() -> bool:
    gravitino_server_running = False
    for i in range(5):
        logger.info("Monitoring Gravitino server status. Attempt %s", i + 1)
        if get_gravitino_server_version():
            logger.debug("Gravitino Server is running")
            gravitino_server_running = True
            break
        else:
            logger.debug("Gravitino Server is not running")
            time.sleep(1)
    return gravitino_server_running


class IntegrationTestEnv(unittest.TestCase):
    """Provide real test environment for the Gravitino Server"""

    gravitino_startup_script = None

    @classmethod
    def setUpClass(cls):
        if os.environ.get("START_EXTERNAL_GRAVITINO") is not None:
            # Maybe Gravitino server already startup by Gradle test command or developer manual startup.
            if not check_gravitino_server_status():
                logger.error("ERROR: Can't find online Gravitino server!")
            return

        gravitino_home = os.environ.get("GRAVITINO_HOME")
        if gravitino_home is None:
            logger.error(
                "Gravitino Python client integration test must configure `GRAVITINO_HOME`"
            )
            sys.exit(0)

        cls.gravitino_startup_script = os.path.join(gravitino_home, "bin/gravitino.sh")
        if not os.path.exists(cls.gravitino_startup_script):
            logger.error(
                "Can't find Gravitino startup script: %s, "
                "Please execute `./gradlew compileDistribution -x test` in the Gravitino project root "
                "directory.",
                cls.gravitino_startup_script,
            )
            sys.exit(0)

        logger.info("Starting integration test environment...")

        # Start Gravitino Server
        result = subprocess.run(
            [cls.gravitino_startup_script, "start"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.stdout:
            logger.info("stdout: %s", result.stdout)
        if result.stderr:
            logger.info("stderr: %s", result.stderr)

        if not check_gravitino_server_status():
            logger.error("ERROR: Can't start Gravitino server!")
            sys.exit(0)

    @classmethod
    def tearDownClass(cls):
        if os.environ.get("START_EXTERNAL_GRAVITINO") is not None:
            return

        logger.info("Stop integration test environment...")
        result = subprocess.run(
            [cls.gravitino_startup_script, "stop"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.stdout:
            logger.info("stdout: %s", result.stdout)
        if result.stderr:
            logger.info("stderr: %s", result.stderr)

        gravitino_server_running = True
        for i in range(5):
            logger.debug("Monitoring Gravitino server status. Attempt %s", i + 1)
            if check_gravitino_server_status():
                logger.debug("Gravitino server still running")
                time.sleep(1)
            else:
                logger.debug("Stop Gravitino server successes!")
                gravitino_server_running = False
                break

        if gravitino_server_running:
            logger.error("Can't stop Gravitino server!")

    @classmethod
    def restart_server(cls):
        logger.info("Restarting Gravitino server...")
        gravitino_home = os.environ.get("GRAVITINO_HOME")
        gravitino_startup_script = os.path.join(gravitino_home, "bin/gravitino.sh")
        if not os.path.exists(gravitino_startup_script):
            raise GravitinoRuntimeException(
                f"Can't find Gravitino startup script: {gravitino_startup_script}, "
                "Please execute `./gradlew compileDistribution -x test` in the Gravitino "
                "project root directory."
            )

        # Restart Gravitino Server
        env_vars = os.environ.copy()
        env_vars["HADOOP_USER_NAME"] = "datastrato"
        result = subprocess.run(
            [gravitino_startup_script, "restart"],
            env=env_vars,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.stdout:
            logger.info("stdout: %s", result.stdout)
        if result.stderr:
            logger.info("stderr: %s", result.stderr)

        if not check_gravitino_server_status():
            raise GravitinoRuntimeException("ERROR: Can't start Gravitino server!")

    @classmethod
    def _append_server_hadoop_conf(cls, config):
        logger.info("Append server hadoop conf.")
        gravitino_home = os.environ.get("GRAVITINO_HOME")
        if gravitino_home is None:
            raise GravitinoRuntimeException("Cannot find GRAVITINO_HOME env.")
        hadoop_conf_path = f"{gravitino_home}/catalogs/hadoop/conf/hadoop.conf"
        if not os.path.exists(hadoop_conf_path):
            raise GravitinoRuntimeException(
                f"Hadoop conf file is not found at `{hadoop_conf_path}`."
            )

        with open(hadoop_conf_path, mode="a", encoding="utf-8") as f:
            for key, value in config.items():
                f.write(f"\n{key} = {value}")

    @classmethod
    def _reset_server_hadoop_conf(cls, config):
        logger.info("Reset server hadoop conf.")
        gravitino_home = os.environ.get("GRAVITINO_HOME")
        if gravitino_home is None:
            raise GravitinoRuntimeException("Cannot find GRAVITINO_HOME env.")
        hadoop_conf_path = f"{gravitino_home}/catalogs/hadoop/conf/hadoop.conf"
        if not os.path.exists(hadoop_conf_path):
            raise GravitinoRuntimeException(
                f"Hadoop conf file is not found at `{hadoop_conf_path}`."
            )
        filtered_lines = []
        with open(hadoop_conf_path, mode="r", encoding="utf-8") as file:
            lines = file.readlines()
            for line in lines:
                line = line.strip()
                find_config = False
                for key, value in config.items():
                    key_to_delete = f"{key} = {value}"
                    if line.startswith(key_to_delete):
                        find_config = True
                        break
                if find_config:
                    continue
                filtered_lines.append(line)

        with open(hadoop_conf_path, mode="w", encoding="utf-8") as file:
            for line in filtered_lines:
                file.write(line + "\n")

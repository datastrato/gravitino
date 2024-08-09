#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#   under the License.

import random
import string
import unittest
from unittest.mock import patch

from gravitino import GravitinoClient, NameIdentifier
from gravitino.api.base_fileset_data_operation_ctx import BaseFilesetDataOperationCtx
from gravitino.api.client_type import ClientType
from gravitino.api.fileset_data_operation import FilesetDataOperation

from tests.unittests import mock_base


def generate_unique_random_string(length):
    characters = string.ascii_letters + string.digits
    random_string = "".join(random.sample(characters, length))
    return random_string


@mock_base.mock_data
class TestFilesetCatalog(unittest.TestCase):
    _local_base_dir_path: str = "file:/tmp/fileset"
    _fileset_dir: str = (
        f"{_local_base_dir_path}/{generate_unique_random_string(10)}/fileset_catalog/tmp"
    )

    def test_get_fileset_context(self, *mock_methods):
        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            return_value=mock_base.mock_get_fileset_context_response(
                "test_get_context",
                f"{self._fileset_dir}/test_get_context",
                f"{self._fileset_dir}/test_get_context/test1",
            ),
        ):
            client = GravitinoClient(
                uri="http://localhost:9090", metalake_name="test_metalake"
            )
            catalog = client.load_catalog("test_catalog")
            ctx = BaseFilesetDataOperationCtx(
                sub_path="/test1",
                operation=FilesetDataOperation.MKDIRS,
                client_type=ClientType.PYTHON_GVFS,
            )
            context = catalog.as_fileset_catalog().get_fileset_context(
                NameIdentifier.of("test_schema", "test_fileset"),
                ctx,
            )
            self.assertEqual(context.fileset().name(), "test_get_context")
            self.assertEqual(
                context.fileset().storage_location(),
                f"{self._fileset_dir}/test_get_context",
            )
            self.assertEqual(
                context.actual_path(), f"{self._fileset_dir}/test_get_context/test1"
            )

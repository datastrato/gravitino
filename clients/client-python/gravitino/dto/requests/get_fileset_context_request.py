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

from dataclasses import dataclass, field
from dataclasses_json import config

from gravitino.api.client_type import ClientType
from gravitino.api.fileset_data_operation import FilesetDataOperation
from gravitino.rest.rest_message import RESTRequest


@dataclass
class GetFilesetContextRequest(RESTRequest):
    """Request to represent to get a fileset context."""

    _sub_path: str = field(metadata=config(field_name="subPath"))
    _operation: FilesetDataOperation = field(metadata=config(field_name="operation"))
    _client_type: ClientType = field(metadata=config(field_name="clientType"))

    def __init__(
        self, sub_path: str, operation: FilesetDataOperation, client_type: ClientType
    ):
        self._sub_path = sub_path
        self._operation = operation
        self._client_type = client_type
        self.validate()

    def validate(self):
        assert self._sub_path is not None, "subPath is required"
        assert self._operation is not None, "operation is required"
        assert self._client_type is not None, "clientType is required"

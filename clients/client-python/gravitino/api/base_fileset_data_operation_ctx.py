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

from gravitino.api.client_type import ClientType
from gravitino.api.fileset_data_operation import FilesetDataOperation
from gravitino.api.fileset_data_operation_ctx import FilesetDataOperationCtx


class BaseFilesetDataOperationCtx(FilesetDataOperationCtx):
    """Base implementation of FilesetDataOperationCtx."""

    _sub_path: str
    _operation: FilesetDataOperation
    _client_type: ClientType

    def __init__(
        self, sub_path: str, operation: FilesetDataOperation, client_type: ClientType
    ):
        assert sub_path is not None
        assert operation is not None
        assert client_type is not None
        self._sub_path = sub_path
        self._operation = operation
        self._client_type = client_type

    def sub_path(self) -> str:
        return self._sub_path

    def operation(self) -> FilesetDataOperation:
        return self._operation

    def client_type(self) -> ClientType:
        return self._client_type

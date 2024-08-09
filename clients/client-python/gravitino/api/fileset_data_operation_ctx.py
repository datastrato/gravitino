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

from abc import ABC, abstractmethod

from gravitino.api.client_type import ClientType
from gravitino.api.fileset_data_operation import FilesetDataOperation


class FilesetDataOperationCtx(ABC):
    """An interface representing a fileset data operation context. This interface defines some
    information need to report to the server.
    """

    @abstractmethod
    def sub_path(self) -> str:
        """The sub path which is operated by the data operation.

        Returns:
            the sub path which is operated by the data operation.
        """
        pass

    @abstractmethod
    def operation(self) -> FilesetDataOperation:
        """The data operation type.

        Returns:
            the data operation type.
        """
        pass

    @abstractmethod
    def client_type(self) -> ClientType:
        """The client type of the data operation.

        Returns:
            the client type of the data operation.
        """
        pass

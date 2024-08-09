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

from gravitino.api.fileset import Fileset


class FilesetContext(ABC):
    """An interface representing a fileset context with an existing fileset. This
    interface defines some contextual information related to Fileset that can be passed.
    """

    @abstractmethod
    def fileset(self) -> Fileset:
        """The fileset object.

        Returns:
            the fileset object.
        """
        pass

    @abstractmethod
    def actual_path(self) -> str:
        """The actual storage path after processing.

        Returns:
            the actual storage path after processing.
        """
        pass

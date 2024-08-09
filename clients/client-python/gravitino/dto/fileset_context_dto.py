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

from abc import ABC
from dataclasses import dataclass, field

from dataclasses_json import config, DataClassJsonMixin

from gravitino.api.fileset_context import FilesetContext
from gravitino.dto.fileset_dto import FilesetDTO


@dataclass
class FilesetContextDTO(FilesetContext, DataClassJsonMixin, ABC):
    """Represents a Fileset Context DTO (Data Transfer Object)."""

    _fileset: FilesetDTO = field(metadata=config(field_name="fileset"))
    _actual_path: str = field(metadata=config(field_name="actualPath"))

    def fileset(self) -> FilesetDTO:
        return self._fileset

    def actual_path(self) -> str:
        return self._actual_path

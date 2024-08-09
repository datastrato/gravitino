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

from enum import Enum


class FilesetDataOperation(Enum):
    """An enum class containing fileset data operations that supported."""

    LIST_STATUS = 1
    GET_FILE_STATUS = 2
    EXISTS = 3
    RENAME = 4
    APPEND = 5
    CREATE = 6
    DELETE = 7
    OPEN = 8
    MKDIRS = 9
    CREATED_TIME = 10
    MODIFIED_TIME = 11
    COPY_FILE = 12
    CAT_FILE = 13
    GET_FILE = 14
    UNKNOWN = 15

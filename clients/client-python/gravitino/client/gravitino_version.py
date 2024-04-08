from dataclasses import dataclass

from gravitino.dto.version_dto import VersionDTO


@dataclass
class GravitinoVersion(VersionDTO):
    """Gravitino version information."""
    def __init__(self, versionDTO):
        super().__init__(versionDTO.version, versionDTO.compile_date, versionDTO.git_commit)

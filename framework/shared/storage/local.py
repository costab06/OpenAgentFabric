import hashlib
from pathlib import Path

from shared.models import ArtifactRef, ArtifactType


class LocalArtifactStore:
    def __init__(self, root: str):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def write_bytes(
        self,
        relative_path: str,
        data: bytes,
        artifact_type: ArtifactType,
        version: int = 1,
    ) -> ArtifactRef:
        path = self.root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

        checksum = hashlib.sha256(data).hexdigest()

        return ArtifactRef(
            artifact_id=relative_path,
            artifact_type=artifact_type,
            uri=str(path),
            version=version,
            checksum=checksum,
        )

    def read_bytes(self, artifact: ArtifactRef) -> bytes:
        return Path(artifact.uri).read_bytes()
        
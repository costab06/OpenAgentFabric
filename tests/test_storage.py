from shared.models import ArtifactType
from shared.storage.local import LocalArtifactStore


def test_local_artifact_store_round_trip(tmp_path):
    store = LocalArtifactStore(str(tmp_path))

    artifact = store.write_bytes(
        relative_path="sec/test/filing.html",
        data=b"hello sec",
        artifact_type=ArtifactType.RAW_DOCUMENT,
    )

    assert artifact.checksum is not None
    assert store.read_bytes(artifact) == b"hello sec"
    
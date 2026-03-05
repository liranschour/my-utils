from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class BlockDesc:
    block_hash: int
    # Physical address / descriptor passed to NIXL for transfer
    addr: int
    size: int


class SecondaryPillar(ABC):

    @abstractmethod
    def load(self, job_id: str, block_hashes: list[int], peer_id: str) -> None:
        """Decoder: initiate load of KV blocks from a remote Prefiller peer."""
        ...

    @abstractmethod
    def save(self, job_id: str, block_descs: list[BlockDesc]) -> None:
        """Prefiller: save KV blocks to CPU cache for later retrieval."""
        ...

    @abstractmethod
    def get_finished(self, job_ids: list[str]) -> list[str]:
        """Return the subset of job_ids whose load/save has completed."""
        ...

    @abstractmethod
    def abort(self, job_id: str) -> None:
        """Abort an in-progress load or save operation."""
        ...


class SecondaryPillars:
    """Registry that dispatches load/save/abort to all registered pillars."""

    def __init__(self) -> None:
        self._pillars: list[SecondaryPillar] = []

    def register(self, pillar: SecondaryPillar) -> None:
        self._pillars.append(pillar)

    def load(self, job_id: str, block_hashes: list[int], peer_id: str) -> None:
        for pillar in self._pillars:
            pillar.load(job_id, block_hashes, peer_id)

    def save(self, job_id: str, block_descs: list[BlockDesc]) -> None:
        for pillar in self._pillars:
            pillar.save(job_id, block_descs)

    def get_finished(self, job_ids: list[str]) -> list[str]:
        # A job is finished only when all pillars report it as finished
        finished: set[str] = set(job_ids)
        for pillar in self._pillars:
            finished &= set(pillar.get_finished(list(finished)))
        return list(finished)

    def abort(self, job_id: str) -> None:
        for pillar in self._pillars:
            pillar.abort(job_id)

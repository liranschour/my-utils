import pytest
from src.secondary_pillar import BlockDesc, SecondaryPillar, SecondaryPillars


class MockPillar(SecondaryPillar):
    def __init__(self, name: str):
        self.name = name
        self.loads: list = []
        self.saves: list = []
        self.aborts: list = []
        self._finished: set[str] = set()

    def load(self, job_id: str, block_hashes: list[int], peer_id: str) -> None:
        self.loads.append((job_id, block_hashes, peer_id))

    def save(self, job_id: str, block_descs: list[BlockDesc]) -> None:
        self.saves.append((job_id, block_descs))

    def get_finished(self, job_ids: list[str]) -> list[str]:
        return [j for j in job_ids if j in self._finished]

    def abort(self, job_id: str) -> None:
        self.aborts.append(job_id)

    def mark_finished(self, job_id: str) -> None:
        self._finished.add(job_id)


def test_register_and_dispatch_load():
    pillars = SecondaryPillars()
    p1, p2 = MockPillar("p1"), MockPillar("p2")
    pillars.register(p1)
    pillars.register(p2)

    pillars.load("job1", [1, 2, 3], "peer-a")

    assert p1.loads == [("job1", [1, 2, 3], "peer-a")]
    assert p2.loads == [("job1", [1, 2, 3], "peer-a")]


def test_register_and_dispatch_save():
    pillars = SecondaryPillars()
    p1, p2 = MockPillar("p1"), MockPillar("p2")
    pillars.register(p1)
    pillars.register(p2)

    descs = [BlockDesc(block_hash=1, addr=0x1000, size=4096)]
    pillars.save("job2", descs)

    assert p1.saves == [("job2", descs)]
    assert p2.saves == [("job2", descs)]


def test_abort_propagates_to_all():
    pillars = SecondaryPillars()
    p1, p2 = MockPillar("p1"), MockPillar("p2")
    pillars.register(p1)
    pillars.register(p2)

    pillars.abort("job3")

    assert p1.aborts == ["job3"]
    assert p2.aborts == ["job3"]


def test_get_finished_requires_all_pillars():
    pillars = SecondaryPillars()
    p1, p2 = MockPillar("p1"), MockPillar("p2")
    pillars.register(p1)
    pillars.register(p2)

    p1.mark_finished("job1")
    p1.mark_finished("job2")
    p2.mark_finished("job2")

    # job1: only p1 finished, job2: both finished
    result = set(pillars.get_finished(["job1", "job2"]))
    assert result == {"job2"}


def test_no_pillars_registered():
    pillars = SecondaryPillars()
    pillars.load("job1", [1], "peer")
    pillars.save("job1", [])
    pillars.abort("job1")
    assert pillars.get_finished(["job1"]) == ["job1"]

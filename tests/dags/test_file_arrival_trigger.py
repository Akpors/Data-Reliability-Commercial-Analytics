"""FileArrivalTrigger (runs in the Airflow container: it subclasses Airflow's BaseEventTrigger)."""

import asyncio

from drct.orchestration.triggers import FileArrivalTrigger


async def test_trigger_fires_on_matching_file_and_leaves_it(tmp_path):
    (tmp_path / "sales_2026-05-01.csv").write_text("x")
    trig = FileArrivalTrigger(str(tmp_path), "sales_*.csv", poll_interval=0.01)
    event = await anext(trig.run())
    assert event.payload["files"] == ["sales_2026-05-01.csv"]
    assert (tmp_path / "sales_2026-05-01.csv").exists()


def test_trigger_serialize_round_trip():
    path, kwargs = FileArrivalTrigger("/d", "*.csv", 1.0).serialize()
    assert path == "drct.orchestration.triggers.FileArrivalTrigger" and kwargs == {
        "directory": "/d",
        "pattern": "*.csv",
        "poll_interval": 1.0,
    }


async def test_trigger_payload_names_directory_and_sorted_files(tmp_path):
    for name in ("sales_2026-05-02.csv", "sales_2026-05-01.csv"):
        (tmp_path / name).write_text("x")
    gen = FileArrivalTrigger(str(tmp_path), "sales_*.csv", poll_interval=0.01).run()
    event = await anext(gen)
    await gen.aclose()
    assert event.payload == {
        "directory": str(tmp_path),
        "files": ["sales_2026-05-01.csv", "sales_2026-05-02.csv"],
    }


async def test_trigger_fires_once_per_batch_then_only_for_new_files(tmp_path):
    (tmp_path / "sales_2026-05-01.csv").write_text("x")
    gen = FileArrivalTrigger(str(tmp_path), "sales_*.csv", poll_interval=0.01).run()
    await anext(gen)

    nxt = asyncio.ensure_future(anext(gen))
    await asyncio.sleep(0.1)  # ~10 polls with only the already-seen file present
    assert not nxt.done()

    (tmp_path / "sales_2026-05-02.csv").write_text("x")
    event = await asyncio.wait_for(nxt, timeout=2)
    await gen.aclose()
    assert event.payload["files"] == ["sales_2026-05-02.csv"]


async def test_trigger_refires_for_a_resent_file_with_the_same_name(tmp_path):
    landed = tmp_path / "sales_2026-05-01.csv"
    landed.write_text("x")
    gen = FileArrivalTrigger(str(tmp_path), "sales_*.csv", poll_interval=0.01).run()
    await anext(gen)

    landed.unlink()  # the loader moved it to _processed/
    nxt = asyncio.ensure_future(anext(gen))
    await asyncio.sleep(0.1)
    assert not nxt.done()

    landed.write_text("y")  # re-sent under the same name
    event = await asyncio.wait_for(nxt, timeout=2)
    await gen.aclose()
    assert event.payload["files"] == ["sales_2026-05-01.csv"]


async def test_trigger_ignores_subfolders_other_names_and_temp_files(tmp_path):
    (tmp_path / "_processed").mkdir()
    (tmp_path / "_processed" / "sales_2026-05-01.csv").write_text("x")
    (tmp_path / "sales_2026-05-02.csv").mkdir()  # a directory, even with a matching name
    (tmp_path / ".sales_2026-05-03.csv.tmp").write_text("x")
    (tmp_path / "payments_2026-05-01.parquet").write_text("x")
    gen = FileArrivalTrigger(str(tmp_path), "sales_*.csv", poll_interval=0.01).run()

    nxt = asyncio.ensure_future(anext(gen))
    await asyncio.sleep(0.1)
    assert not nxt.done()

    (tmp_path / "sales_2026-05-04.csv").write_text("x")
    event = await asyncio.wait_for(nxt, timeout=2)
    await gen.aclose()
    assert event.payload["files"] == ["sales_2026-05-04.csv"]
    assert (tmp_path / "_processed" / "sales_2026-05-01.csv").exists()


async def test_trigger_waits_for_a_missing_directory(tmp_path):
    folder = tmp_path / "sales"
    gen = FileArrivalTrigger(str(folder), "sales_*.csv", poll_interval=0.01).run()

    nxt = asyncio.ensure_future(anext(gen))
    await asyncio.sleep(0.1)
    assert not nxt.done()

    folder.mkdir()
    (folder / "sales_2026-05-01.csv").write_text("x")
    event = await asyncio.wait_for(nxt, timeout=2)
    await gen.aclose()
    assert event.payload["files"] == ["sales_2026-05-01.csv"]

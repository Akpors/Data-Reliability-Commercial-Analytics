import json
from pathlib import Path

from drct.monitoring.lineage import downstream_of, node_for_asset

MANIFEST = json.loads(
    (Path(__file__).resolve().parents[1] / "fixtures" / "dbt" / "manifest.json").read_text()
)


def test_blast_radius_reaches_exposure():
    down = downstream_of(MANIFEST, "source.drct.raw.payments")
    assert "model.drct.fct_payments" in down and "exposure.drct.control_tower" in down
    assert not any(n.startswith("test.") for n in down)


def test_downstream_is_sorted_unique_and_excludes_the_start_node():
    down = downstream_of(MANIFEST, "source.drct.raw.payments")
    assert down == sorted(set(down))
    assert "source.drct.raw.payments" not in down
    assert all(n.startswith(("model.", "exposure.")) for n in down)


def test_downstream_of_unknown_id_is_empty():
    assert downstream_of(MANIFEST, "model.drct.nope") == []


def test_downstream_handles_cycles():
    manifest = {"child_map": {"model.a": ["model.b"], "model.b": ["model.a"]}}
    assert downstream_of(manifest, "model.a") == ["model.b"]


def test_node_for_asset():
    assert node_for_asset("raw.sales") == "source.drct.raw.sales"
    assert node_for_asset("model.drct.fct_sales") == "model.drct.fct_sales"
    assert node_for_asset("test.drct.unique_x.abc") == "test.drct.unique_x.abc"
    assert node_for_asset("source.drct.raw.sales") == "source.drct.raw.sales"

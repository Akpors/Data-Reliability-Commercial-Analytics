"""dbt lineage helpers for incident blast radius."""

__all__ = ["downstream_of", "node_for_asset"]

_NODE_PREFIXES = ("model.", "test.", "source.")


def downstream_of(manifest: dict, unique_id: str) -> list[str]:
    """Return the sorted, unique ``model.*`` and ``exposure.*`` ids reachable from ``unique_id``.

    Walks ``child_map`` transitively (tests are traversed but never reported) and excludes the
    starting node itself.
    """
    child_map = manifest.get("child_map", {})
    seen = {unique_id}
    stack = [unique_id]
    while stack:
        for child in child_map.get(stack.pop(), []):
            if child not in seen:
                seen.add(child)
                stack.append(child)
    seen.discard(unique_id)
    return sorted(n for n in seen if n.startswith(("model.", "exposure.")))


def node_for_asset(asset: str) -> str:
    """Map an asset name such as ``raw.sales`` to its dbt source id ``source.drct.raw.sales``.

    Ids that already start with ``model.``, ``test.`` or ``source.`` pass through unchanged.
    """
    if asset.startswith(_NODE_PREFIXES):
        return asset
    return f"source.drct.{asset}"

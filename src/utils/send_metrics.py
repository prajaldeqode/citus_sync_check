import os
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

registry = CollectorRegistry()
push_gateway_url: str = str(os.environ.get("PUSH_GATEWAY_URL"))
fetch_feature_duration_ms = Gauge(
    "subgraph_arbitrum_to_citus_sync_difference",
    "Sync Difference in Citus and Subgraph Arbitrum database",
    ["table_name"],
    registry=registry,
)


def expose_difference_in_vid(table_name: str, difference: int):
    fetch_feature_duration_ms.labels(table_name=table_name).set(difference)
    push_to_gateway(push_gateway_url, "symmetric-ds", registry=registry)
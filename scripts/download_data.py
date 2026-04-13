#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "datasets.json"
CHUNK_SIZE = 1024 * 1024


def log(message: str) -> None:
    print(message, flush=True)


def load_config() -> dict[str, dict[str, Any]]:
    with CONFIG_PATH.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return payload["datasets"]


def build_headers() -> dict[str, str]:
    headers = {"User-Agent": "dallas-crime-intelligence/0.1"}
    app_token = os.getenv("SOCRATA_APP_TOKEN")
    if app_token:
        headers["X-App-Token"] = app_token
    return headers


def fetch_json(url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    request_url = url
    if params:
        request_url = f"{url}?{urlencode(params, doseq=True)}"

    request = Request(request_url, headers=build_headers())
    with urlopen(request, timeout=180) as response:
        return json.load(response)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def stream_to_file(url: str, output_path: Path) -> None:
    ensure_parent(output_path)
    request = Request(url, headers=build_headers())

    total_bytes = 0
    with urlopen(request, timeout=180) as response, output_path.open("wb") as handle:
        while True:
            chunk = response.read(CHUNK_SIZE)
            if not chunk:
                break
            handle.write(chunk)
            total_bytes += len(chunk)

    log(f"Saved {output_path.relative_to(ROOT)} ({total_bytes / (1024 * 1024):.1f} MB)")


def download_json_api(name: str, dataset: dict[str, Any], force: bool) -> None:
    output_path = ROOT / dataset["output"]
    if output_path.exists() and not force:
        log(f"Skipping {name}: {dataset['output']} already exists. Use --force to replace it.")
        return

    ensure_parent(output_path)
    log(f"Downloading {name} from JSON API...")
    payload = fetch_json(dataset["url"])
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")
    log(f"Saved {output_path.relative_to(ROOT)} ({len(payload)} records)")


def download_socrata_snapshot_csv(name: str, dataset: dict[str, Any], force: bool) -> None:
    output_path = ROOT / dataset["output"]
    if output_path.exists() and not force:
        log(f"Skipping {name}: {dataset['output']} already exists. Use --force to replace it.")
        return

    log(f"Downloading {name} from Socrata snapshot API...")
    stream_to_file(dataset["url"], output_path)


def download_arcgis_geojson(name: str, dataset: dict[str, Any], force: bool) -> None:
    output_path = ROOT / dataset["output"]
    if output_path.exists() and not force:
        log(f"Skipping {name}: {dataset['output']} already exists. Use --force to replace it.")
        return

    service_url = dataset["service_url"]
    where = dataset.get("where", "1=1")
    batch_size = int(dataset.get("batch_size", 2000))

    log(f"Counting features for {name}...")
    count_payload = fetch_json(
        service_url,
        {
            "where": where,
            "returnCountOnly": "true",
            "f": "json",
        },
    )
    total = int(count_payload.get("count", 0))
    log(f"Fetching {total} features for {name}...")

    features: list[dict[str, Any]] = []
    offset = 0
    while True:
        page = fetch_json(
            service_url,
            {
                "where": where,
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": 4326,
                "f": "geojson",
                "resultOffset": offset,
                "resultRecordCount": batch_size,
            },
        )

        page_features = page.get("features", [])
        if not page_features:
            break

        features.extend(page_features)
        offset += len(page_features)
        log(f"  fetched {offset}/{total or '?'} features")

        if len(page_features) < batch_size:
            break

    payload = {
        "type": "FeatureCollection",
        "name": dataset.get("description", name),
        "features": features,
    }

    ensure_parent(output_path)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")

    log(f"Saved {output_path.relative_to(ROOT)} ({len(features)} features)")


def download_dataset(name: str, dataset: dict[str, Any], force: bool) -> None:
    kind = dataset["kind"]

    if kind in {"json_api", "socrata_json"}:
        download_json_api(name, dataset, force)
        return

    if kind == "socrata_snapshot_csv":
        download_socrata_snapshot_csv(name, dataset, force)
        return

    if kind == "arcgis_geojson":
        download_arcgis_geojson(name, dataset, force)
        return

    if kind == "manual_config_required":
        log(f"Skipping {name}: {dataset.get('notes', 'No endpoint configured yet.')}")
        return

    raise ValueError(f"Unsupported dataset kind: {kind}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Dallas crime project datasets from APIs.")
    parser.add_argument(
        "--dataset",
        action="append",
        help="Dataset key from config/datasets.json. Repeat to download multiple datasets.",
    )
    parser.add_argument(
        "--include-disabled",
        action="store_true",
        help="Include disabled config entries. Useful after you finish configuring a placeholder dataset.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite files that already exist.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_config()

    selected_names = args.dataset or list(config)
    missing = [name for name in selected_names if name not in config]
    if missing:
        log(f"Unknown dataset(s): {', '.join(missing)}")
        return 2

    for name in selected_names:
        dataset = config[name]
        if not dataset.get("enabled", True) and not args.include_disabled:
            log(f"Skipping {name}: disabled in config. Use --include-disabled after configuring it.")
            continue

        try:
            download_dataset(name, dataset, args.force)
        except (HTTPError, URLError, TimeoutError) as exc:
            log(f"Failed to download {name}: {exc}")
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

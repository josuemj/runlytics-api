"""
Convert a JSON file (list of objects) into a flattened CSV.
"""

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple, Union


Scalar = Union[str, int, float, bool, None]


def is_scalar(value: Any) -> bool:
    """
    Check if the value is a scalar type.
    """
    return value is None or isinstance(value, (str, int, float, bool))


def flatten_json(value: Any, prefix: str, out: Dict[str, Union[Scalar, str]]) -> None:
    """
    Flatten a JSON object into a flat dictionary with prefixed keys."""
    if is_scalar(value):
        out[prefix] = value
        return

    if isinstance(value, dict):
        for key, nested in value.items():
            nested_prefix = f"{prefix}_{key}" if prefix else str(key)
            flatten_json(nested, nested_prefix, out)
        return

    if isinstance(value, (list, tuple)):
        if all(is_scalar(item) for item in value):
            for idx, item in enumerate(value):
                out[f"{prefix}_{idx}"] = item
        else:
            out[prefix] = json.dumps(value, ensure_ascii=False)
        return

    out[prefix] = str(value)


def to_rows(payload: Any) -> List[Dict[str, Any]]:
    """
    Convert the JSON payload into a list of rows (dictionaries).
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        return payload["data"]
    if isinstance(payload, dict):
        return [payload]
    raise TypeError(f"Unsupported JSON root type: {type(payload).__name__}")


def flatten_rows(rows: Iterable[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Flatten a list of JSON objects into a list of flat dictionaries and collect all column names.
    """
    flattened: List[Dict[str, Any]] = []
    columns: List[str] = []
    seen = set()

    for row in rows:
        out: Dict[str, Any] = {}
        for key, value in row.items():
            flatten_json(value, str(key), out)
        flattened.append(out)

        for col in out.keys():
            if col not in seen:
                seen.add(col)
                columns.append(col)

    return flattened, columns


def load_json_files(input_path: Path) -> List[Path]:
    """
    Return a sorted list of JSON files from a file path or directory path.
    """
    if input_path.is_file():
        return [input_path]
    if input_path.is_dir():
        return sorted(input_path.glob("*.json"))
    raise FileNotFoundError(f"Input path not found: {input_path}")


def main() -> int:
    """
    Main function to parse arguments and convert JSON to CSV."""
    parser = argparse.ArgumentParser(
        description="Convert a JSON file (list of objects) into a flattened CSV."
    )
    parser.add_argument(
        "input",
        type=Path,
        help="Path to a JSON file or a folder with .json files (e.g. data/page_1.json or data/)",
    )
    parser.add_argument(
        "output",
        nargs="?",
        type=Path,
        help="Path to the output CSV file (default: input.csv for files, input/merged.csv for folders)",
    )
    parser.add_argument(
        "--source-col",
        default=None,
        help="If set, add a column with the source JSON filename (e.g. --source-col source_file).",
    )
    args = parser.parse_args()

    input_path: Path = args.input
    json_files = load_json_files(input_path)
    if not json_files:
        raise SystemExit(f"No .json files found in folder: {input_path}")

    if args.output:
        output_path: Path = args.output
    else:
        output_path = (
            input_path.with_suffix(".csv")
            if input_path.is_file()
            else input_path / "merged.csv"
        )

    all_flattened: List[Dict[str, Any]] = []
    all_columns: List[str] = []
    seen_columns = set()

    for json_path in json_files:
        payload = json.loads(json_path.read_text(encoding="utf-8"))
        rows = to_rows(payload)
        flattened, columns = flatten_rows(rows)

        if args.source_col:
            for row in flattened:
                row[args.source_col] = json_path.name
            if args.source_col not in columns:
                columns.append(args.source_col)

        all_flattened.extend(flattened)
        for col in columns:
            if col not in seen_columns:
                seen_columns.add(col)
                all_columns.append(col)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_flattened)

    print(f"Wrote {len(all_flattened)} rows from {len(json_files)} file(s) to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

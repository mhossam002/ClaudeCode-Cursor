"""
diagnose.py — Standalone script to print table map before first run.
Run: python diagnose.py "path/to/Form 10-Q.docx"

Prints index, dimensions, and header rows for every table in the document
so you can verify the 0-based table indices assumed in engine.py.
"""
import sys
import json
import io

# Allow running from project root
import os
sys.path.insert(0, os.path.dirname(__file__))

# Force UTF-8 output on Windows to avoid codec errors with special chars
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from rollforward.docx_parser import load_document, extract_table_map


def main():
    if len(sys.argv) < 2:
        print("Usage: python diagnose.py <path-to-docx>")
        sys.exit(1)

    path = sys.argv[1]
    print(f"Loading: {path}\n")
    doc = load_document(path)
    table_map = extract_table_map(doc)

    print(f"Found {len(table_map)} tables\n")
    print("=" * 70)
    for entry in table_map:
        print(f"Table {entry['index']:>3}  |  {entry['rows']} rows × {entry['cols']} cols")
        for r_idx, header_row in enumerate(entry["headers"]):
            print(f"  Row {r_idx}: {header_row}")
        print("-" * 70)

    # Also emit JSON for piping / UI use
    out_path = os.path.join(os.path.dirname(path), "table_map.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(table_map, f, indent=2)
    print(f"\nJSON written to: {out_path}")


if __name__ == "__main__":
    main()

"""Safely rename legacy local data layers to Bronze / Silver / Gold."""
from __future__ import annotations

import argparse
from pathlib import Path

from config.settings import DATA_DIR

LAYERS = (("raw", "bronze"), ("processed", "silver"), ("warehouse", "gold"))


def migrate(dry_run: bool = False) -> list[tuple[Path, Path]]:
    """Rename legacy layer directories without overwriting existing destinations."""
    moves = [(DATA_DIR / old, DATA_DIR / new) for old, new in LAYERS
             if (DATA_DIR / old).exists()]
    conflicts = [(old, new) for old, new in moves if new.exists()]
    if conflicts:
        details = ", ".join(f"{old} -> {new} (destination exists)" for old, new in conflicts)
        raise FileExistsError(f"Resolve data-layer migration conflicts before continuing: {details}")

    if not dry_run:
        for old, new in moves:
            old.rename(new)
    return moves


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate local data directories to Bronze/Silver/Gold")
    parser.add_argument("--dry-run", action="store_true", help="List moves without changing directories")
    args = parser.parse_args()
    for old, new in migrate(dry_run=args.dry_run):
        action = "Would move" if args.dry_run else "Moved"
        print(f"{action}: {old} -> {new}")


if __name__ == "__main__":
    main()

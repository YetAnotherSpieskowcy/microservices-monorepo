import argparse
import json
from typing import Any


def prepare_dataset() -> dict[Any, Any]:
    return {}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("output_file")
    args = parser.parse_args()

    output = prepare_dataset()

    with open(args.output_file, "w", encoding="utf-8") as fp:
        json.dump(output, fp)


if __name__ == "__main__":
    main()

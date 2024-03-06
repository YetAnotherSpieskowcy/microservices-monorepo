from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import os
import os.path
import pickle
import time
from types import TracebackType
from typing import Any, TextIO, Self

import requests
from requests_toolbelt.utils import dump


log = logging.getLogger()


def get_gql_query_by_name(name: str) -> str:
    with open(os.path.join(GQL_DIR, f"{name}.gql"), encoding="utf-8") as fp:
        return fp.read()


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
GQL_DIR = os.path.join(SCRIPT_DIR, "gql")
GQL_API_URL = "https://www.itaka.pl/graphql"
GQL_GET_DESTINATIONS_QUERY = get_gql_query_by_name("get_destinations")
GQL_GET_RATES_QUERY = get_gql_query_by_name("get_rates")
GQL_GET_TRANSPORT_DETAILS_QUERY = get_gql_query_by_name("get_transport_details")
RATE_PARAMS = {
    "supplier": "itaka",
    "language": "pl",
    "currency": "PLN",
    "adultsNumber": 2,
}


@dataclasses.dataclass
class Country:
    #: URL-friendly unique identifier, i.e. "wlochy" for "Włochy"
    identifier: str
    #: Country name
    title: str
    #: Mapping of cities - {city_id: city}
    cities: dict[str, City] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class City:  # "province" in the API
    #: URL-friendly unique identifier, i.e. "paryz" for "Paryż"
    identifier: str
    #: City name
    title: str


@dataclasses.dataclass(kw_only=True)
class RawDataSet:
    destination_regions: list[dict[str, Any]] | None = None
    rates: list[dict[str, Any]] | None = None
    transport_details: dict[str, dict[str, Any]] | None = None


@dataclasses.dataclass(kw_only=True)
class DataSet:
    countries: dict[str, Country]
    rates: dict[str, dict[str, Any]]

    @classmethod
    def from_raw(cls, raw_dataset: RawDataSet) -> Self:
        return DataSet(
            countries=cls._parse_destination_regions(raw_dataset.destination_regions),
            rates=cls._parse_rates(raw_dataset.rates, raw_dataset.transport_details),
        )

    @classmethod
    def _parse_destination_regions(
        cls, regions: list[dict[str, Any]] | None
    ) -> dict[str, Country]:
        if regions is None:
            raise TypeError()

        countries = {}
        for region in regions:
            if region["type"] == "country" and not region["value"].endswith("-narty"):
                countries[region["value"]] = Country(region["value"], region["title"])

        for region in regions:
            if region["type"] != "province":
                continue
            country = countries.get(region["parent"])
            if country is None:
                continue
            country.cities[region["value"]] = City(region["value"], region["title"])

        return countries

    @classmethod
    def _parse_rates(
        cls,
        raw_rates: list[dict[str, Any]] | None,
        raw_transport_details: dict[str, dict[str, Any]] | None,
    ) -> dict[str, dict[str, Any]]:
        if raw_rates is None or raw_transport_details is None:
            raise TypeError()

        # TODO: extract actually important data...
        rates = {}
        for rate in raw_rates:
            copied_rate = pickle.loads(pickle.dumps(rate))
            rates[rate["id"]] = copied_rate
            copied_rate["detailedSegments"] = raw_transport_details.get(rate["id"])
        return rates


class Scraper:
    def __init__(self, output_dir: str, *, skip_scraping: bool = False) -> None:
        self.output_dir = output_dir
        self._skip_scraping = skip_scraping
        self.raw_dataset = RawDataSet()
        self._session = requests.Session()
        self._raw_dataset_fp: TextIO | None = None

    @property
    def raw_dataset_path(self) -> str:
        return os.path.join(self.output_dir, "raw_dataset.json")

    @property
    def parsed_dataset_path(self) -> str:
        return os.path.join(self.output_dir, "parsed_dataset.json")

    def __enter__(self) -> Self:
        os.makedirs(self.output_dir, exist_ok=True)
        if self._skip_scraping:
            with open(self.raw_dataset_path, encoding="utf-8") as fp:
                self.raw_dataset = RawDataSet(**json.load(fp))
        else:
            self._raw_dataset_fp = open(self.raw_dataset_path, "x", encoding="utf-8")

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._session.close()
        if self._raw_dataset_fp is not None:
            json.dump(
                dataclasses.asdict(self.raw_dataset), self._raw_dataset_fp, indent=4
            )

    def run(self) -> DataSet:
        if not self._skip_scraping:
            self._prepare_raw_dataset()

        return self._generate_dataset()

    def _prepare_raw_dataset(self) -> None:
        self.raw_dataset.destination_regions = self._get_destination_regions()
        self.raw_dataset.rates = self._get_rates()
        transport_details = self.raw_dataset.transport_details = {}
        rate_count = len(self.raw_dataset.rates)
        for idx, rate in enumerate(self.raw_dataset.rates):
            logging.info("Getting rate's transport details (%s out of %s)", idx, rate_count)
            # who cares about ratelimits :)
            transport_details[rate["id"]] = self._get_transport_details(rate["id"])

    def _generate_dataset(self) -> DataSet:
        return DataSet.from_raw(self.raw_dataset)

    def _gql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        try_count = 5
        for try_number in range(try_count):
            resp = self._session.post(
                GQL_API_URL,
                json={
                    "query": query,
                    "variables": variables,
                },
            )

            try:
                resp.raise_for_status()
            except requests.HTTPError:
                log.error(
                    "HTTP request failed with status %s\n%s",
                    resp.status_code,
                    dump.dump_all(resp).decode("utf-8"),
                )
                # reliability of the API varies
                if (try_number + 1) < try_count and resp.status_code == 502:
                    log.warning("Retrying request...")
                    time.sleep(1 + try_number * 2)
                    continue
                raise

            break

        log.debug("HTTP request succeeded\n%s", dump.dump_all(resp).decode("utf-8"))

        return resp.json()["data"]

    def _get_destination_regions(self) -> list[dict[str, Any]]:
        """Get countries and their cities."""
        logging.info("Getting destination regions...")
        data = self._gql(GQL_GET_DESTINATIONS_QUERY, {"rateParams": RATE_PARAMS})
        return data["properties"]["destinationRegions"]

    def _get_rates(self) -> list[dict[str, Any]]:
        rates: list[dict[str, Any]] = []
        rate_count = 1
        page = 0
        per_page = 100
        while len(rates) < rate_count:
            logging.info("Getting rates (%s out of %s)", len(rates), rate_count)
            data = self._gql(
                GQL_GET_RATES_QUERY,
                {
                    "rateParams": RATE_PARAMS,
                    "skip": page * per_page,
                    "take": per_page,
                    "order": "popularity",
                },
            )
            rate_count = data["rates"]["ratesCount"]
            current_page_rates = data["rates"]["list"]
            if not current_page_rates:
                break
            rates.extend(current_page_rates)
            page += 1

        return rates

    def _get_transport_details(self, id_: str) -> Any:
        data = self._gql(GQL_GET_TRANSPORT_DETAILS_QUERY, {**RATE_PARAMS, "id": id_})
        segments = data["rate"]["segments"]
        return segments


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skip-scraping",
        action="store_true",
        help=(
            "Skip data scraping."
            " This requires that the output_dir contains valid raw_dataset.json"
        ),
    )
    parser.add_argument("output_dir")
    args = parser.parse_args()

    logging.basicConfig(filename="scraper.log", encoding="utf-8", level=logging.DEBUG)
    stdout_logger = logging.StreamHandler()
    stdout_logger.setLevel(logging.INFO)
    log.addHandler(stdout_logger)

    with Scraper(args.output_dir, skip_scraping=args.skip_scraping) as scraper:
        parsed_dataset = scraper.run()

    with open(scraper.parsed_dataset_path, "w", encoding="utf-8") as fp:
        json.dump(dataclasses.asdict(parsed_dataset), fp, indent=4)


if __name__ == "__main__":
    main()

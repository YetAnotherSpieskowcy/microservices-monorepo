from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import os
import os.path
import pickle
import re
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
GQL_GET_PRODUCT_CONTENT_QUERY = get_gql_query_by_name("get_product_content")
RATE_PARAMS = {
    "supplier": "itaka",
    "language": "pl",
    "currency": "PLN",
    "adultsNumber": 2,
}
ROOM_FIRST_SPLIT_PATTERN = re.compile(r"[ \-]os\b")
ROOM_SECOND_SPLIT_PATTERN = re.compile(r"[ \-]dost")
ROOM_DIGIT_PATTERN = re.compile(r"\d")


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


@dataclasses.dataclass
class RoutePoint:
    """Bus stop or airport."""

    #: Route point's code
    code: str
    #: The city where the route point is located; may be inaccurate for bus stops
    city: str


@dataclasses.dataclass
class Route:
    #: Route point where the route starts
    origin: RoutePoint
    #: Intermediate route points (empty for direct flights/bus routes)
    via: tuple[RoutePoint, ...]
    #: Route point where the route ends
    destination: RoutePoint

    @property
    def key(self) -> str:
        return "-".join(
            point.code for point in (self.origin, *self.via, self.destination)
        )


@dataclasses.dataclass(frozen=True)
class Meal:
    #: Unique identifier, i.e. "A" for "All inclusive 24h"
    identifier: str
    #: Name of the meal option
    title: str


@dataclasses.dataclass(frozen=True)
class Room:
    #: Room's name
    title: str
    #: Number of beds without counting the extras that can be added
    bed_count: int
    #: Number of extra beds that can be added
    extra_bed_count: int


@dataclasses.dataclass
class Hotel:
    #: Hotel's name
    title: str
    #: Hotel's star count * 10 (so 35 instead of 3.5 for 3 and a half stars)
    hotel_rating: int
    #: Destination country that the offer should be for if the hotel is offered
    destination_country_id: str
    #: Destination city that the offer should be for if the hotel is offered
    destination_city_id: str | None
    #: Latitude
    latitude: float
    #: Longitude
    longitude: float
    #: Available meal options
    meals: list[Meal] = dataclasses.field(default_factory=list)
    #: Available rooms
    rooms: list[Room] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class Tour:
    """Tour is a general product description."""


@dataclasses.dataclass(kw_only=True)
class RawDataSet:
    destination_regions: list[dict[str, Any]] | None = None
    rates: list[dict[str, Any]] | None = None
    transport_details: dict[str, list[dict[str, Any]]] | None = None
    all_product_content: dict[str, dict[str, Any]] | None = None


@dataclasses.dataclass(kw_only=True)
class DataSet:
    countries: dict[str, Country]
    airports: dict[str, RoutePoint]
    flight_routes: dict[str, Route]
    bus_stops: dict[str, RoutePoint]
    bus_routes: dict[str, Route]
    hotels: dict[str, Hotel]
    meals: dict[str, Meal]
    rates: dict[str, dict[str, Any]]

    @classmethod
    def from_raw(cls, raw_dataset: RawDataSet) -> Self:
        dataset = DataSet(
            countries={},
            airports={},
            flight_routes={},
            bus_stops={},
            bus_routes={},
            hotels={},
            meals={},
            rates={},
        )
        dataset._parse_destination_regions(raw_dataset.destination_regions)
        dataset._parse_rates(
            raw_dataset.rates,
            raw_dataset.transport_details,
            raw_dataset.all_product_content,
        )
        return dataset

    def _parse_destination_regions(self, regions: list[dict[str, Any]] | None) -> None:
        if regions is None:
            raise TypeError()

        countries = self.countries
        for region in regions:
            if region["type"] == "country" and not region["value"].endswith("-narty"):
                countries[region["value"]] = Country(region["value"], region["title"])

        for region in regions:
            if region["type"] != "province":
                continue
            country = countries[region["parent"].removesuffix("-narty")]
            country.cities[region["value"]] = City(region["value"], region["title"])

    def _parse_transport_details(
        self,
        product_content: dict[str, Any],
        transport_details: dict[str, Any] | None,
        routes: dict[str, Route],
        route_points: dict[str, RoutePoint],
    ) -> None:
        if transport_details is None:
            return
        origin = RoutePoint(
            code=transport_details["from"]["code"],
            city=transport_details["from"]["city"],
        )
        destination = RoutePoint(
            code=transport_details["to"]["code"],
            city=transport_details["to"]["city"],
        )
        if origin.code == origin.city:
            origin.city = product_content["destination"]["country"]["title"]
        if destination.code == destination.city:
            destination.city = product_content["destination"]["country"]["title"]
        via = tuple(
            RoutePoint(code=point["code"], city=point["city"])
            for point in transport_details["via"]
        )
        route = Route(origin, via, destination)
        routes[route.key] = route
        route_points[origin.code] = origin
        route_points[destination.code] = destination
        for point in via:
            route_points[point.code] = point

    def _parse_hotel(self, segment: dict[str, Any]) -> Hotel | None:
        content = segment["content"]
        title = content["title"]
        hotel_rating = content["hotelRating"]
        destination_country_id = content["destinations"]["country"]["id"].removesuffix(
            "-narty"
        )
        destination_city_id = (content["destinations"]["province"] or {}).get("id")
        geolocation = content["geolocation"]
        if geolocation is None:
            # skip the odd ones
            return None
        latitude = geolocation["lat"]
        longitude = geolocation["lng"]
        hotel = self.hotels.setdefault(
            title,
            Hotel(
                title=title,
                hotel_rating=hotel_rating,
                destination_country_id=destination_country_id,
                destination_city_id=destination_city_id,
                latitude=latitude,
                longitude=longitude,
            ),
        )
        meal = Meal(
            identifier=segment["meal"]["id"],
            title=segment["meal"]["title"],
        )
        self.meals[meal.identifier] = meal
        if meal not in hotel.meals:
            hotel.meals.append(meal)

        return hotel

    def _parse_room(self, hotel: Hotel, section: dict[str, Any]) -> None:
        bed_count = 1
        extra_bed_count = 0
        it = (section["title"], *section["lists"][0]["items"])

        # approximate the number of beds (not 100% accurate but good enough)
        for unstructured_info in it:
            # regular beds
            match = ROOM_FIRST_SPLIT_PATTERN.search(unstructured_info)
            if match is None:
                continue
            regular_start, regular_end = match.span()
            bed_count = max(
                bed_count,
                max(
                    (
                        int(m.group())
                        for m in ROOM_DIGIT_PATTERN.finditer(
                            unstructured_info, 0, regular_start
                        )
                    ),
                    default=bed_count,
                ),
            )

            # extra beds
            match = ROOM_SECOND_SPLIT_PATTERN.search(unstructured_info, regular_end)
            if match is None:
                # no extra beds
                break
            extra_start, _ = match.span()
            extra_bed_count = max(
                extra_bed_count,
                1,
                max(
                    (
                        int(m.group())
                        for m in ROOM_DIGIT_PATTERN.finditer(
                            unstructured_info, regular_end, extra_start
                        )
                    ),
                    default=extra_bed_count,
                ),
            )
            break
        else:
            return

        room = Room(
            title=section["title"],
            bed_count=bed_count,
            extra_bed_count=extra_bed_count,
        )
        if room not in hotel.rooms:
            hotel.rooms.append(room)

    def _parse_rooms(self, hotel: Hotel, product_content: dict[str, Any]) -> None:
        for description in product_content["descriptions"]:
            if description["id"] != "rooms":
                continue
            for section in description["sections"]:
                self._parse_room(hotel, section)

    def _cleanup_incomplete_hotels(self) -> None:
        to_remove = []
        for hotel in self.hotels.values():
            if not hotel.rooms or not hotel.meals:
                to_remove.append(hotel)
        for hotel in to_remove:
            del self.hotels[hotel.title]

    def _parse_rates(
        self,
        raw_rates: list[dict[str, Any]] | None,
        raw_transport_details: dict[str, list[dict[str, Any]]] | None,
        raw_all_product_content: dict[str, dict[str, Any]] | None,
    ) -> None:
        if (
            raw_rates is None
            or raw_transport_details is None
            or raw_all_product_content is None
        ):
            raise TypeError()

        for rate in raw_rates:
            copied_rate = pickle.loads(pickle.dumps(rate))
            self.rates[rate["id"]] = copied_rate
            detailed_segments = raw_transport_details.get(rate["id"])
            product_content = raw_all_product_content[rate["id"]]
            copied_rate["detailedSegments"] = detailed_segments
            copied_rate["productContent"] = product_content

            for segment in detailed_segments or []:
                if segment["type"] == "flight":
                    self._parse_transport_details(
                        product_content,
                        segment["transportDetails"],
                        self.flight_routes,
                        self.airports,
                    )
                elif segment["type"] == "bus":
                    self._parse_transport_details(
                        product_content,
                        segment["transportDetails"],
                        self.bus_routes,
                        self.bus_stops,
                    )

            hotel = None
            for segment in rate["segments"] or []:
                if segment["type"] == "hotel":
                    hotel = self._parse_hotel(segment)
                    if hotel is not None:
                        break

            if hotel is None:
                continue

            if product_content is None:
                continue

            self._parse_rooms(hotel, product_content)

        self._cleanup_incomplete_hotels()


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
        all_product_content = self.raw_dataset.all_product_content = {}
        rate_count = len(self.raw_dataset.rates)
        for idx, rate in enumerate(self.raw_dataset.rates):
            logging.info(
                "Getting rate's transport details (%s out of %s)", idx, rate_count
            )
            # who cares about ratelimits :)
            transport_details[rate["id"]] = self._get_transport_details(rate["id"])
            logging.info(
                "Getting rate's product content (%s out of %s)", idx, rate_count
            )
            all_product_content[rate["id"]] = self._get_product_content(
                rate["supplierObjectId"]
            )

    def _generate_dataset(self) -> DataSet:
        return DataSet.from_raw(self.raw_dataset)

    def _request(
        self,
        url: str,
        *,
        json: dict[str, Any] | None = None,
    ) -> requests.Response:
        try_count = 5
        for try_number in range(try_count):
            resp = self._session.post(url, json=json)

            try:
                resp.raise_for_status()
            except requests.HTTPError:
                log.error(
                    "HTTP request failed with status %s\n%s",
                    resp.status_code,
                    dump.dump_all(resp).decode("utf-8"),
                )
                # reliability of the API varies
                if (try_number + 1) < try_count and resp.status_code >= 500:
                    log.warning("Retrying request...")
                    time.sleep(1 + try_number * 2)
                    continue
                raise

            break

        log.debug("HTTP request succeeded\n%s", dump.dump_all(resp).decode("utf-8"))

        return resp

    def _gql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        return self._request(
            GQL_API_URL,
            json={
                "query": query,
                "variables": variables,
            },
        ).json()["data"]

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
        segments = (data.get("rate") or {}).get("segments")
        if segments is None:
            log.warning("Unable to get transport details for %s", id_)
        return segments

    def _get_product_content(self, supplier_object_id: str) -> Any:
        data = self._gql(
            GQL_GET_PRODUCT_CONTENT_QUERY,
            {**RATE_PARAMS, "supplierObjectId": supplier_object_id},
        )
        content = (data.get("content") or {}).get("newContent")
        if content is None:
            log.warning("Unable to get product content for %s", supplier_object_id)
        return content


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

# NOTE: generating SQL queries the way it's done here is not a good idea
# in normal scenarios, it's only done like that here to generate sample SQL scripts.
from __future__ import annotations

import argparse
import contextlib
import datetime
import json
import logging
import random
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any, NamedTuple, TextIO

from faker import Faker
from psycopg import sql


log = logging.getLogger()

SQL_QUERIES_INSERT_EVENT = """

INSERT INTO events (id, entity_id, event_name, data)
VALUES (
    {event_id},
    {id},
    {event_name},
    {data}
);
"""

MONGO_QUERIES_INSERT_SNAPSHOT = """
db.snapshots.insertOne({document});
"""

ENTITY_TYPE_BUS_STOP = "BusStop"
ENTITY_TYPE_BUS_ROUTE = "BusRoute"
ENTITY_TYPE_AIRPORT = "Airport"
ENTITY_TYPE_FLIGHT_ROUTE = "FlightRoute"
ENTITY_TYPE_COUNTRY = "Country"
ENTITY_TYPE_CITY = "City"
ENTITY_TYPE_MEAL = "Meal"
ENTITY_TYPE_HOTEL = "Hotel"
ENTITY_TYPE_TOUR = "Tour"

AGE_CHOICES = (0, 3, 10, 18)
EARLIEST_DATE = datetime.date(2024, 5, 21)
LATEST_DATE = EARLIEST_DATE + datetime.timedelta(days=60)


class SnapshotIds(NamedTuple):
    entity_id: str
    last_event_id: int


class Snapshot(NamedTuple):
    ids: SnapshotIds
    entity_type: str
    data: dict[str, Any]


def event_blob_to_sql(event: dict[str, Any]) -> str:
    result = sql.quote(json.dumps(event))
    if result.startswith(" E'"):
        return result[1:]
    return result


def _mongo_insert(fp: TextIO, snapshot: Snapshot) -> SnapshotIds:
    document = {
        "entity_id": snapshot.ids.entity_id,
        "entity_type": snapshot.entity_type,
        "last_event_id": snapshot.ids.last_event_id,
        "data": snapshot.data,
    }
    fp.write(MONGO_QUERIES_INSERT_SNAPSHOT.format(document=json.dumps(document, indent=2)))
    return snapshot.ids


class App:
    def __init__(self) -> None:
        self._args = self._parse_args()
        self._uuid_rand = random.Random(44)
        self._last_id = 0
        self.__i = 50
        self._airport_ids: dict[str, SnapshotIds] = {}
        self._bus_stop_ids: dict[str, SnapshotIds] = {}
        self._country_ids: dict[str, SnapshotIds] = {}
        self._city_ids: dict[str, SnapshotIds] = {}
        self._meal_ids: dict[str, SnapshotIds] = {}
        self._room_ids: dict[str, SnapshotIds] = {}
        self._hotel_ids: dict[str, SnapshotIds] = {}
        self._tour_ids: dict[str, SnapshotIds] = {}
        with open(self._args.input_file, encoding="utf-8") as fp:
            self._data = json.load(fp)
        self._output_dir = Path(self._args.output_dir)
        self._output_dir.mkdir(exist_ok=True)
        self._mongo_dir = self._output_dir / "mongo"
        self._mongo_dir.mkdir(exist_ok=True)
        self._sql_dir = self._output_dir / "sql"
        self._sql_dir.mkdir(exist_ok=True)

    def _parse_args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser()
        parser.add_argument("input_file")
        parser.add_argument("output_dir")
        return parser.parse_args()

    def _setup_logging(self) -> None:
        logging.basicConfig(
            filename="generator.log", encoding="utf-8", level=logging.DEBUG
        )
        stdout_logger = logging.StreamHandler()
        stdout_logger.setLevel(logging.INFO)
        log.addHandler(stdout_logger)

    def next_id(self) -> int:
        self._last_id += 1
        return self._last_id

    def _i(self) -> str:
        i = self.__i
        self.__i += 1
        return f"{i}".rjust(2, "0")

    def _reset_i(self) -> None:
        self.__i = 50

    def _uuid4(self) -> str:
        return str(uuid.UUID(int=self._uuid_rand.getrandbits(128), version=4))

    def _sql_insert(
        self,
        fp: TextIO,
        *,
        entity_type: str,
        event_name: str,
        data: dict[str, Any],
    ) -> Snapshot:
        id_ = self._uuid4()
        event_id = self.next_id()
        fp.write(
            SQL_QUERIES_INSERT_EVENT.format(
                event_id=event_id,
                event_name=sql.quote(event_name),
                id=sql.quote(id_),
                data=event_blob_to_sql(data),
            )
        )
        return Snapshot(
            SnapshotIds(id_, event_id),
            entity_type,
            data,
        )

    def run(self) -> None:
        self._setup_logging()
        self._reset_i()
        self._generate_airport_queries()
        self._generate_flight_route_queries()
        self._generate_bus_stop_queries()
        self._generate_bus_route_queries()
        self._generate_country_city_queries()
        self._generate_meal_queries()
        self._generate_hotel_queries()

        self._generate_tour_queries()

    @contextlib.contextmanager
    def _open(self, name: str) -> Iterator[tuple[TextIO, TextIO]]:
        header = f"Sample {name} data"
        i = self._i()
        sql_fp = open(self._sql_dir / f"{i}_{name}.sql", "w", encoding="utf-8")
        try:
            sql_fp.write(f"-- {header}\n-- @generated")
            mongo_fp = open(self._mongo_dir / f"{i}_{name}.js", "w", encoding="utf-8")
            try:
                mongo_fp.write(
                    f"// {header}\n"
                    "// @generated\n"
                    'db = db.getSiblingDB("rsww_184529");\n'
                )
                yield (sql_fp, mongo_fp)
            finally:
                mongo_fp.close()
        finally:
            sql_fp.close()

    def _generate_airport_queries(self) -> None:
        with self._open("airports") as (sql_fp, mongo_fp):
            for airport in self._data["airports"].values():
                self._airport_ids[airport["code"]] = _mongo_insert(
                    mongo_fp,
                    self._sql_insert(
                        sql_fp,
                        entity_type=ENTITY_TYPE_AIRPORT,
                        event_name="AirportCreated",
                        data=airport,
                    ),
                )

    def _generate_flight_route_queries(self) -> None:
        rand = random.Random(420)
        with self._open("flight_routes") as (sql_fp, mongo_fp):
            for flight_route in self._data["flight_routes"].values():
                reservation_limit = rand.randint(200, 400)
                reservation_count = rand.randint(
                    reservation_limit - 6, reservation_limit - 1
                )
                _mongo_insert(
                    mongo_fp,
                    self._sql_insert(
                        sql_fp,
                        entity_type=ENTITY_TYPE_FLIGHT_ROUTE,
                        event_name="FlightRouteCreated",
                        data={
                            "origin_airport_id": self._airport_ids[
                                flight_route["origin"]["code"]
                            ].entity_id,
                            "via_airport_ids": [
                                self._airport_ids[airport["code"]].entity_id
                                for airport in flight_route["via"]
                            ],
                            "destination_airport_id": self._airport_ids[
                                flight_route["destination"]["code"]
                            ].entity_id,
                            "reservation_count": reservation_count,
                            "reservation_limit": reservation_limit,
                        },
                    ),
                )

    def _generate_bus_stop_queries(self) -> None:
        with self._open("bus_stops") as (sql_fp, mongo_fp):
            for stop in self._data["bus_stops"].values():
                self._bus_stop_ids[stop["code"]] = _mongo_insert(
                    mongo_fp,
                    self._sql_insert(
                        sql_fp,
                        entity_type=ENTITY_TYPE_BUS_STOP,
                        event_name="BusStopCreated",
                        data=stop,
                    ),
                )

    def _generate_bus_route_queries(self) -> None:
        rand = random.Random(420)
        with self._open("bus_routes") as (sql_fp, mongo_fp):
            for bus_route in self._data["bus_routes"].values():
                reservation_limit = rand.randint(20, 40)
                reservation_count = rand.randint(
                    reservation_limit - 6, reservation_limit - 1
                )
                _mongo_insert(
                    mongo_fp,
                    self._sql_insert(
                        sql_fp,
                        entity_type=ENTITY_TYPE_BUS_ROUTE,
                        event_name="BusRouteCreated",
                        data={
                            "origin_bus_stop_id": self._bus_stop_ids[
                                bus_route["origin"]["code"]
                            ].entity_id,
                            "via_bus_stop_ids": [
                                self._bus_stop_ids[stop["code"]].entity_id
                                for stop in bus_route["via"]
                            ],
                            "destination_bus_stop_id": self._bus_stop_ids[
                                bus_route["destination"]["code"]
                            ].entity_id,
                            "reservation_count": reservation_count,
                            "reservation_limit": reservation_limit,
                        },
                    )
                )

    def _generate_country_city_queries(self) -> None:
        with self._open("countries_and_cities") as (sql_fp, mongo_fp):
            for country in self._data["countries"].values():
                country_ids = self._country_ids[country["identifier"]] = _mongo_insert(
                    mongo_fp,
                    self._sql_insert(
                        sql_fp,
                        entity_type=ENTITY_TYPE_COUNTRY,
                        event_name="CountryCreated",
                        data={
                            "title": country["title"],
                        },
                    ),
                )

                for city in country["cities"].values():
                    self._city_ids[city["identifier"]] = _mongo_insert(
                        mongo_fp,
                        self._sql_insert(
                            sql_fp,
                            entity_type=ENTITY_TYPE_CITY,
                            event_name="CityCreated",
                            data={
                                "title": city["title"],
                                "country_id": country_ids.entity_id,
                            },
                        ),
                    )

    def _generate_meal_queries(self) -> None:
        with self._open("meals") as (sql_fp, mongo_fp):
            for meal in self._data["meals"].values():
                self._meal_ids[meal["identifier"]] = _mongo_insert(
                    mongo_fp,
                    self._sql_insert(
                        sql_fp,
                        entity_type=ENTITY_TYPE_MEAL,
                        event_name="MealCreated",
                        data={
                            "title": meal["title"],
                        },
                    ),
                )

    def _generate_hotel_queries(self) -> None:
        rand = random.Random(420)
        rand2 = random.Random(421)
        with self._open("hotels") as (sql_fp, mongo_fp):
            for hotel in self._data["hotels"].values():
                reservation_limit = rand2.randint(20, 100)
                reservation_count = rand2.randint(
                    reservation_limit - 6, reservation_limit - 1
                )
                minimum_age = rand2.choice(AGE_CHOICES)
                data = {
                    **hotel,
                    "meals": [
                        self._meal_ids[meal["identifier"]].entity_id
                        for meal in hotel["meals"]
                    ],
                    "destination_country_id": self._country_ids[
                        hotel["destination_country_id"]
                    ].entity_id,
                    "destination_city_id": (
                        hotel["destination_city_id"]
                        and self._city_ids[hotel["destination_city_id"]].entity_id
                    ),
                    "reservation_count": reservation_count,
                    "reservation_limit": reservation_limit,
                    "minimum_age": minimum_age,
                }
                if len(data["rooms"]) == 1:
                    # a lot of hotels only have a single room type
                    # and we don't want for it to be hard to find a hotel
                    # with room options
                    data["rooms"].append(
                        {"title": "deluxe", "bed_count": 2, "extra_bed_count": 1}
                        if data["rooms"][0]["title"] == "standardowy"
                        else {
                            "title": "standardowy",
                            "bed_count": 2,
                            "extra_bed_count": 1,
                        }
                    )
                elif len(data["rooms"]) == 0:
                    # a lot of hotels only have a single room type
                    # and we don't want for it to be hard to find a hotel
                    # with room options
                    data["rooms"] = [
                        {"title": "deluxe", "bed_count": 2, "extra_bed_count": 1},
                        {"title": "standardowy", "bed_count": 2, "extra_bed_count": 1},
                    ]
                data["max_people_per_reservation"] = max(
                    room["bed_count"] + room["extra_bed_count"]
                    for room in data["rooms"]
                )
                if len(data["meals"]) < 2:
                    # the vast majority of hotels only have a single meal type
                    # and we don't want for it to be hard to find a hotel
                    # with meal options
                    meals = self._data["meals"].copy()
                    del meals[hotel["meals"][0]["identifier"]]
                    population = list(meals.values())
                    k = rand.randint(1, 2) + (1 - len(data["meals"]))
                    for meal in rand.sample(population, k):
                        data["meals"].append(
                            self._meal_ids[meal["identifier"]].entity_id
                        )
                self._hotel_ids[hotel["title"]] = _mongo_insert(
                    mongo_fp,
                    self._sql_insert(
                        sql_fp,
                        entity_type=ENTITY_TYPE_HOTEL,
                        event_name="HotelCreated",
                        data=data,
                    ),
                )

    def _generate_tour_queries(self) -> None:
        # a.k.a. general product descriptions
        rand = random.Random(420)
        fake = Faker()
        fake.seed_instance(421)
        tours = []
        for rate in self._data["rates"].values():
            hotel_ids = next(
                (
                    self._hotel_ids.get(segment["content"]["title"])
                    for segment in rate["segments"]
                    if segment["type"] == "hotel"
                ),
                None,
            )
            if hotel_ids is None:
                # it's a trip without specific hotels attached, let's just select
                # a random hotel matching the country
                country_id = rate["productContent"]["destination"]["country"]["id"]
                population = [
                    self._hotel_ids[hotel["title"]]
                    for hotel in self._data["hotels"].values()
                    if hotel["destination_country_id"] == country_id
                ]
                try:
                    hotel_ids = rand.choice(population)
                except IndexError:
                    # just give up...
                    continue
            duration = rate["duration"]["days"]
            start_date = fake.date_between(EARLIEST_DATE, LATEST_DATE)
            end_date = start_date + datetime.timedelta(days=duration)
            tour = {
                "duration": duration,
                "title": rate["productContent"]["title"],
                "description": rate["productContent"]["initialDescription"],
                "photos": [photo["url"] for photo in rate["productContent"]["photos"]],
                "geolocation": rate["productContent"]["geolocation"],
                "hotel": hotel_ids.entity_id,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            }
            tours.append(tour)

        with self._open("tours") as (sql_fp, mongo_fp):
            for tour in tours:
                tour_ids = _mongo_insert(
                    mongo_fp,
                    self._sql_insert(
                        sql_fp,
                        entity_type=ENTITY_TYPE_TOUR,
                        event_name="TourCreated",
                        data=tour,
                    )
                )
                self._tour_ids[tour_ids.entity_id] = tour_ids


def main() -> None:
    app = App()
    app.run()


if __name__ == "__main__":
    main()

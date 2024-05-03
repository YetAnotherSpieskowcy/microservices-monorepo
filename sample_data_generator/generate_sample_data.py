# NOTE: generating SQL queries the way it's done here is not a good idea
# in normal scenarios, it's only done like that here to generate sample SQL scripts.
from __future__ import annotations

import argparse
import json
import logging
from uuid import uuid4
from pathlib import Path
from typing import Any, TextIO

from psycopg import sql


log = logging.getLogger()

QUERIES_INSERT_AGGREGATE_AND_EVENT = """

WITH aggregate AS (
    INSERT INTO {service_name}_aggregates (id, type)
    VALUES ({id}, {type})
    RETURNING id, version
)

INSERT INTO {service_name}_events (aggregate_id, version, data)
VALUES (
    (SELECT id FROM aggregate),
    (SELECT version + 1 FROM aggregate),
    {data}
);
"""

SERVICE_BUS = "bus"
SERVICE_FLIGHT = "flight"
SERVICE_HOTEL = "hotel"
SERVICE_TOUR_OFFER = "tour_offer"
AGGREGATE_TYPE_BUS_STOP = sql.quote("BusStop")
AGGREGATE_TYPE_BUS_ROUTE = sql.quote("BusRoute")
AGGREGATE_TYPE_AIRPORT = sql.quote("Airport")
AGGREGATE_TYPE_FLIGHT_ROUTE = sql.quote("FlightRoute")
AGGREGATE_TYPE_COUNTRY = sql.quote("Country")
AGGREGATE_TYPE_CITY = sql.quote("City")
AGGREGATE_TYPE_MEAL = sql.quote("Meal")
AGGREGATE_TYPE_HOTEL = sql.quote("Hotel")
AGGREGATE_TYPE_ROOM = sql.quote("Room")


def event_to_sql(event: dict[str, Any]) -> str:
    result = sql.quote(json.dumps(event))
    if result.startswith(" E'"):
        return result[1:]
    return result


def _insert(
    fp: TextIO,
    *,
    service_name: str,
    aggregate_type: str,
    event_name: str,
    data: dict[str, Any],
) -> str:
    id_ = str(uuid4())
    event = {
        "name": event_name,
        "data": data,
    }
    fp.write(
        QUERIES_INSERT_AGGREGATE_AND_EVENT.format(
            service_name=service_name,
            id=sql.quote(id_),
            type=aggregate_type,
            data=event_to_sql(event),
        )
    )
    return id_


def _write_header(fp: TextIO, header: str) -> None:
    fp.write(f"-- {header}\n")
    fp.write("-- @generated")


class App:
    def __init__(self) -> None:
        self._args = self._parse_args()
        self.__i = 50
        self._airport_ids: dict[str, str] = {}
        self._bus_stop_ids: dict[str, str] = {}
        self._country_ids: dict[str, str] = {}
        self._city_ids: dict[str, str] = {}
        self._meal_ids: dict[str, str] = {}
        self._room_ids: dict[str, str] = {}
        with open(self._args.input_file, encoding="utf-8") as fp:
            self._data = json.load(fp)
        self._output_dir = Path(self._args.output_dir)
        self._output_dir.mkdir(exist_ok=True)

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

    def _i(self) -> str:
        i = self.__i
        self.__i += 1
        return f"{i}".rjust(2, "0")

    def _reset_i(self) -> None:
        self.__i = 50

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

    def _generate_airport_queries(self) -> None:
        with open(
            self._output_dir / f"{self._i()}_airports.sql", "w", encoding="utf-8"
        ) as fp:
            _write_header(fp, "Sample airport data for the flights service")
            for airport in self._data["airports"].values():
                self._airport_ids[airport["code"]] = _insert(
                    fp,
                    service_name=SERVICE_FLIGHT,
                    aggregate_type=AGGREGATE_TYPE_AIRPORT,
                    event_name="AirportCreated",
                    data=airport,
                )

    def _generate_flight_route_queries(self) -> None:
        with open(
            self._output_dir / f"{self._i()}_flight_routes.sql", "w", encoding="utf-8"
        ) as fp:
            _write_header(fp, "Sample flight route data for the flights service")
            for flight_route in self._data["flight_routes"].values():
                _insert(
                    fp,
                    service_name=SERVICE_FLIGHT,
                    aggregate_type=AGGREGATE_TYPE_FLIGHT_ROUTE,
                    event_name="FlightRouteCreated",
                    data={
                        "origin_airport_id": self._airport_ids[
                            flight_route["origin"]["code"]
                        ],
                        "via_airport_ids": [
                            self._airport_ids[airport["code"]]
                            for airport in flight_route["via"]
                        ],
                        "destination_airport_id": self._airport_ids[
                            flight_route["destination"]["code"]
                        ],
                    },
                )

    def _generate_bus_stop_queries(self) -> None:
        with open(
            self._output_dir / f"{self._i()}_bus_stops.sql", "w", encoding="utf-8"
        ) as fp:
            _write_header(fp, "Sample bus stop data for the bus service")
            for stop in self._data["bus_stops"].values():
                self._bus_stop_ids[stop["code"]] = _insert(
                    fp,
                    service_name=SERVICE_BUS,
                    aggregate_type=AGGREGATE_TYPE_BUS_STOP,
                    event_name="BusStopCreated",
                    data=stop,
                )

    def _generate_bus_route_queries(self) -> None:
        with open(
            self._output_dir / f"{self._i()}_bus_routes.sql", "w", encoding="utf-8"
        ) as fp:
            _write_header(fp, "Sample bus route data for the bus service")
            for bus_route in self._data["bus_routes"].values():
                _insert(
                    fp,
                    service_name=SERVICE_BUS,
                    aggregate_type=AGGREGATE_TYPE_BUS_ROUTE,
                    event_name="BusRouteCreated",
                    data={
                        "origin_bus_stop_id": self._bus_stop_ids[
                            bus_route["origin"]["code"]
                        ],
                        "via_bus_stop_ids": [
                            self._bus_stop_ids[stop["code"]]
                            for stop in bus_route["via"]
                        ],
                        "destination_bus_stop_id": self._bus_stop_ids[
                            bus_route["destination"]["code"]
                        ],
                    },
                )

    def _generate_country_city_queries(self) -> None:
        with open(
            self._output_dir / f"{self._i()}_countries_and_cities.sql",
            "w",
            encoding="utf-8",
        ) as fp:
            _write_header(fp, "Sample country and city data for the tour offer service")
            for country in self._data["countries"].values():
                country_id = self._country_ids[country["identifier"]] = _insert(
                    fp,
                    service_name=SERVICE_TOUR_OFFER,
                    aggregate_type=AGGREGATE_TYPE_COUNTRY,
                    event_name="CountryCreated",
                    data={
                        "title": country["title"],
                    },
                )

                for city in country["cities"].values():
                    self._city_ids[city["identifier"]] = _insert(
                        fp,
                        service_name=SERVICE_TOUR_OFFER,
                        aggregate_type=AGGREGATE_TYPE_CITY,
                        event_name="CityCreated",
                        data={
                            "title": city["title"],
                            "country_id": country_id,
                        },
                    )

    def _generate_meal_queries(self) -> None:
        with open(
            self._output_dir / f"{self._i()}_meals.sql", "w", encoding="utf-8"
        ) as fp:
            _write_header(fp, "Sample meal data for the hotel service")
            for meal in self._data["meals"].values():
                self._meal_ids[meal["identifier"]] = _insert(
                    fp,
                    service_name=SERVICE_HOTEL,
                    aggregate_type=AGGREGATE_TYPE_MEAL,
                    event_name="MealCreated",
                    data={
                        "title": meal["title"],
                    },
                )

    def _generate_hotel_queries(self) -> None:
        with open(
            self._output_dir / f"{self._i()}_hotels.sql", "w", encoding="utf-8"
        ) as fp:
            _write_header(fp, "Sample hotel data for the hotel service")
            for hotel in self._data["hotels"].values():
                _insert(
                    fp,
                    service_name=SERVICE_HOTEL,
                    aggregate_type=AGGREGATE_TYPE_HOTEL,
                    event_name="HotelCreated",
                    data={
                        **hotel,
                        "meals": [
                            self._meal_ids[meal["identifier"]]
                            for meal in hotel["meals"]
                        ],
                        "destination_country_id": self._country_ids[
                            hotel["destination_country_id"]
                        ],
                        "destination_city_id": (
                            hotel["destination_city_id"]
                            and self._city_ids[hotel["destination_city_id"]]
                        ),
                    },
                )


def main() -> None:
    app = App()
    app.run()


if __name__ == "__main__":
    main()

"""Schedule route metadata; no collection locations are inferred or seeded."""
import json
import math
from sqlalchemy import inspect, text


def collection_points(value):
    if not isinstance(value, list):
        raise ValueError("collection_points must be a list")
    result = []
    ids = set()
    for point in value:
        if not isinstance(point, dict):
            raise ValueError("Each collection point must be an object")
        point_id, name = point.get("id"), point.get("name")
        if not isinstance(point_id, str) or not point_id.strip() or point_id in ids:
            raise ValueError("Collection point IDs must be nonempty unique strings")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Collection point names must be nonempty strings")
        latitude, longitude = point.get("latitude"), point.get("longitude")
        if (latitude is None) != (longitude is None):
            raise ValueError("Collection point coordinates must both be present or absent")
        for coordinate, bound in ((latitude, 90), (longitude, 180)):
            if coordinate is not None and (
                isinstance(coordinate, bool) or not isinstance(coordinate, (int, float))
                or not math.isfinite(coordinate) or not -bound <= coordinate <= bound
            ):
                raise ValueError("Collection point coordinates are out of range")
        ids.add(point_id)
        result.append({"id": point_id, "name": name.strip(),
                       "latitude": latitude, "longitude": longitude})
    return result


def route_name(value):
    if not isinstance(value, str):
        raise ValueError("route_name must be a string")
    if len(value) > 255:
        raise ValueError("route_name must be at most 255 characters")
    return value.strip()


def route_payload(mapping):
    points = mapping.get("collection_points")
    if isinstance(points, str):
        points = json.loads(points) if points else []
    return {"route_name": mapping.get("route_name") or "",
            "collection_points": collection_points(points if points is not None else [])}


def schedule_scope(user):
    if user.get("role", "resident") == "resident":
        return " AND s.barangay = :scope_barangay", {"scope_barangay": user.get("barangay") or ""}
    if user.get("role") == "driver":
        return " AND s.driver_id = :scope_driver", {"scope_driver": user.get("id") or ""}
    return "", {}


def validate_driver(db, driver_id):
    if driver_id is None:
        return
    if not isinstance(driver_id, str) or not driver_id.strip():
        raise ValueError("driver_id must be a nonempty string or null")
    driver = db.execute(text("SELECT id FROM users WHERE id = :id AND role = 'driver' AND is_active = TRUE"),
                        {"id": driver_id}).fetchone()
    if not driver:
        raise ValueError("Assigned driver must be an active driver")


def validate_schedule(values):
    for key in ("barangay", "waste_type", "collection_type"):
        if key in values and (not isinstance(values[key], str) or not values[key].strip()):
            raise ValueError(f"{key} must be a nonempty string")
    if "day_of_week" in values and (type(values["day_of_week"]) is not int or not 0 <= values["day_of_week"] <= 6):
        raise ValueError("day_of_week must be an integer from 0 to 6")
    if "is_active" in values and not isinstance(values["is_active"], bool):
        raise ValueError("is_active must be a boolean")
    if "waste_type" in values:
        values["collection_type"] = values["waste_type"]
    if "time_slot" in values:
        if not isinstance(values["time_slot"], str) or len(values["time_slot"].split("-")) != 2:
            raise ValueError("time_slot must contain start and end times")
        values["start_time"], values["end_time"] = [part.strip() for part in values["time_slot"].split("-")]
    from datetime import datetime
    for key in ("start_time", "end_time"):
        if key in values:
            value = values[key]
            if not isinstance(value, str):
                raise ValueError(f"{key} must be a time string")
            for pattern in ("%H:%M", "%H:%M:%S", "%I:%M %p"):
                try:
                    datetime.strptime(value, pattern)
                    break
                except ValueError:
                    pass
            else:
                raise ValueError(f"{key} must be a valid time")


def migrate_schedule_routes(engine):
    """Add only missing columns; preserve schedule IDs, assignments and progress."""
    if engine is None:
        return
    with engine.begin() as connection:
        schema = inspect(connection)
        if schema.has_table("schedules"):
            columns = {column["name"] for column in schema.get_columns("schedules")}
            for name, ddl in (("route_name", "VARCHAR(255) NOT NULL DEFAULT ''"),
                              ("collection_points", "TEXT")):
                if name not in columns:
                    connection.execute(text(f"ALTER TABLE schedules ADD COLUMN {name} {ddl}"))
        if schema.has_table("barangays"):
            import uuid
            for name in ("Apopong", "Batomelong"):
                connection.execute(text("INSERT INTO barangays (id, name) SELECT :id, :name "
                                        "WHERE NOT EXISTS (SELECT 1 FROM barangays WHERE name = :name)"),
                                   {"id": str(uuid.uuid4()), "name": name})

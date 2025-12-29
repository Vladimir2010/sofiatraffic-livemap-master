import time
import asyncio
import httpx
import csv
import math
import pickle
import os
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from google.transit import gtfs_realtime_pb2

# ================= CONFIG =================

GTFS_RT_VEHICLE_POSITIONS = "https://gtfs.sofiatraffic.bg/api/v1/vehicle-positions"
GTFS_RT_TRIP_UPDATES = "https://gtfs.sofiatraffic.bg/api/v1/trip-updates"
GTFS_STATIC_PATH = "./gtfs"
GTFS_CACHE_FILE = "gtfs_cache.pkl"

OCCUPANCY_MAP = {
    0: "EMPTY",
    1: "MANY_SEATS_AVAILABLE",
    2: "FEW_SEATS_AVAILABLE",
    3: "STANDING_ROOM_ONLY",
    4: "CRUSHED_STANDING_ROOM_ONLY",
    5: "FULL",
    6: "NOT_ACCEPTING_PASSENGERS",
}

# ================= FASTAPI =================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/health")
def health():
    return {"status": "healthy"}

# ================= GTFS STATIC (LAZY LOAD) =================

stops_map = {}
trip_last_stop = {}
stop_times_map = {}

_gtfs_loaded = False

def time_to_seconds(t: str) -> int:
    try:
        h, m, s = map(int, t.split(":"))
        return h * 3600 + m * 60 + s
    except:
        return 0

def load_gtfs_static():
    global stops_map, trip_last_stop, stop_times_map

    if os.path.exists(GTFS_CACHE_FILE):
        with open(GTFS_CACHE_FILE, "rb") as f:
            data = pickle.load(f)
            stops_map = data["stops_map"]
            trip_last_stop = data["trip_last_stop"]
            stop_times_map = data["stop_times_map"]
        print("GTFS loaded from cache")
        return

    print("Parsing GTFS static CSVs...")

    # -------- stops.txt --------
    with open(f"{GTFS_STATIC_PATH}/stops.txt", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            stops_map[r["stop_id"]] = int(
                "".join(filter(str.isdigit, r["stop_id"])) or 0
            )

    # -------- stop_times.txt (memory optimized) --------
    last_sequence = {}

    with open(f"{GTFS_STATIC_PATH}/stop_times.txt", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            tid = r["trip_id"]
            sid = r["stop_id"]
            seq = int(r["stop_sequence"])

            stop_times_map[(tid, sid)] = time_to_seconds(r["arrival_time"])

            if tid not in last_sequence or seq > last_sequence[tid][0]:
                last_sequence[tid] = (seq, sid)

    trip_last_stop.update(
        {tid: sid for tid, (_, sid) in last_sequence.items()}
    )

    with open(GTFS_CACHE_FILE, "wb") as f:
        pickle.dump({
            "stops_map": stops_map,
            "trip_last_stop": trip_last_stop,
            "stop_times_map": stop_times_map
        }, f)

    print("GTFS parsed and cached")

def ensure_gtfs_loaded():
    global _gtfs_loaded
    if not _gtfs_loaded:
        load_gtfs_static()
        _gtfs_loaded = True

# ================= DELAYS =================

async def fetch_trip_delays():
    delays = {}

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(GTFS_RT_TRIP_UPDATES)

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(r.content)

        for e in feed.entity:
            if not e.HasField("trip_update"):
                continue

            tu = e.trip_update
            tid = tu.trip.trip_id
            delay = 0

            if tu.stop_time_update:
                u = tu.stop_time_update[0]

                if u.HasField("arrival") and u.arrival.HasField("delay"):
                    delay = u.arrival.delay
                elif u.HasField("departure") and u.departure.HasField("delay"):
                    delay = u.departure.delay
                elif u.HasField("arrival") and u.arrival.HasField("time"):
                    key = (tid, u.stop_id)
                    if key in stop_times_map:
                        scheduled = stop_times_map[key]
                        est = u.arrival.time
                        tm = time.localtime(est)
                        midnight = time.mktime(
                            (tm.tm_year, tm.tm_mon, tm.tm_mday, 0, 0, 0, 0, 0, -1)
                        )
                        delay = int(est - (midnight + scheduled))
                        if abs(delay) > 43200:
                            delay = 0

            delays[tid] = delay

    except Exception as e:
        print("Delay fetch error:", e)

    return delays

# ================= VEHICLES =================

async def fetch_vehicles():
    ensure_gtfs_loaded()

    async with httpx.AsyncClient(timeout=10) as client:
        vehicle_resp, delays = await asyncio.gather(
            client.get(GTFS_RT_VEHICLE_POSITIONS),
            fetch_trip_delays()
        )

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(vehicle_resp.content)

    vehicles = []

    for e in feed.entity:
        if not e.HasField("vehicle"):
            continue

        v = e.vehicle
        if not v.trip or not v.position:
            continue

        trip_id = v.trip.trip_id
        vehicle_id = v.vehicle.id if v.vehicle else ""
        inv_number = int("".join(filter(str.isdigit, vehicle_id))) if vehicle_id else None

        delay_sec = delays.get(trip_id, 0)
        delay_min = (
            math.ceil(delay_sec / 60) - 2 if delay_sec > 0 else
            math.floor(delay_sec / 60) - 2 if delay_sec < 0 else -2
        )

        vehicles.append({
            "trip": trip_id,
            "coords": [v.position.latitude, v.position.longitude],
            "speed": int(v.position.speed or 0),
            "next_stop": stops_map.get(v.stop_id),
            "destination_stop": stops_map.get(trip_last_stop.get(trip_id)),
            "occupancy": OCCUPANCY_MAP.get(v.occupancy_status)
                if v.HasField("occupancy_status") else None,
            "cgm_id": vehicle_id,
            "inv_number": inv_number,
            "route_id": v.trip.route_id,
            "timestamp": v.timestamp or int(time.time()),
            "delay": delay_min
        })

    return vehicles

# ================= WEBSOCKET =================

@app.websocket("/v2/livemap/")
async def websocket_livemap(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            data = await fetch_vehicles()
            await ws.send_json(data)
            await asyncio.sleep(5)
    except Exception as e:
        print("WebSocket closed:", e)

import httpx
from google.transit import gtfs_realtime_pb2
import asyncio

GTFS_RT_TRIP_UPDATES = "https://gtfs.sofiatraffic.bg/api/v1/trip-updates"

async def inspect_feed():
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(GTFS_RT_TRIP_UPDATES)
            print(f"Status: {r.status_code}")
        
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(r.content)
        
        count = 0
        with_delay = 0
        
        print(f"Total entities: {len(feed.entity)}")
        
        for entity in feed.entity:
            if not entity.HasField('trip_update'):
                continue
                
            tu = entity.trip_update
            if tu.stop_time_update:
                count += 1
                first = tu.stop_time_update[0]
                
                has_arrival_delay = first.HasField('arrival') and first.arrival.HasField('delay')
                has_departure_delay = first.HasField('departure') and first.departure.HasField('delay')
                
                if has_arrival_delay or has_departure_delay:
                    with_delay += 1
                    if count <= 5: # Print first 5
                        delay = first.arrival.delay if has_arrival_delay else first.departure.delay
                        print(f"Trip {tu.trip.trip_id}: Delay {delay}s")
                else:
                    if count <= 2:
                        print(f"Trip {tu.trip.trip_id}: No explicit delay field. Fields: {first}")

        print(f"Trips with updates: {count}")
        print(f"Trips with explicit delay: {with_delay}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(inspect_feed())

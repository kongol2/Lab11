import os
from dotenv import load_dotenv
import logging
import json
import requests
import time
import threading
from datetime import datetime, timezone, timedelta
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import AckCallback, RecordType, StreamConfigurationOptions, TableProperties

load_dotenv()

client_id = os.environ["ZEROBUS_CLIENT_ID"]
client_secret = os.environ["ZEROBUS_CLIENT_SECRET"]

SERVER_ENDPOINT = "https://7405605185991044.zerobus.westeurope.azuredatabricks.net" 
WORKSPACE_URL   = "https://adb-7405605185991044.4.azuredatabricks.net"                    
TARGET_TABLE    = "dbr_dev.skybound_bronze.flights_bronze"                     
CLIENT_ID = client_id
CLIENT_SECRET = client_secret


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("flights_producer")

class FlightsAckCallback(AckCallback):
    def __init__(self):
        self.acked = 0
        self.errors = 0

    def on_ack(self, offset: int) -> None:
        self.acked += 1
        if self.acked % 500 == 0:
            logger.info(f"Acknowledged {self.acked} records (offset: {offset})")

    def on_error(self, offset: int, error_message: str) -> None:
        self.errors += 1
        logger.error(f"Ingestion error at offset {offset}: {error_message}")

# --- OpenSky API config ---
EUROPE_BBOX = {
    "lamin": 34.0,
    "lomin": -24.0,
    "lamax": 71.0,
    "lomax": 40.0
}
OPENSKY_URL = "https://opensky-network.org/api/states/all"

OPENSKY_COLUMNS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source", "category"
]

def fetch_opensky_states():
    """Fetch current flight states from OpenSky Network API."""
    try:
        response = requests.get(OPENSKY_URL, params=EUROPE_BBOX, timeout=30)
        response.raise_for_status()
        data = response.json()
        states = data.get("states", [])
        logger.info(f"Fetched {len(states)} flight states from OpenSky")
        return states
    except Exception as e:
        logger.error(f"OpenSky API error: {e}")
        return []

def state_to_record(state_vector):
    """Convert OpenSky state vector to a dict — all fields as STRING.
    Sends empty string for nulls to avoid SDK issues with None values."""
    record = {}
    for i, col in enumerate(OPENSKY_COLUMNS):
        val = state_vector[i] if i < len(state_vector) else None

        if val is None:
            record[col] = ""
        elif col == "sensors":
            record[col] = json.dumps(val)  
        elif col == "callsign":
            record[col] = str(val).strip()
        else:
            record[col] = str(val)

    record["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
    return record

def close_stream_with_timeout(stream, timeout_sec=5):
    """Close stream with a hard timeout to prevent hanging."""
    t = threading.Thread(target=stream.close)
    t.start()
    t.join(timeout=timeout_sec)
    if t.is_alive():
        logger.warning(f"stream.close() did not complete within {timeout_sec}s, moving on")

POLL_INTERVAL_SEC = 50 

ack_callback = FlightsAckCallback()

def create_zerobus_stream():
    """Create a new ZeroBus stream with AckCallback."""
    sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)
    table_properties = TableProperties(TARGET_TABLE)
    options = StreamConfigurationOptions(
        record_type=RecordType.JSON,
        ack_callback=ack_callback
    )
    stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_properties, options)
    time.sleep(1)  # Let stream stabilize before sending
    return stream

stream = create_zerobus_stream()
logger.info("ZeroBus stream created successfully")

try:
    batch = 0
    while True:
        batch += 1
        states = fetch_opensky_states()

        if not states:
            logger.warning(f"Batch {batch}: No states received, retrying in {POLL_INTERVAL_SEC}s...")
        else:
            sent = 0
            skipped = 0
            try:
                for state in states:
                    try:
                        record = state_to_record(state)
                        stream.ingest_record_offset(record)
                        sent += 1
                    except Exception as rec_err:
                        skipped += 1
                        if skipped <= 3:
                            logger.warning(f"Skipped bad record (icao24={state[0] if state else '?'}): {rec_err}")

                logger.info(
                    f"Batch {batch}: sent {sent}, skipped {skipped} "
                    f"(total acked: {ack_callback.acked}, errors: {ack_callback.errors})"
                )
            except Exception as e:
                logger.error(f"Batch {batch} stream failure: {e}. Recreating stream...")
                close_stream_with_timeout(stream, timeout_sec=5)
                stream = create_zerobus_stream()
                logger.info("Stream recreated successfully")

        time.sleep(POLL_INTERVAL_SEC)

except KeyboardInterrupt:
    logger.info("Producer stopped by user")
finally:
    close_stream_with_timeout(stream, timeout_sec=5)
    logger.info(f"Final stats: {ack_callback.acked} acknowledged, {ack_callback.errors} errors")
    logger.info("ZeroBus stream closed")
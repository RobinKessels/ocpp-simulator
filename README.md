# OCPP 2.1 Charger & CSMS Simulator

A containerized Python simulator for EV Charge Points and a Central System (CSMS) using **OCPP 2.1** over WebSocket.

## Setup and Running

Ensure you have [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/) installed.

`docker-compose up --build`

`docker-compose up --scale ocpp-client=10 -d`

## What is implemented

- CSMS WebSocket server on port `9000`
- CSMS HTTP API on port `8000`
- Charge point simulator with:
  - `BootNotification`
  - periodic `Heartbeat`
  - remote start handling (`RequestStartTransaction`)
  - remote stop handling (`RequestStopTransaction`)
  - charging session simulation via `TransactionEvent` (`Started` -> `Updated` -> `Ended`)

## API quickstart

- List connected chargers:
  - `GET /chargers`
- List charger state (status, SoC, current transaction):
  - `GET /stations`
- Get site load balancing state:
  - `GET /site-state`
- Get persisted session history (SQLite):
  - `GET /sessions`
- Get assigned current for one EVSE:
  - `GET /allocation/{cp_id}/{evse_id}`
- Start charging on an EVSE:
  - `POST /remote-start/{cp_id}/{evse_id}`
- Stop charging on an EVSE:
  - `POST /remote-stop/{cp_id}/{evse_id}`

## Frontend monitor UI

The monitor runs in a dedicated frontend container.

Open `http://localhost:8081/` in your browser to view:
- connected stations and EVSE status
- live SoC updates
- assigned current per EVSE (load balancing)
- site utilization (% allocated current vs site max)
- current transaction ID
- Start Charging / Stop buttons

Frontend source is editable in:
- `frontend/index.html`
- `frontend/styles.css`
- `frontend/app.js`

## Persistence

- CSMS stores station/session data in SQLite.
- Database file is mounted to `./data/csms.db` via Docker volume.

## Optional SQL container

- A Postgres container is included in `docker-compose.yml` for future migration work.
- Current app persistence still uses SQLite for simplicity.

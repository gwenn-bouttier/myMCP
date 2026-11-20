# myMCP

Personal MCP server, built with FastMCP, extended on the fly by Claude and me, and deployed on Prefect Horizon.

## Deploying on Horizon

1. Sign in to horizon.prefect.io with GitHub and select the `gwenn-bouttier/myMCP` repository.
2. Entry point: `main.py:mcp`
3. Dependencies are read from `requirements.txt`.
4. Every push to `main` triggers an automatic redeploy.

Server URL: `https://myfirstmcpgbo.fastmcp.app/mcp` (authentication required)

## Adding a tool

In `main.py`:

```python
@mcp.tool
def my_tool(param: str) -> str:
    """Description read by Claude."""
    return param
```

## Running locally

```bash
pip install -r requirements.txt
python main.py
```

## Ski / mountain weather tools

| Tool | Purpose |
|---|---|
| `meteo_station_ski(station, date_samedi?)` | Weekend analysis for one resort: fresh snow, snow depth, sunshine, wind gusts, temperatures, freezing level |
| `meilleures_stations_ski(depart, date_samedi?, top=3, stations?, trajet_max_h?)` | Ranks resorts (about 25 French resorts by default) and returns the top N |

Score out of 100: powder 35 · weather 25 · travel time 15 · snow depth 15 · cold 10 (detailed rules in `score_station`, `main.py`).

Free sources, no API key needed: [Open-Meteo](https://open-meteo.com) (geocoding + forecasts, 16 days max) and the public [OSRM](https://project-osrm.org) demo server for driving time (fallback: straight-line estimate).

⚠️ Off-piste: always check the avalanche bulletin (BERA, Météo-France). Open-Meteo snow depth is modeled, not measured.

"""Serveur MCP de Gwenn, déployé sur Prefect Horizon (FastMCP).

Point d'entrée Horizon : main.py:mcp
Les outils seront ajoutés ici avec le décorateur @mcp.tool.

Sources de données (gratuites, sans clé API) :
- Open-Meteo Geocoding : https://open-meteo.com/en/docs/geocoding-api
- Open-Meteo Forecast  : https://open-meteo.com/en/docs
- OSRM (serveur de démo public, usage modéré) : https://project-osrm.org
"""

import asyncio
import math
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import httpx
from fastmcp import FastMCP

mcp = FastMCP("myMCP")


@mcp.tool
def statut() -> str:
    """Vérifie que le serveur MCP est en ligne."""
    return "ok le mcp est live"


# =============================================================================
# Météo montagne / choix de station de ski
# =============================================================================

GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OSRM_TABLE_URL = "https://router.project-osrm.org/table/v1/driving/"

# Stations françaises interrogées par défaut (noms passés au géocodage Open-Meteo).
# Seuls les noms sont fixés ici : coordonnées et altitude viennent du géocodage.
STATIONS_PAR_DEFAUT = [
    # Alpes du Nord
    "Tignes", "Val-d'Isère", "Val Thorens", "Les Menuires", "Courchevel",
    "Méribel", "La Plagne", "Sainte-Foy-Tarentaise", "Chamonix-Mont-Blanc",
    "Avoriaz", "Flaine", "La Clusaz", "Valloire",
    # Alpes du Sud / Oisans
    "La Grave", "Les Deux Alpes", "Alpe d'Huez", "Serre Chevalier",
    "Montgenèvre", "Vars", "Isola 2000",
    # Pyrénées
    "Saint-Lary-Soulan", "Cauterets", "Barèges", "Font-Romeu-Odeillo-Via",
]

# Poids de l'algorithme (total = 100).
POIDS = {
    "poudreuse": 35,   # neige fraîche tombée juste avant / pendant le week-end
    "meteo": 25,       # soleil et vent les jours de ski
    "trajet": 15,      # temps de route depuis le point de départ
    "base": 15,        # hauteur de neige au sol (modélisée)
    "froid": 10,       # températures qui conservent la poudreuse
}

_cache_geo: dict[str, dict | None] = {}


def _borne(x: float, a: float = 0.0, b: float = 1.0) -> float:
    return max(a, min(b, x))


def _lineaire(valeur: float, mauvais: float, bon: float) -> float:
    """Ramène `valeur` sur 0..1 : 0 à `mauvais`, 1 à `bon` (sens quelconque)."""
    if bon == mauvais:
        return 1.0
    return _borne((valeur - mauvais) / (bon - mauvais))


def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def prochain_weekend(aujourd_hui: date) -> tuple[date, date]:
    """Samedi/dimanche à venir (ou le week-end en cours si on est samedi/dimanche)."""
    wd = aujourd_hui.weekday()  # lundi = 0
    if wd == 6:
        samedi = aujourd_hui - timedelta(days=1)
    else:
        samedi = aujourd_hui + timedelta(days=(5 - wd))
    return samedi, samedi + timedelta(days=1)


async def geocoder(
    client: httpx.AsyncClient, nom: str, pays: str | None = None, critere: str = "elevation"
) -> dict | None:
    """Géocode un lieu. Pour un nom ambigu dans le pays demandé, garde le résultat au plus
    grand `critere` : "elevation" pour une station de ski, "population" pour une ville.
    `pays` = code ISO à 2 lettres (ex. "FR")."""
    cle = f"{nom}|{pays}|{critere}"
    if cle in _cache_geo:
        return _cache_geo[cle]
    data = await _get_json(
        client, GEOCODING_URL, {"name": nom, "count": 10, "language": "fr", "format": "json"}
    )
    resultats = data.get("results") or []
    if pays:
        resultats = [x for x in resultats if x.get("country_code") == pays.upper()]
    lieu = None
    if resultats:
        lieu = max(resultats, key=lambda x: x.get(critere) or 0) if pays else resultats[0]
    _cache_geo[cle] = lieu
    return lieu


async def _get_json(client: httpx.AsyncClient, url: str, params: dict, essais: int = 6):
    """GET avec nouvelles tentatives sur 429/5xx et coupures réseau
    (le quota gratuit Open-Meteo est compté par IP, souvent partagée en hébergement)."""
    for i in range(essais):
        dernier = i == essais - 1
        try:
            r = await client.get(url, params=params)
        except httpx.TransportError:
            if dernier:
                raise
            await asyncio.sleep(1 + i)
            continue
        if (r.status_code == 429 or r.status_code >= 500) and not dernier:
            await asyncio.sleep(1 + i)
            continue
        r.raise_for_status()
        return r.json()


async def previsions_stations(
    client: httpx.AsyncClient, lieux: list[dict], samedi: date
) -> list[dict]:
    """Prévisions des stations via des requêtes Open-Meteo multi-coordonnées (paquets de 8),
    de J-3 (neige tombée avant) au dimanche. Renvoie une prévision par lieu, dans l'ordre."""
    if len(lieux) > 8:
        res = []
        for k in range(0, len(lieux), 8):
            res += await previsions_stations(client, lieux[k:k + 8], samedi)
        return res
    data = await _get_json(
        client,
        FORECAST_URL,
        {
            "latitude": ",".join(str(l["latitude"]) for l in lieux),
            "longitude": ",".join(str(l["longitude"]) for l in lieux),
            "daily": ",".join([
                "snowfall_sum", "precipitation_sum", "temperature_2m_max",
                "temperature_2m_min", "wind_gusts_10m_max", "sunshine_duration",
                "weather_code",
            ]),
            "hourly": "snow_depth,freezing_level_height",
            "start_date": (samedi - timedelta(days=3)).isoformat(),
            "end_date": (samedi + timedelta(days=1)).isoformat(),
            "timezone": "Europe/Paris",
        },
    )
    return data if isinstance(data, list) else [data]


def verifier_fenetre(samedi: date) -> str | None:
    """Open-Meteo prévoit 16 jours max : refuse un week-end trop lointain."""
    aujourd_hui = datetime.now(ZoneInfo("Europe/Paris")).date()
    if samedi + timedelta(days=1) > aujourd_hui + timedelta(days=15):
        return f"Week-end du {samedi} hors de la fenêtre de prévision (16 jours)."
    if samedi + timedelta(days=1) < aujourd_hui - timedelta(days=60):
        return f"Week-end du {samedi} trop ancien."
    return None


def _jour(daily: dict, d: date) -> dict | None:
    try:
        i = daily["time"].index(d.isoformat())
    except (KeyError, ValueError):
        return None
    return {k: (v[i] if isinstance(v, list) else None) for k, v in daily.items()}


def _horaire(hourly: dict, d: date, heure: int, champ: str):
    cible = f"{d.isoformat()}T{heure:02d}:00"
    try:
        return hourly[champ][hourly["time"].index(cible)]
    except (KeyError, ValueError, IndexError):
        return None


def analyser_weekend(prev: dict, samedi: date) -> dict | None:
    """Extrait les indicateurs utiles au ski hors-piste pour le week-end donné."""
    daily, hourly = prev.get("daily", {}), prev.get("hourly", {})
    jours_ski = [samedi, samedi + timedelta(days=1)]
    jours_avant = [samedi - timedelta(days=k) for k in (3, 2, 1)]
    ski = [_jour(daily, d) for d in jours_ski]
    if any(j is None for j in ski):
        return None  # week-end hors de la fenêtre de prévision (16 jours)

    def v(j, k, defaut=0.0):
        x = j.get(k) if j else None
        return defaut if x is None else x

    neige_avant = sum(v(_jour(daily, d), "snowfall_sum") for d in jours_avant)
    neige_weekend = sum(v(j, "snowfall_sum") for j in ski)
    hauteur_m = _horaire(hourly, samedi, 9, "snow_depth")
    iso0 = [_horaire(hourly, d, 12, "freezing_level_height") for d in jours_ski]
    return {
        "neige_fraiche_3j_avant_cm": round(neige_avant, 1),
        "neige_pendant_weekend_cm": round(neige_weekend, 1),
        "hauteur_neige_samedi_cm": None if hauteur_m is None else round(hauteur_m * 100),
        "jours": [
            {
                "date": d.isoformat(),
                "soleil_h": round(v(j, "sunshine_duration") / 3600, 1),
                "rafales_max_kmh": v(j, "wind_gusts_10m_max"),
                "temp_min_c": v(j, "temperature_2m_min"),
                "temp_max_c": v(j, "temperature_2m_max"),
                "neige_cm": v(j, "snowfall_sum"),
                "code_meteo_wmo": j.get("weather_code"),
                "iso0_midi_m": z,
            }
            for d, j, z in zip(jours_ski, ski, iso0)
        ],
    }


def score_station(analyse: dict, trajet_h: float | None) -> dict:
    """Algorithme de notation (0-100). Chaque critère est ramené sur 0..1 puis pondéré.

    - poudreuse : neige des 3 jours avant + 50 % de celle du week-end ; 40 cm => 1
    - base      : hauteur de neige samedi matin ; 0 cm => 0, 150 cm => 1
    - meteo     : par jour, 60 % soleil (8 h => 1) + 40 % vent (rafales 40 km/h => 1,
                  80 km/h => 0, risque de remontées fermées) ; moyenne samedi/dimanche
    - froid     : température max moyenne ; 6 °C => 0, -2 °C => 1
    - trajet    : 3 h ou moins => 1, 10 h ou plus => 0 (inconnu => 0,5)
    """
    jours = analyse["jours"]
    poudreuse_cm = analyse["neige_fraiche_3j_avant_cm"] + 0.5 * analyse["neige_pendant_weekend_cm"]
    hauteur = analyse["hauteur_neige_samedi_cm"]
    meteo = sum(
        0.6 * _lineaire(j["soleil_h"], 0, 8) + 0.4 * _lineaire(j["rafales_max_kmh"], 80, 40)
        for j in jours
    ) / len(jours)
    tmax = sum(j["temp_max_c"] for j in jours) / len(jours)

    criteres = {
        "poudreuse": _lineaire(poudreuse_cm, 0, 40),
        "base": 0.0 if hauteur is None else _lineaire(hauteur, 0, 150),
        "meteo": meteo,
        "froid": _lineaire(tmax, 6, -2),
        "trajet": 0.5 if trajet_h is None else _lineaire(trajet_h, 10, 3),
    }
    total = sum(POIDS[k] * c for k, c in criteres.items())
    return {
        "score_sur_100": round(total, 1),
        "detail_sur_100": {k: round(POIDS[k] * c, 1) for k, c in criteres.items()},
    }


async def temps_de_trajet(
    client: httpx.AsyncClient, depart: dict, stations: list[dict]
) -> list[dict]:
    """Durée/distance en voiture via OSRM (une seule requête 'table').
    Repli : distance à vol d'oiseau x 1,3 à 80 km/h de moyenne (marqué 'estimation')."""
    coords = ";".join(f"{p['longitude']},{p['latitude']}" for p in [depart, *stations])
    try:
        r = await client.get(
            OSRM_TABLE_URL + coords,
            params={"sources": 0, "annotations": "duration,distance"},
        )
        r.raise_for_status()
        data = r.json()
        durees, distances = data["durations"][0][1:], data["distances"][0][1:]
        return [
            {
                "duree_h": None if d is None else round(d / 3600, 1),
                "distance_km": None if km is None else round(km / 1000),
                "methode": "OSRM (route)",
            }
            for d, km in zip(durees, distances)
        ]
    except Exception:
        res = []
        for s in stations:
            km = _haversine_km(depart["latitude"], depart["longitude"], s["latitude"], s["longitude"]) * 1.3
            res.append({"duree_h": round(km / 80, 1), "distance_km": round(km), "methode": "estimation"})
        return res


def _samedi_cible(date_samedi: str | None) -> date:
    if date_samedi:
        return prochain_weekend(date.fromisoformat(date_samedi))[0]
    return prochain_weekend(datetime.now(ZoneInfo("Europe/Paris")).date())[0]


@mcp.tool
async def meteo_station_ski(station: str, date_samedi: str | None = None) -> dict:
    """Analyse météo montagne d'une station de ski pour un week-end : neige fraîche,
    hauteur de neige, soleil, vent, températures, isotherme 0 °C.

    Args:
        station: nom de la station (ex. "Tignes", "La Grave").
        date_samedi: samedi visé au format AAAA-MM-JJ (défaut : prochain week-end,
            16 jours max).
    """
    samedi = _samedi_cible(date_samedi)
    if err := verifier_fenetre(samedi):
        return {"erreur": err}
    async with httpx.AsyncClient(timeout=15) as client:
        lieu = await geocoder(client, station, pays=None)
        if not lieu:
            return {"erreur": f"Station introuvable : {station}"}
        prev = (await previsions_stations(client, [lieu], samedi))[0]
    analyse = analyser_weekend(prev, samedi)
    if not analyse:
        return {"erreur": f"Week-end du {samedi} hors de la fenêtre de prévision (16 jours)."}
    return {
        "station": lieu.get("name"),
        "region": lieu.get("admin1"),
        "altitude_point_prevision_m": prev.get("elevation"),
        "weekend": samedi.isoformat(),
        **analyse,
        **score_station(analyse, None),
        "source": "Open-Meteo (open-meteo.com) - neige au sol modélisée, pas mesurée",
    }


@mcp.tool
async def meilleures_stations_ski(
    depart: str,
    date_samedi: str | None = None,
    top: int = 3,
    stations: list[str] | None = None,
    trajet_max_h: float | None = None,
    pays_depart: str = "FR",
) -> dict:
    """Classe les stations de ski pour un week-end hors-piste / poudreuse en combinant
    neige fraîche, hauteur de neige, météo (soleil, vent), froid et temps de trajet.
    Renvoie le top N avec le détail du score.

    Args:
        depart: ville de départ (ex. "Rennes").
        date_samedi: samedi visé au format AAAA-MM-JJ (défaut : prochain week-end).
        top: nombre de stations à renvoyer (défaut 3).
        stations: liste de stations à comparer (défaut : ~25 stations françaises).
        trajet_max_h: exclut les stations au-delà de cette durée de route.
        pays_depart: code pays ISO de la ville de départ (défaut "FR").
    """
    samedi = _samedi_cible(date_samedi)
    if err := verifier_fenetre(samedi):
        return {"erreur": err}
    noms = stations or STATIONS_PAR_DEFAUT
    sem = asyncio.Semaphore(5)

    async with httpx.AsyncClient(timeout=20) as client:
        origine = await geocoder(client, depart, pays=pays_depart, critere="population")
        if not origine:
            origine = await geocoder(client, depart, pays=None)
        if not origine:
            return {"erreur": f"Ville de départ introuvable : {depart}"}

        async def geo(nom):
            async with sem:
                try:
                    return await geocoder(client, nom, pays="FR" if stations is None else None)
                except httpx.HTTPError:
                    return None

        lieux = await asyncio.gather(*(geo(n) for n in noms))
        introuvables = [n for n, l in zip(noms, lieux) if not l]
        trouves = [(n, l) for n, l in zip(noms, lieux) if l]
        if not trouves:
            return {"erreur": "Aucune station géocodée.", "introuvables": introuvables}

        trajets = await temps_de_trajet(client, origine, [l for _, l in trouves])

        try:
            previsions = await previsions_stations(client, [l for _, l in trouves], samedi)
        except httpx.HTTPError as e:
            return {"erreur": f"Prévisions Open-Meteo indisponibles : {e}"}

    classement, erreurs = [], []
    for (nom, lieu), trajet, p in zip(trouves, trajets, previsions):
        if trajet_max_h is not None and trajet["duree_h"] is not None and trajet["duree_h"] > trajet_max_h:
            continue
        analyse = analyser_weekend(p, samedi) if p else None
        if not analyse:
            erreurs.append(nom)
            continue
        classement.append({
            "station": lieu.get("name"),
            "region": lieu.get("admin1"),
            "altitude_point_prevision_m": p.get("elevation"),
            "trajet": trajet,
            **score_station(analyse, trajet["duree_h"]),
            **analyse,
        })

    classement.sort(key=lambda x: x["score_sur_100"], reverse=True)
    return {
        "depart": f"{origine.get('name')} ({origine.get('admin1')}, {origine.get('country_code')})",
        "weekend": f"{samedi.isoformat()} / {(samedi + timedelta(days=1)).isoformat()}",
        "top": classement[: max(1, top)],
        "nb_stations_comparees": len(classement),
        "stations_introuvables": introuvables,
        "stations_sans_prevision": erreurs,
        "algorithme": {"poids_sur_100": POIDS, "regles": score_station.__doc__},
        "avertissement": (
            "Hors-piste : vérifier impérativement le bulletin de risque d'avalanche "
            "(BERA, Météo-France) avant de partir. Neige au sol = valeur modélisée."
        ),
        "sources": [
            "Open-Meteo Geocoding + Forecast (open-meteo.com)",
            "OSRM router.project-osrm.org (temps de route) ou estimation à vol d'oiseau",
        ],
    }


if __name__ == "__main__":
    # Utilisé uniquement en local ; Horizon ignore ce bloc.
    mcp.run()

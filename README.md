# myMCP

Serveur MCP personnel, écrit avec FastMCP, enrichi à la volée par moi et claude et déployé sur Prefect Horizon.

## Déploiement sur Horizon

1. Se connecter sur horizon.prefect.io avec GitHub et choisir le dépôt `gwenn-bouttier/myMCP`.
2. Point d'entrée : `main.py:mcp`
3. Les dépendances sont lues dans `requirements.txt`.
4. Chaque push sur `main` redéploie automatiquement.

URL du serveur : `https://myfirstmcpgbo.fastmcp.app/mcp` (authentification requise)

## Ajouter un outil

Dans `main.py` :

```python
@mcp.tool
def mon_outil(param: str) -> str:
    """Description lue par Claude."""
    return param
```

## Tester en local

```bash
pip install -r requirements.txt
python main.py
```

## Outils ski / météo montagne

| Outil | Rôle |
|---|---|
| `meteo_station_ski(station, date_samedi?)` | Analyse d'une station pour un week-end : neige fraîche, hauteur de neige, soleil, rafales, températures, isotherme 0 °C |
| `meilleures_stations_ski(depart, date_samedi?, top=3, stations?, trajet_max_h?)` | Classe les stations (≈25 stations françaises par défaut) et renvoie le top N |

Score sur 100 : poudreuse 35 · météo 25 · trajet 15 · hauteur de neige 15 · froid 10 (règles détaillées dans `score_station`, `main.py`).

Sources gratuites, sans clé : [Open-Meteo](https://open-meteo.com) (géocodage + prévisions, 16 jours max) et le serveur de démo public [OSRM](https://project-osrm.org) pour le temps de route (repli : estimation à vol d'oiseau).

⚠️ Hors-piste : toujours consulter le bulletin d'avalanche (BERA, Météo-France). La hauteur de neige d'Open-Meteo est modélisée, pas mesurée.

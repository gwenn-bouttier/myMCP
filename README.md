# myMCP

Serveur MCP personnel, écrit avec FastMCP et déployé sur Prefect Horizon.

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

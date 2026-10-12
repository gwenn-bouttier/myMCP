"""Serveur MCP de Gwenn, déployé sur Prefect Horizon (FastMCP).

Point d'entrée Horizon : main.py:mcp
Les outils seront ajoutés ici avec le décorateur @mcp.tool.
"""

from fastmcp import FastMCP

mcp = FastMCP("myMCP")


@mcp.tool
def statut() -> str:
    """Vérifie que le serveur MCP est en ligne."""
    return "ok le mcp est live"


# Exemple pour ajouter un outil plus tard :
#
# @mcp.tool
# def bonjour(nom: str) -> str:
#     """Renvoie un message de bienvenue."""
#     return f"Bonjour {nom} !"


if __name__ == "__main__":
    # Utilisé uniquement en local ; Horizon ignore ce bloc.
    mcp.run()

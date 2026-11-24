# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

"""Verificação de frescor (check) para agregados do SIDRA, sem download.

O frescor é por existência (ADR 2026-11-19, D6): um pedaço está em dia se o
arquivo ``t-<id>/<consulta>@AAAAMMDD.json`` com a data de modificação da API
já existe. Revisão do IBGE muda a data, logo o nome, e o pedaço volta a ser
"a baixar". Não se compara mtime.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .download import modificacao_do_chunk
from .storage import DataRepository, nome_arquivo

__all__ = ["check_agregado"]


def check_agregado(
    client: Any,
    agregado_id: int,
    output_dir: str | Path,
    filtros: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Verificar frescor de um agregado sem baixar dados.

    Só toca a rede para metadados (agregado + plano); nunca baixa valores.

    Args:
        client: Cliente com ``get_agregado`` e ``plan_dados_agregado``.
        agregado_id: ID do agregado.
        output_dir: Raiz do bruto (os arquivos vivem em ``t-<id>/``).
        filtros: Mesmos filtros de ``plan_dados_agregado``
            (``niveis_territoriais``, ``variaveis``, ``periodos``,
            ``classificacoes``).

    Returns:
        Tupla ``(veredictos, resumo)``: lista de veredictos no schema de
        ``CheckPlanItem`` do SDK (com ``entry`` reconstruível) e resumo com
        ``agregado_id``, ``agregado_nome`` e ``n_niveis``.
    """
    filtros = dict(filtros or {})
    agregado = client.get_agregado(agregado_id)
    chunks = client.plan_dados_agregado(agregado_id, agregado=agregado, **filtros)
    pasta = Path(output_dir) / DataRepository.dataset_id(agregado_id)
    modificacoes = {p.id: p.modificacao for p in agregado.periodos}

    por_nivel: dict[str, list[tuple[Path, Any]]] = {}
    for chunk in chunks:
        modificacao = modificacao_do_chunk(chunk, modificacoes)
        nome, _hashes = nome_arquivo(chunk.parametro, modificacao)
        por_nivel.setdefault(chunk.nivel_territorial, []).append(
            (pasta / nome, modificacao)
        )

    veredictos: list[dict[str, Any]] = []
    for nivel in sorted(por_nivel):
        alvos = por_nivel[nivel]
        presentes = sum(1 for caminho, _m in alvos if caminho.exists())
        datas = [m for _c, m in alvos if m is not None]
        max_mod = max(datas) if datas else None
        if presentes == len(alvos):
            acao, motivo = "skip-up-to-date", "local-fresh"
        else:
            acao = "download"
            motivo = "not-present" if presentes == 0 else "remote-newer"
        dataset = DataRepository.dataset_id(agregado_id)
        veredictos.append(
            {
                "dataset": dataset,
                "id": f"{dataset}:{nivel}",
                "url": agregado.url,
                "partition": nivel,
                "local_path": str(pasta),
                "remote_etag": None,
                "remote_last_modified": max_mod.isoformat() if max_mod else None,
                "remote_size": None,
                "local_exists": presentes > 0,
                "action": acao,
                "reason": motivo,
                "entry": {
                    "id": f"{dataset}:{nivel}",
                    "url": agregado.url,
                    "dataset": dataset,
                    "agregado_id": agregado_id,
                    "nivel": nivel,
                    "n_arquivos": len(alvos),
                    "n_presentes": presentes,
                    "filtros": filtros,
                },
            }
        )
    resumo = {
        "agregado_id": agregado_id,
        "agregado_nome": agregado.nome,
        "n_niveis": len(por_nivel),
    }
    return veredictos, resumo

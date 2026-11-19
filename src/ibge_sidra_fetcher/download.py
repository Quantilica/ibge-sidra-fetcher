# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

"""Download completo de dados de um agregado SIDRA, respeitando o limite da API.

Este módulo planeja (chunking) e executa o download de TODOS os dados de uma
tabela SIDRA (endpoint `/values`) a partir de um único ``agregado_id``,
dividindo a seleção completa em requisições que nunca excedem
``SIDRA_API_VALUES_LIMIT`` (50.000 valores). Ao contrário do ``sidra-sql``
(que assume que cada período isolado já cabe no limite — garantido
manualmente por quem escreve o pipeline), este módulo descobre e divide
automaticamente, priorizando nesta ordem: período -> lote de localidades ->
variável -> combinação de categorias de classificação.

Cada nível territorial do agregado (administrativo, especial, ibge) é
planejado e baixado separadamente — nunca combinado num único ``Parametro``.

Typical usage:

    >>> from ibge_sidra_fetcher.fetcher import SidraClient
    >>> with SidraClient() as client:
    ...     chunks = client.plan_dados_agregado(1705)
    ...     resumo = describe_download_plan(chunks)
    ...     res = client.download_dados_agregado(1705, "./ibge-sidra")
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import time
from collections.abc import Callable, Generator, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from quantilica.core.exceptions import FetchError
from quantilica.core.http import RateLimiter

from .agregados import Agregado
from .reader import DateEncoder
from .sidra import SIDRA_API_VALUES_LIMIT, Formato, Parametro, Precisao
from .storage import DataRepository, nome_arquivo

if TYPE_CHECKING:
    from .fetcher import SidraClient


@contextlib.contextmanager
def graceful_executor(max_workers: int) -> Generator[ThreadPoolExecutor, None, None]:
    """ThreadPoolExecutor que cancela os futures pendentes no Ctrl-C."""
    executor = ThreadPoolExecutor(max_workers=max_workers)
    try:
        yield executor
        executor.shutdown(wait=True)
    except KeyboardInterrupt:
        executor.shutdown(wait=False, cancel_futures=True)
        raise


def _validar_linhas(url: str, linhas: object) -> list:
    """Garante que a resposta do SIDRA é uma lista de registros.

    A API às vezes responde HTTP 200 com um corpo de erro em formato de dict
    (``{"status": ..., "message": ...}``); sem esta checagem, o fatiamento
    ``linhas[1:]`` levantaria um ``TypeError`` obscuro no meio do download.
    """
    if not isinstance(linhas, list):
        raise FetchError(f"Resposta inesperada do SIDRA em {url}: {linhas!r:.200}")
    return linhas


@dataclass(frozen=True)
class DownloadChunk:
    """Um request planejado para o endpoint `/values` do SIDRA.

    Attributes:
        nivel_territorial: Nível territorial na forma da API de metadados
            (ex.: ``"N6"``).
        parametro: Parâmetros completos do request.
        n_valores: Estimativa exata de valores retornados por este chunk.
    """

    nivel_territorial: str
    parametro: Parametro
    n_valores: int


@dataclass(frozen=True)
class _Pools:
    """Pools completos (ou restritos pelo usuário) de ids para um nível territorial."""

    periodos: list[str]
    localidades: list[str]
    variaveis: list[str]
    classificacoes: dict[str, list[str]]


@dataclass(frozen=True)
class _PlanContext:
    """Seleção corrente durante o chunking.

    ``None`` num campo (ou por chave, em ``classificacoes``) significa "todo
    o pool", renderizado como o seletor ``all``/``[]`` do SIDRA — evita
    materializar listas gigantes (ex.: 5.570 municípios) quando o usuário só
    quer "todos".
    """

    periodos: list[str] | None = None
    localidades: list[str] | None = None
    variaveis: list[str] | None = None
    classificacoes: dict[str, list[str] | None] = field(default_factory=dict)


def _context_n_valores(ctx: _PlanContext, pools: _Pools) -> int:
    """Calcula o total de valores (produto das dimensões) de um contexto."""
    n_periodos = len(ctx.periodos) if ctx.periodos is not None else len(pools.periodos)
    n_localidades = (
        len(ctx.localidades) if ctx.localidades is not None else len(pools.localidades)
    )
    n_variaveis = (
        len(ctx.variaveis) if ctx.variaveis is not None else len(pools.variaveis)
    )
    n_dimensoes = 1
    for cid, pool_categorias in pools.classificacoes.items():
        selecionadas = ctx.classificacoes.get(cid)
        n_dimensoes *= (
            len(selecionadas) if selecionadas is not None else len(pool_categorias)
        )
    return n_periodos * n_localidades * n_variaveis * n_dimensoes


def _chunked(items: list[str], size: int) -> Generator[list[str], None, None]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _split_classificacoes(
    ctx: _PlanContext, pools: _Pools, limit: int
) -> Generator[_PlanContext, None, None]:
    """Fallback final: expande, uma categoria por vez, a classificação com
    mais categorias selecionadas atualmente — generalização de
    ``sidra_sql.sidra.unnest_classificacoes`` que para assim que o contexto
    cabe no limite, em vez de sempre expandir tudo.
    """
    if _context_n_valores(ctx, pools) <= limit:
        yield ctx
        return

    candidatos = [
        (
            cid,
            ctx.classificacoes.get(cid)
            if ctx.classificacoes.get(cid) is not None
            else pool_categorias,
        )
        for cid, pool_categorias in pools.classificacoes.items()
    ]
    if not candidatos:
        # Não há mais nenhuma dimensão para dividir; o chunk é atômico.
        yield ctx
        return

    cid, categorias = max(candidatos, key=lambda kv: len(kv[1]))
    if len(categorias) <= 1:
        yield ctx
        return

    for categoria in categorias:
        # Ao contrário de `sidra_sql.sidra.unnest_classificacoes` (que pula a
        # categoria "0"/"Total" porque pipelines curados só querem as
        # categorias granulares), aqui a categoria "0" é só mais um valor
        # retornado pela API e precisa ser baixada — omiti-la deixaria dados
        # faltando, contrariando o objetivo de baixar TUDO.
        nova_classificacoes = {**ctx.classificacoes, cid: [categoria]}
        yield from _split_classificacoes(
            replace(ctx, classificacoes=nova_classificacoes), pools, limit
        )


def _split_context(
    ctx: _PlanContext, pools: _Pools, limit: int
) -> Generator[_PlanContext, None, None]:
    """Divide recursivamente um contexto até que cada resultado caiba no limite.

    Ordem de prioridade: período -> lote de localidades -> variável ->
    combinação de classificações (via :func:`_split_classificacoes`).
    """
    if _context_n_valores(ctx, pools) <= limit:
        yield ctx
        return

    # 1) período
    periodos = ctx.periodos if ctx.periodos is not None else pools.periodos
    if len(periodos) > 1:
        for periodo in periodos:
            yield from _split_context(replace(ctx, periodos=[periodo]), pools, limit)
        return

    # 2) lote de localidades — calcula o maior lote que cabe no limite.
    localidades = ctx.localidades if ctx.localidades is not None else pools.localidades
    if len(localidades) > 1:
        tamanho_um = _context_n_valores(
            replace(ctx, localidades=[localidades[0]]), pools
        )
        tamanho_lote = max(1, limit // tamanho_um) if tamanho_um else len(localidades)
        for lote in _chunked(localidades, tamanho_lote):
            yield from _split_context(replace(ctx, localidades=lote), pools, limit)
        return

    # 3) variável
    variaveis = ctx.variaveis if ctx.variaveis is not None else pools.variaveis
    if len(variaveis) > 1:
        for variavel in variaveis:
            yield from _split_context(replace(ctx, variaveis=[variavel]), pools, limit)
        return

    # 4) combinação de classificações (fallback final)
    yield from _split_classificacoes(ctx, pools, limit)


def _context_to_parametro(
    agregado_id: int | str,
    nivel: str,
    ctx: _PlanContext,
    formato: Formato,
    decimais: dict[str, Precisao],
) -> Parametro:
    """Converte um ``_PlanContext`` resolvido em um ``Parametro`` de requisição.

    Nota: os níveis territoriais dos metadados usam a forma prefixada
    (``"N6"``), mas ``Parametro.territorios`` espera a chave numérica pura
    (``"6"``) — daí o ``removeprefix("N")``.
    """
    nivel_numero = nivel.removeprefix("N")
    return Parametro(
        agregado=str(agregado_id),
        territorios={nivel_numero: list(ctx.localidades) if ctx.localidades else []},
        variaveis=list(ctx.variaveis) if ctx.variaveis else [],
        periodos=list(ctx.periodos) if ctx.periodos else [],
        classificacoes={
            cid: (list(cats) if cats else [])
            for cid, cats in ctx.classificacoes.items()
        },
        formato=formato,
        decimais=decimais,
    )


def _niveis_territoriais_disponiveis(agregado: Agregado) -> list[str]:
    """Todos os níveis territoriais do agregado, na ordem administrativo,
    especial, ibge, sem duplicatas.
    """
    niveis: list[str] = []
    for nivel in (
        agregado.nivel_territorial.administrativo
        + agregado.nivel_territorial.especial
        + agregado.nivel_territorial.ibge
    ):
        if nivel not in niveis:
            niveis.append(nivel)
    return niveis


def _restringir_pool(
    nome: str, pool: list[str], escolhidos: list[str] | None
) -> list[str]:
    """Restringe ``pool`` à seleção do usuário, validando os ids.

    Retorna o pool completo quando ``escolhidos is None`` (sem restrição).
    Levanta ``ValueError`` se algum id escolhido não existe no agregado ou
    se a seleção resulta vazia — para uma tabela arbitrária, um filtro que
    não casa com nada deve falhar alto, não baixar tudo silenciosamente.
    """
    if escolhidos is None:
        return pool
    validos = set(pool)
    desconhecidos = [x for x in escolhidos if x not in validos]
    if desconhecidos:
        raise ValueError(f"{nome} inexistente(s) no agregado: {desconhecidos}")
    escolhidos_set = set(escolhidos)
    restrito = [x for x in pool if x in escolhidos_set]
    if not restrito:
        raise ValueError(f"Seleção de {nome} vazia após o filtro")
    return restrito


def plan_agregado_download(
    agregado: Agregado,
    *,
    niveis_territoriais: list[str] | None = None,
    variaveis: list[str] | None = None,
    periodos: list[str] | None = None,
    classificacoes: dict[str, list[str]] | None = None,
    formato: Formato = Formato.A,
    decimais: dict[str, Precisao] | None = None,
    limit: int = SIDRA_API_VALUES_LIMIT,
) -> list[DownloadChunk]:
    """Planeja os requests necessários para baixar todos os dados de um agregado.

    Por padrão, baixa todas as variáveis, períodos, classificações/categorias
    e níveis territoriais disponíveis no agregado. Cada nível territorial é
    planejado separadamente — nunca combinado num único ``Parametro``.

    Args:
        agregado: Metadados completos do agregado (com ``periodos`` e
            ``localidades`` já preenchidos, ex.: via ``SidraClient.get_agregado``).
        niveis_territoriais: Níveis a baixar (forma ``"N6"``). Padrão: todos
            os níveis disponíveis no agregado.
        variaveis: IDs de variáveis a restringir. Padrão: todas.
        periodos: IDs de períodos a restringir. Padrão: todos.
        classificacoes: Mapa de classificação -> categorias a restringir.
            Classificações omitidas usam todas as categorias. Padrão: todas.
        formato: Formato de saída do SIDRA (padrão ``Formato.A``).
        decimais: Precisão decimal (padrão precisão máxima).
        limit: Limite de valores por requisição (padrão ``SIDRA_API_VALUES_LIMIT``).

    Returns:
        Lista de :class:`DownloadChunk`, cobrindo exatamente a seleção pedida
        sem sobreposição e sem excesso, cada um respeitando ``limit``.
    """
    if decimais is None:
        decimais = {"": Precisao.M}

    disponiveis = _niveis_territoriais_disponiveis(agregado)
    if niveis_territoriais is not None:
        desconhecidos = [n for n in niveis_territoriais if n not in disponiveis]
        if desconhecidos:
            raise ValueError(
                f"Nível(is) territorial(is) inexistente(s) no agregado "
                f"{agregado.id}: {desconhecidos}"
            )
        niveis = niveis_territoriais
    else:
        niveis = disponiveis

    periodos_pool = _restringir_pool(
        "período(s)", [p.id for p in agregado.periodos], periodos
    )
    variaveis_pool = _restringir_pool(
        "variável(is)", [str(v.id) for v in agregado.variaveis], variaveis
    )

    classificacoes_pool: dict[str, list[str]] = {
        str(c.id): [str(cat.id) for cat in c.categorias]
        for c in agregado.classificacoes
    }
    if classificacoes is not None:
        desconhecidas = [c for c in classificacoes if c not in classificacoes_pool]
        if desconhecidas:
            raise ValueError(
                f"Classificação(ões) inexistente(s) no agregado "
                f"{agregado.id}: {desconhecidas}"
            )
        classificacoes_pool = {
            cid: _restringir_pool(
                f"categoria(s) da classificação {cid}",
                cats,
                classificacoes.get(cid),
            )
            for cid, cats in classificacoes_pool.items()
        }

    chunks: list[DownloadChunk] = []
    for nivel in niveis:
        localidades_pool = [
            loc.id for loc in agregado.localidades if loc.nivel.id == nivel
        ]
        if not localidades_pool:
            # Nível declarado nos metadados do agregado, mas sem nenhuma
            # localidade associada (acontece com alguns níveis "especiais")
            # — não há nada para baixar, então não geramos um request vazio.
            continue
        pools = _Pools(
            periodos=periodos_pool,
            localidades=localidades_pool,
            variaveis=variaveis_pool,
            classificacoes=classificacoes_pool,
        )
        # `None` num campo do contexto inicial só é seguro quando o usuário
        # NÃO restringiu aquela dimensão (pool == universo completo do
        # agregado) — nesse caso, "None" -> "all" é exatamente o que se
        # quer. Quando há restrição explícita, ela precisa ficar
        # materializada desde o início: se a seleção restrita já couber
        # num único chunk, o splitter nunca vai tocar esse campo, e
        # `_context_to_parametro` renderizaria "all" (todos os valores do
        # agregado) em vez da restrição pedida — perdendo o filtro.
        ctx_inicial = _PlanContext(
            periodos=list(periodos_pool) if periodos is not None else None,
            variaveis=list(variaveis_pool) if variaveis is not None else None,
            classificacoes={
                cid: (
                    list(cats)
                    if classificacoes is not None and cid in classificacoes
                    else None
                )
                for cid, cats in classificacoes_pool.items()
            },
        )
        # Um pedaço cobre sempre um período só (ADR 2026-11-19, D3): o nome do
        # arquivo carrega a data de modificação desse período, e um período
        # novo ou revisado gera só os arquivos dele. Por isso a divisão por
        # período é incondicional, mesmo quando a tabela inteira cabe no limite.
        if len(periodos_pool) > 1:
            ctxs = (
                ctx
                for periodo in periodos_pool
                for ctx in _split_context(
                    replace(ctx_inicial, periodos=[periodo]), pools, limit
                )
            )
        else:
            ctxs = _split_context(ctx_inicial, pools, limit)
        for ctx in ctxs:
            parametro = _context_to_parametro(
                agregado.id, nivel, ctx, formato, decimais
            )
            chunks.append(
                DownloadChunk(
                    nivel_territorial=nivel,
                    parametro=parametro,
                    n_valores=_context_n_valores(ctx, pools),
                )
            )
    return chunks


def describe_download_plan(chunks: list[DownloadChunk]) -> dict[str, Any]:
    """Resumo de um plano de download: total de requests e valores estimados.

    Args:
        chunks (list[DownloadChunk]): Lista de chunks gerados pelo planejador.

    Returns:
        Dict com ``n_requests``, ``n_valores`` e ``por_nivel`` (mapa de nível
        territorial para ``{"n_requests": int, "n_valores": int}``).
    """
    por_nivel: dict[str, dict[str, int]] = {}
    for chunk in chunks:
        resumo = por_nivel.setdefault(
            chunk.nivel_territorial, {"n_requests": 0, "n_valores": 0}
        )
        resumo["n_requests"] += 1
        resumo["n_valores"] += chunk.n_valores
    return {
        "n_requests": len(chunks),
        "n_valores": sum(c.n_valores for c in chunks),
        "por_nivel": por_nivel,
    }


def iter_download_chunks(
    client: SidraClient,
    chunks: list[DownloadChunk],
    *,
    politeness_delay: float = 0.0,
) -> Generator[tuple[DownloadChunk, list[dict]], None, None]:
    """Baixa os chunks sequencialmente, um de cada vez (sem threads).

    Seguro em memória (um chunk por vez) — apropriado para tabelas grandes
    ou quando concorrência não é necessária/desejada.

    Args:
        client (SidraClient): Cliente configurado para realizar requisições.
        chunks (list[DownloadChunk]): Lista de chunks a baixar.
        politeness_delay (float): Pausa (segundos) entre o disparo de cada request.

    Yields:
        tuple[DownloadChunk, list[dict]]: A tuple of the chunk and its retrieved rows.
    """
    for i, chunk in enumerate(chunks):
        url = chunk.parametro.url()
        rows = _validar_linhas(url, client.get(url))
        yield chunk, rows
        if politeness_delay and i < len(chunks) - 1:
            time.sleep(politeness_delay)


@dataclass(frozen=True)
class DownloadResult:
    """Resultado de :func:`download_agregado_dados`.

    Attributes:
        baixados: Arquivos de dados gravados nesta execução.
        existentes: Arquivos que já existiam (pedaço em dia, pulado).
        metadados: ``metadados@AAAAMMDD.json`` gravado agora, ou ``None`` se o
            sha256 não mudou desde a última versão.
    """

    baixados: list[Path] = field(default_factory=list)
    existentes: list[Path] = field(default_factory=list)
    metadados: Path | None = None


def modificacao_do_chunk(
    chunk: DownloadChunk, modificacoes: Mapping[str, dt.date | None]
) -> dt.date | None:
    """Data de modificação de um pedaço: a maior dos períodos que ele cobre.

    Cada pedaço costuma cobrir um período só (a divisão começa por período);
    quando cobre vários (tabela pequena que cabe numa consulta), vale a maior
    data, de modo que uma revisão de qualquer período gera arquivo novo.
    ``None`` se a API não informou data para nenhum deles.
    """
    cobertos = chunk.parametro.periodos or list(modificacoes)
    datas = [d for pid in cobertos if isinstance(d := modificacoes.get(pid), dt.date)]
    return max(datas) if datas else None


def nomes_do_plano(chunks: list[DownloadChunk], agregado: Agregado) -> list[str]:
    """Nomes de arquivo (``<consulta>@AAAAMMDD.json``) que o plano vai gerar."""
    modificacoes = {p.id: p.modificacao for p in agregado.periodos}
    return [
        nome_arquivo(c.parametro, modificacao_do_chunk(c, modificacoes))[0]
        for c in chunks
    ]


def _baixar_chunk(
    client: SidraClient,
    repo: DataRepository,
    chunk: DownloadChunk,
    modificacao: dt.date | None,
    *,
    limiter: RateLimiter,
    acquire_slot: Callable[[str], Any] | None,
) -> Path:
    """Baixa um pedaço e grava a resposta crua + sidecar."""
    url = chunk.parametro.url()
    ctx = (
        acquire_slot(f"Nível {chunk.nivel_territorial}")
        if acquire_slot
        else contextlib.nullcontext()
    )
    with ctx:
        limiter.acquire()
        content = client.get_bytes(url)
    try:
        linhas = json.loads(content)
    except ValueError as exc:
        raise FetchError(f"Resposta do SIDRA não é JSON válido em {url}") from exc
    _validar_linhas(url, linhas)
    return repo.write_dados(
        chunk.parametro,
        modificacao,
        content,
        url=url,
        nivel_territorial=chunk.nivel_territorial,
    )


def download_agregado_dados(
    client: SidraClient,
    agregado_id: int,
    output_dir: str | Path,
    *,
    agregado: Agregado | None = None,
    niveis_territoriais: list[str] | None = None,
    variaveis: list[str] | None = None,
    periodos: list[str] | None = None,
    classificacoes: dict[str, list[str]] | None = None,
    limit: int = SIDRA_API_VALUES_LIMIT,
    max_workers: int = 4,
    politeness_delay: float = 0.0,
    on_chunk_done: Callable[[DownloadChunk], None] | None = None,
    acquire_slot: Callable[[str], Any] | None = None,
    hoje: dt.date | None = None,
) -> DownloadResult:
    """Baixa os dados de um agregado: um arquivo por consulta ao ``/values``.

    Cada pedaço do plano vira ``<output_dir>/t-<id>/<consulta>@AAAAMMDD.json``
    com a resposta crua do servidor (byte a byte, sem mesclar), gravada de forma
    atômica, mais o sidecar ``.manifest.json`` (``source_id="ibge-sidra"``,
    ``dataset_id="t-<id>"``, URL e sha256). Pedaços cujo arquivo já existe são
    pulados: uma revisão do IBGE muda a data e gera arquivo novo, sem apagar o
    antigo. Também grava ``metadados@AAAAMMDD.json`` (data da coleta) quando o
    sha256 dos metadados mudou.

    Args:
        client: Cliente já configurado para fazer os requests.
        agregado_id: ID do agregado a baixar.
        output_dir: Raiz do bruto (``t-<id>/`` é criado dentro dela).
        agregado: Metadados já carregados, para evitar um round-trip extra.
        max_workers: Concorrência máxima de requests.
        politeness_delay: Pausa (segundos) entre o disparo de cada request.
        on_chunk_done: Callback após cada pedaço concluído ou pulado.
        acquire_slot: Fábrica de contexto para limitar a concorrência por nível.
        hoje: Data da coleta dos metadados (padrão: hoje).

    Returns:
        :class:`DownloadResult` com os arquivos gravados, os já existentes e o
        arquivo de metadados novo (se houve).
    """
    if agregado is None:
        agregado = client.get_agregado(agregado_id)

    chunks = plan_agregado_download(
        agregado,
        niveis_territoriais=niveis_territoriais,
        variaveis=variaveis,
        periodos=periodos,
        classificacoes=classificacoes,
        limit=limit,
    )

    repo = DataRepository(output_dir)
    modificacoes = {p.id: p.modificacao for p in agregado.periodos}
    hoje = hoje or dt.date.today()

    conteudo_meta = json.dumps(
        agregado.asdict(), ensure_ascii=False, indent=2, cls=DateEncoder
    ).encode("utf-8")
    metadados = repo.write_metadados(
        agregado_id,
        conteudo_meta,
        hoje,
        url=agregado.url
        or f"https://servicodados.ibge.gov.br/api/v3/agregados/{agregado_id}",
        metadata={"nome": agregado.nome, "pesquisa": agregado.pesquisa.nome},
    )

    resultado = DownloadResult(metadados=metadados)
    pendentes: list[tuple[DownloadChunk, dt.date | None]] = []
    for chunk in chunks:
        modificacao = modificacao_do_chunk(chunk, modificacoes)
        alvo = repo.path_dados(chunk.parametro, modificacao)
        if alvo.exists():
            resultado.existentes.append(alvo)
            sidecar = alvo.with_name(alvo.name + ".manifest.json")
            if not sidecar.exists():  # queda entre o dado e o sidecar
                _nome, hashes = nome_arquivo(chunk.parametro, modificacao)
                repo.write_dados_manifest(
                    alvo,
                    chunk.parametro,
                    modificacao,
                    alvo.read_bytes(),
                    url=chunk.parametro.url(),
                    nivel_territorial=chunk.nivel_territorial,
                    hashes=hashes,
                )
            if on_chunk_done is not None:
                on_chunk_done(chunk)
        else:
            pendentes.append((chunk, modificacao))

    limiter = RateLimiter(politeness_delay)
    with graceful_executor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _baixar_chunk,
                client,
                repo,
                chunk,
                modificacao,
                limiter=limiter,
                acquire_slot=acquire_slot,
            ): chunk
            for chunk, modificacao in pendentes
        }
        for future in as_completed(futures):
            resultado.baixados.append(future.result())
            if on_chunk_done is not None:
                on_chunk_done(futures[future])
    resultado.baixados.sort()
    return resultado

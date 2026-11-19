# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

"""Layout do bruto do ``ibge-sidra``: um arquivo por consulta ao ``/values``.

Layout (ADR 2026-11-19, D2-D6)::

    <raiz>/t-<id>/t-<id>_p-<periodo>_f-<formato>_n<nivel>-<locs>_v-<vars>[_c<id>-<cats>...]@AAAAMMDD.json
    <raiz>/t-<id>/metadados@AAAAMMDD.json
    <raiz>/indice_pesquisas@AAAAMMDD.json

O miolo do nome é o caminho da URL de :meth:`Parametro.url` com cada par
``/<chave>/<valor>`` virando ``_<chave>-<valor>`` (ordem do nome: ``t``, ``p``,
``f``, ``n...``, ``v``, ``c...``). ``@AAAAMMDD`` é a data de modificação do
período no SIDRA (``@00010101`` quando a API não informa). O arquivo guarda a
resposta crua do servidor, e a existência dele é o critério de frescor.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import re
import tempfile
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from quantilica.core.manifests import DownloadManifest, write_manifest_sidecar
from quantilica.core.storage import BaseDataRepository

from .sidra import Formato, Parametro

SOURCE_ID = "ibge-sidra"
PRODUCER = "ibge-sidra-fetcher"
MAX_NOME = 200
SEM_MODIFICACAO = dt.date(1, 1, 1)

_RE_DATA = re.compile(r"^(?P<stem>.+)@(?P<data>\d{8}|\d{4}-\d{2}-\d{2})\.json$")
_RE_CHAVE = re.compile(r"^(t|p|f|v|n\d+|c\d+)$")
_RE_HASH = re.compile(r"^h[0-9a-f]{8}$")


def stamp_data(data: dt.date | None) -> str:
    """Formata a data como ``AAAAMMDD`` (``00010101`` quando ausente)."""
    if not isinstance(data, dt.date):
        data = SEM_MODIFICACAO
    return f"{data.year:04d}{data.month:02d}{data.day:02d}"


def parse_data(texto: str) -> dt.date:
    """Converte ``AAAAMMDD`` (ou o legado ``AAAA-MM-DD``) em data."""
    limpo = texto.replace("-", "")
    return dt.date(int(limpo[:4]), int(limpo[4:6]), int(limpo[6:8]))


def hash_lista(valores: Iterable[str]) -> str:
    """Token ``h`` + 8 hex do sha256 da lista ordenada (separada por vírgulas)."""
    ordenada = ",".join(sorted(str(v) for v in valores))
    return "h" + hashlib.sha256(ordenada.encode("utf-8")).hexdigest()[:8]


def _valor(lista: list[str]) -> str:
    return ",".join(lista) if lista else "all"


@dataclass(frozen=True)
class NomeConsulta:
    """Nome (sem data nem extensão) de uma consulta.

    Attributes:
        stem: Miolo do nome, ex.: ``t-1086_p-199701_f-a_n3-all_v-all``.
        hashes: Listas que viraram token ``h<hash>``, por chave do token
            (``"n6"``, ``"v"``, ``"c81"``, ``"p"``), com a lista completa.
    """

    stem: str
    hashes: dict[str, list[str]] = field(default_factory=dict)


def nome_consulta(parametro: Parametro, *, max_len: int = MAX_NOME) -> NomeConsulta:
    """Monta o nome da consulta a partir de um :class:`Parametro`.

    Quando uma lista deixaria o nome com mais de ``max_len`` caracteres, o token
    dela vira ``h`` + 8 hex do sha256 da lista ordenada (a mais longa primeiro,
    até caber). A lista completa fica em ``NomeConsulta.hashes``.
    """
    tokens: list[tuple[str, list[str] | None, str]] = []
    # (chave, lista|None, valor renderizado)
    tokens.append(("t", None, str(parametro.agregado)))
    tokens.append(("p", list(parametro.periodos), _valor(parametro.periodos)))
    tokens.append(("f", None, parametro.formato.value))
    for nivel, locs in parametro.territorios.items():
        tokens.append((f"n{nivel}", list(locs), _valor(locs)))
    tokens.append(("v", list(parametro.variaveis), _valor(parametro.variaveis)))
    for cid, cats in parametro.classificacoes.items():
        tokens.append((f"c{cid}", list(cats), _valor(cats)))

    hashes: dict[str, list[str]] = {}

    def render() -> str:
        return "_".join(f"{c}-{v}" for c, _lista, v in tokens)

    while len(render()) > max_len:
        candidatos = [
            i
            for i, (c, lista, _v) in enumerate(tokens)
            if lista is not None and len(lista) > 1 and c not in hashes
        ]
        if not candidatos:
            break
        i = max(candidatos, key=lambda k: len(tokens[k][2]))
        chave, lista, _v = tokens[i]
        assert lista is not None
        hashes[chave] = lista
        tokens[i] = (chave, lista, hash_lista(lista))
    return NomeConsulta(stem=render(), hashes=hashes)


def nome_arquivo(
    parametro: Parametro, modificacao: dt.date | None
) -> tuple[str, dict[str, list[str]]]:
    """Nome do arquivo (``<consulta>@AAAAMMDD.json``) e listas hasheadas."""
    nome = nome_consulta(parametro)
    return f"{nome.stem}@{stamp_data(modificacao)}.json", nome.hashes


@dataclass(frozen=True)
class ConsultaArquivo:
    """Resultado de :func:`parse_nome_arquivo`.

    Attributes:
        parametro: Parâmetros da consulta. Lista vazia equivale a ``all``.
            Um token ``h<hash>`` fica como lista de um item (o próprio token)
            e é listado em ``hashes``.
        modificacao: Data do sufixo ``@AAAAMMDD``.
        hashes: Chaves de token (``"n6"``, ``"v"``...) cuja lista não é
            recuperável pelo nome (está em ``metadata.parametros`` do manifesto).
    """

    parametro: Parametro
    modificacao: dt.date
    hashes: tuple[str, ...] = ()
    path: Path | None = None

    @property
    def stem(self) -> str:
        return nome_consulta(self.parametro).stem


def parse_nome_arquivo(nome: str) -> ConsultaArquivo | None:
    """Converte o nome de um arquivo de dados em parâmetros (inverso do nome).

    Aceita os nomes do acervo: vários ``n`` no mesmo nome, ``c<id>-`` vazio,
    listas de variáveis, formato ``c`` e a data ``@AAAA-MM-DD`` legada.
    Devolve ``None`` quando o nome não é de um arquivo de dados.
    """
    m = _RE_DATA.match(Path(nome).name)
    if not m:
        return None
    try:
        modificacao = parse_data(m.group("data"))
    except ValueError:
        return None
    agregado = None
    periodos: list[str] = []
    formato = Formato.A
    territorios: dict[str, list[str]] = {}
    variaveis: list[str] = []
    classificacoes: dict[str, list[str]] = {}
    hashes: list[str] = []
    for token in m.group("stem").split("_"):
        chave, sep, valor = token.partition("-")
        if not sep or not _RE_CHAVE.match(chave):
            return None
        lista = [] if valor in ("", "all") else valor.split(",")
        if len(lista) == 1 and _RE_HASH.match(lista[0]):
            hashes.append(chave)
        if chave == "t":
            agregado = valor
        elif chave == "p":
            periodos = lista
        elif chave == "f":
            try:
                formato = Formato(valor)
            except ValueError:
                return None
        elif chave == "v":
            variaveis = lista
        elif chave.startswith("n"):
            territorios[chave[1:]] = lista
        else:
            classificacoes[chave[1:]] = lista
    if agregado is None or not territorios:
        return None
    parametro = Parametro(
        agregado=agregado,
        territorios=territorios,
        variaveis=variaveis,
        periodos=periodos,
        classificacoes=classificacoes,
        formato=formato,
    )
    return ConsultaArquivo(
        parametro=parametro, modificacao=modificacao, hashes=tuple(hashes)
    )


def listar_dados(pasta: Path | str) -> list[ConsultaArquivo]:
    """Arquivos de dados de um diretório ``t-<id>``, com o nome interpretado.

    Ignora ``metadados@...``, sidecars e arquivos que não seguem o layout.
    """
    pasta = Path(pasta)
    if not pasta.is_dir():
        return []
    achados: list[ConsultaArquivo] = []
    for arq in sorted(pasta.iterdir()):
        if arq.name.startswith("metadados@") or arq.name.endswith(".manifest.json"):
            continue
        consulta = parse_nome_arquivo(arq.name)
        if consulta is not None:
            achados.append(
                ConsultaArquivo(
                    parametro=consulta.parametro,
                    modificacao=consulta.modificacao,
                    hashes=consulta.hashes,
                    path=arq,
                )
            )
    return achados


def atomic_write_bytes(path: Path, content: bytes) -> None:
    """Grava ``content`` em ``path`` de forma atômica (tmp + replace)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp_path = Path(tmp)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


class DataRepository(BaseDataRepository):
    """Repositório do bruto do ``ibge-sidra`` (um arquivo por consulta).

    Args:
        root: Diretório raiz (ex.: ``/data/ibge-sidra``).
    """

    def __init__(self, root: Path | str):
        super().__init__(root)
        self.source_id = SOURCE_ID

    # -- caminhos ---------------------------------------------------------
    @staticmethod
    def dataset_id(agregado_id: int | str) -> str:
        """``dataset_id`` da tabela: ``t-<id>``."""
        return f"t-{agregado_id}"

    def dir_tabela(self, agregado_id: int | str) -> Path:
        """Diretório da tabela: ``<raiz>/t-<id>``."""
        return self.storage.path_for(self.dataset_id(agregado_id))

    def path_dados(self, parametro: Parametro, modificacao: dt.date | None) -> Path:
        """Caminho do arquivo de uma consulta: ``t-<id>/<nome>@AAAAMMDD.json``."""
        nome, _hashes = nome_arquivo(parametro, modificacao)
        return self.dir_tabela(parametro.agregado) / nome

    def path_metadados(self, agregado_id: int | str, data: dt.date) -> Path:
        """Caminho de ``t-<id>/metadados@AAAAMMDD.json`` (data da coleta)."""
        return self.dir_tabela(agregado_id) / f"metadados@{stamp_data(data)}.json"

    def path_indice(self, data: dt.date) -> Path:
        """Caminho de ``indice_pesquisas@AAAAMMDD.json`` na raiz."""
        return self.storage.root / f"indice_pesquisas@{stamp_data(data)}.json"

    def list_dados(self, agregado_id: int | str) -> list[ConsultaArquivo]:
        """Arquivos de dados da tabela (nome já interpretado), ordenados."""
        return listar_dados(self.dir_tabela(agregado_id))

    # -- gravação ---------------------------------------------------------
    def write_manifest_for(
        self,
        path: Path,
        *,
        dataset_id: str,
        url: str,
        content: bytes | None = None,
        sha256: str | None = None,
        size_bytes: int | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> Path:
        """Gera e grava o manifesto sidecar para um arquivo do repositório."""
        if content is not None:
            manifest = DownloadManifest.from_content(
                source_id=self.source_id,
                dataset_id=dataset_id,
                url=url,
                content=content,
                path=str(path.absolute()),
                producer=PRODUCER,
                metadata=metadata,
            )
        elif sha256 is not None and size_bytes is not None:
            manifest = DownloadManifest.from_digest(
                source_id=self.source_id,
                dataset_id=dataset_id,
                url=url,
                sha256=sha256,
                size_bytes=size_bytes,
                path=str(path.absolute()),
                producer=PRODUCER,
                metadata=metadata,
            )
        else:
            raise ValueError("Informe 'content' ou ('sha256' e 'size_bytes')")
        return write_manifest_sidecar(path, manifest)

    def write_dados(
        self,
        parametro: Parametro,
        modificacao: dt.date | None,
        content: bytes,
        *,
        url: str,
        nivel_territorial: str | None = None,
    ) -> Path:
        """Grava a resposta crua de uma consulta + sidecar ``.manifest.json``."""
        nome, hashes = nome_arquivo(parametro, modificacao)
        path = self.dir_tabela(parametro.agregado) / nome
        atomic_write_bytes(path, content)
        self.write_dados_manifest(
            path,
            parametro,
            modificacao,
            content,
            url=url,
            nivel_territorial=nivel_territorial,
            hashes=hashes,
        )
        return path

    def write_dados_manifest(
        self,
        path: Path,
        parametro: Parametro,
        modificacao: dt.date | None,
        content: bytes,
        *,
        url: str,
        nivel_territorial: str | None = None,
        hashes: Mapping[str, list[str]] | None = None,
    ) -> Path:
        """Grava (ou regrava) o sidecar de um arquivo de dados."""
        metadata: dict[str, Any] = {
            "modificacao": (modificacao or SEM_MODIFICACAO).isoformat(),
            "periodos": list(parametro.periodos),
        }
        if nivel_territorial:
            metadata["nivel_territorial"] = nivel_territorial
        if hashes:
            metadata["parametros"] = {k: list(v) for k, v in hashes.items()}
        return self.write_manifest_for(
            path,
            dataset_id=self.dataset_id(parametro.agregado),
            url=url,
            content=content,
            metadata=metadata,
        )

    def _write_versionado(
        self,
        pasta: Path,
        prefixo: str,
        data: dt.date,
        content: bytes,
        *,
        dataset_id: str,
        url: str,
        metadata: Mapping[str, Any] | None,
    ) -> Path | None:
        """Grava ``<prefixo>@AAAAMMDD.json`` só se o sha256 mudou.

        Compara com a versão mais recente já existente. Devolve o caminho
        gravado ou ``None`` quando o conteúdo é igual ao da última versão.
        """
        existentes = sorted(
            p
            for p in pasta.glob(f"{prefixo}@*.json")
            if re.fullmatch(rf"{re.escape(prefixo)}@\d{{8}}\.json", p.name)
        )
        if existentes:
            ultimo = existentes[-1].read_bytes()
            if hashlib.sha256(ultimo).digest() == hashlib.sha256(content).digest():
                return None
        path = pasta / f"{prefixo}@{stamp_data(data)}.json"
        atomic_write_bytes(path, content)
        self.write_manifest_for(
            path,
            dataset_id=dataset_id,
            url=url,
            content=content,
            metadata=metadata,
        )
        return path

    def write_metadados(
        self,
        agregado_id: int | str,
        content: bytes,
        data: dt.date,
        *,
        url: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> Path | None:
        """Grava ``t-<id>/metadados@AAAAMMDD.json`` se o sha256 mudou."""
        return self._write_versionado(
            self.dir_tabela(agregado_id),
            "metadados",
            data,
            content,
            dataset_id=self.dataset_id(agregado_id),
            url=url,
            metadata=metadata,
        )

    def write_indice(
        self,
        content: bytes,
        data: dt.date,
        *,
        url: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> Path | None:
        """Grava ``indice_pesquisas@AAAAMMDD.json`` se o sha256 mudou."""
        return self._write_versionado(
            self.storage.root,
            "indice_pesquisas",
            data,
            content,
            dataset_id="indice_pesquisas",
            url=url,
            metadata=metadata,
        )

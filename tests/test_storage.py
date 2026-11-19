# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

"""Testes do layout do bruto: nome da consulta, ida e volta, versões."""

import datetime as dt
import hashlib
import json
from pathlib import Path

import pytest

from ibge_sidra_fetcher.sidra import Formato, Parametro
from ibge_sidra_fetcher.storage import (
    DataRepository,
    hash_lista,
    nome_arquivo,
    nome_consulta,
    parse_nome_arquivo,
)


def _param(**kw) -> Parametro:
    base = {
        "agregado": "1086",
        "territorios": {"3": []},
        "variaveis": [],
        "periodos": ["199701"],
        "classificacoes": {"12716": [], "12529": []},
    }
    base.update(kw)
    return Parametro(**base)


def test_nome_a_partir_de_parametro() -> None:
    nome, hashes = nome_arquivo(_param(), dt.date(2009, 10, 27))
    assert (
        nome == "t-1086_p-199701_f-a_n3-all_v-all_c12716-all_c12529-all@20091027.json"
    )
    assert hashes == {}


def test_nome_ordem_t_p_f_n_v_c_e_varios_n() -> None:
    p = Parametro(
        agregado="7",
        territorios={"1": [], "2": [], "3": []},
        variaveis=["1", "2", "3", "4"],
        periodos=["2003"],
        classificacoes={},
        formato=Formato.C,
    )
    assert nome_consulta(p).stem == "t-7_p-2003_f-c_n1-all_n2-all_n3-all_v-1,2,3,4"


def test_periodo_sem_modificacao_vira_00010101() -> None:
    nome, _ = nome_arquivo(_param(), None)
    assert nome.endswith("@00010101.json")


def test_lista_longa_vira_hash_e_vai_para_hashes() -> None:
    municipios = [str(3500000 + i) for i in range(400)]
    p = _param(territorios={"6": municipios})
    nome, hashes = nome_arquivo(p, dt.date(2024, 1, 2))
    esperado = hash_lista(municipios)
    assert f"_n6-{esperado}_" in nome
    assert len(esperado) == 9 and esperado.startswith("h")
    assert len(nome) < 250
    assert hashes == {"n6": municipios}
    # o hash é da lista ordenada: independe da ordem
    assert hash_lista(reversed(municipios)) == esperado


def test_lista_curta_nao_vira_hash() -> None:
    _nome, hashes = nome_arquivo(_param(variaveis=["1", "2"]), dt.date(2024, 1, 2))
    assert hashes == {}


# nomes reais do acervo (data já sem hífens)
ACERVO = [
    "t-1000_p-2003_f-a_n6-all_v-allxp_c81-allxt@20131030.json",
    "t-1086_p-199701_f-a_n3-all_v-all_c12716-_c12529-@20091027.json",
    "t-5_p-2010_f-a_n1-all_n2-all_n3-all_v-9_c1-2@20100101.json",
    "t-8_p-201201_f-a_n6-all_v-1,2,3,4@20120301.json",
    "t-9_p-199801,199802_f-c_n3-1,2_v-all@00010101.json",
    "t-10_p-201301-201312_f-a_n1-all_v-all@20140101.json",
]


@pytest.mark.parametrize("nome", ACERVO)
def test_ida_e_volta_nome_parametros(nome: str) -> None:
    consulta = parse_nome_arquivo(nome)
    assert consulta is not None
    # o c<id>- vazio do acervo equivale a "all"; o nome novo nunca o grava
    esperado = nome.replace("c12716-_", "c12716-all_").replace(
        "c12529-@", "c12529-all@"
    )
    novo, _ = nome_arquivo(consulta.parametro, consulta.modificacao)
    assert novo == esperado


def test_parse_valores_do_acervo() -> None:
    c = parse_nome_arquivo(ACERVO[0])
    assert c is not None
    assert c.parametro.agregado == "1000"
    assert c.parametro.periodos == ["2003"]
    assert c.parametro.territorios == {"6": []}
    assert c.parametro.variaveis == ["allxp"]
    assert c.parametro.classificacoes == {"81": ["allxt"]}
    assert c.modificacao == dt.date(2013, 10, 30)

    c = parse_nome_arquivo(ACERVO[1])
    assert c is not None
    assert c.parametro.classificacoes == {"12716": [], "12529": []}

    c = parse_nome_arquivo(ACERVO[2])
    assert c is not None
    assert list(c.parametro.territorios) == ["1", "2", "3"]

    c = parse_nome_arquivo(ACERVO[4])
    assert c is not None
    assert c.parametro.formato is Formato.C
    assert c.parametro.periodos == ["199801", "199802"]
    assert c.modificacao == dt.date(1, 1, 1)


def test_parse_aceita_data_legada_com_hifens_e_marca_hash() -> None:
    c = parse_nome_arquivo("t-1_p-2003_f-a_n6-hdeadbeef_v-all@2013-10-30.json")
    assert c is not None
    assert c.modificacao == dt.date(2013, 10, 30)
    assert c.hashes == ("n6",)


@pytest.mark.parametrize(
    "nome",
    ["metadados@20260101.json", "lixo.json", "t-1_p-2@20260101.json", "x@1.json"],
)
def test_parse_rejeita_o_que_nao_e_dado(nome: str) -> None:
    assert parse_nome_arquivo(nome) is None


def test_repository_paths(tmp_path: Path) -> None:
    repo = DataRepository(tmp_path)
    assert repo.source_id == "ibge-sidra"
    p = _param()
    assert repo.path_dados(p, dt.date(2009, 10, 27)) == (
        tmp_path
        / "t-1086"
        / "t-1086_p-199701_f-a_n3-all_v-all_c12716-all_c12529-all@20091027.json"
    )
    assert repo.path_metadados(1086, dt.date(2026, 11, 19)) == (
        tmp_path / "t-1086" / "metadados@20261119.json"
    )
    assert repo.path_indice(dt.date(2026, 11, 19)) == (
        tmp_path / "indice_pesquisas@20261119.json"
    )


def test_write_dados_grava_bruto_e_manifesto(tmp_path: Path) -> None:
    repo = DataRepository(tmp_path)
    p = _param()
    bruto = b'[{"NC":"x"},\n {"NC":"1","V":"10"}]\n'
    path = repo.write_dados(
        p, dt.date(2009, 10, 27), bruto, url=p.url(), nivel_territorial="N3"
    )
    assert path.read_bytes() == bruto
    manifest = json.loads(Path(str(path) + ".manifest.json").read_text())
    assert manifest["source_id"] == "ibge-sidra"
    assert manifest["dataset_id"] == "t-1086"
    assert manifest["url"] == p.url()
    assert manifest["sha256"] == hashlib.sha256(bruto).hexdigest()
    assert manifest["metadata"]["modificacao"] == "2009-10-27"
    assert [a.path for a in repo.list_dados(1086)] == [path]


def test_write_dados_guarda_lista_hasheada_no_manifesto(tmp_path: Path) -> None:
    repo = DataRepository(tmp_path)
    municipios = [str(3500000 + i) for i in range(400)]
    p = _param(territorios={"6": municipios})
    path = repo.write_dados(p, dt.date(2024, 1, 2), b"[]", url=p.url())
    manifest = json.loads(Path(str(path) + ".manifest.json").read_text())
    assert manifest["metadata"]["parametros"] == {"n6": municipios}


def test_metadados_so_regrava_quando_o_sha_muda(tmp_path: Path) -> None:
    repo = DataRepository(tmp_path)
    url = "https://servicodados.ibge.gov.br/api/v3/agregados/1086"
    a = repo.write_metadados(1086, b'{"v":1}', dt.date(2026, 11, 1), url=url)
    assert a is not None and a.name == "metadados@20261101.json"
    # mesmo conteúdo, outra data: não grava
    assert repo.write_metadados(1086, b'{"v":1}', dt.date(2026, 11, 8), url=url) is None
    # conteúdo novo: grava versão nova e preserva a antiga
    c = repo.write_metadados(1086, b'{"v":2}', dt.date(2026, 11, 15), url=url)
    assert c is not None and c.name == "metadados@20261115.json"
    assert a.exists()
    # voltou ao conteúdo antigo, mas a última versão é a v2: grava
    d = repo.write_metadados(1086, b'{"v":1}', dt.date(2026, 11, 20), url=url)
    assert d is not None


def test_indice_so_regrava_quando_o_sha_muda(tmp_path: Path) -> None:
    repo = DataRepository(tmp_path)
    url = "https://servicodados.ibge.gov.br/api/v3/agregados"
    a = repo.write_indice(b"[1]", dt.date(2026, 11, 1), url=url)
    assert a == tmp_path / "indice_pesquisas@20261101.json"
    assert repo.write_indice(b"[1]", dt.date(2026, 11, 2), url=url) is None
    assert repo.write_indice(b"[2]", dt.date(2026, 11, 3), url=url) is not None

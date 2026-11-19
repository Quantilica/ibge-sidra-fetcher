# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

"""Round-trip tests for the agregado metadata JSON reader/writer pair."""

import datetime as dt
from pathlib import Path

from ibge_sidra_fetcher.agregados import (
    Agregado,
    AgregadoNivelTerritorial,
    Categoria,
    Classificacao,
    ClassificacaoSumarizacao,
    Localidade,
    NivelTerritorial,
    Periodicidade,
    Periodo,
    Pesquisa,
    Variavel,
)
from ibge_sidra_fetcher.reader import load_agregado, save_agregado


def _sample_agregado() -> Agregado:
    return Agregado(
        id=188,
        nome="Agregado de exemplo",
        url="https://servicodados.ibge.gov.br/api/v3/agregados/188",
        pesquisa=Pesquisa(id="5741", nome="Pesquisa de exemplo"),
        assunto="Produção agrícola",
        periodicidade=Periodicidade(frequencia="Anual", inicio="p0000", fim="p9999"),
        nivel_territorial=AgregadoNivelTerritorial(
            administrativo=["N1", "N2", "N3"], especial=[], ibge=[]
        ),
        variaveis=[
            Variavel(id="1", nome="Área plantada", unidade="Hectares", sumarizacao=[]),
            Variavel(
                id="4", nome="Quantidade produzida", unidade="Toneladas", sumarizacao=[]
            ),
        ],
        classificacoes=[
            Classificacao(
                id="15704",
                nome="Grão",
                sumarizacao=ClassificacaoSumarizacao(status=True, excecao=[0]),
                categorias=[
                    Categoria(id=0, nome="Total", unidade=None, nivel=0),
                    Categoria(id=1190, nome="Arroz", unidade=None, nivel=1),
                ],
            )
        ],
        periodos=[
            Periodo(
                id="A1974",
                literals=["1974"],
                modificacao=dt.date(2024, 5, 22),
                frequencia="anual",
                data_inicio=dt.date(1974, 1, 1),
                data_fim=dt.date(1974, 12, 31),
                ano=1974,
            )
        ],
        localidades=[
            Localidade(
                id="N1@1",
                nome="Brasil",
                nivel=NivelTerritorial(id="N1", nome="Brasil"),
            )
        ],
    )


def test_save_load_roundtrip(tmp_path: Path):
    """save_agregado -> load_agregado reproduz o mesmo Agregado."""
    agregado = _sample_agregado()
    path = tmp_path / "agregado_188.json"

    save_agregado(agregado, path)
    loaded = load_agregado(path)

    assert loaded == agregado


def test_save_load_roundtrip_writes_manifest(tmp_path: Path):
    """A carga não exige que o manifest esteja presente, mas save o grava."""
    agregado = _sample_agregado()
    path = tmp_path / "agregado.json"

    save_agregado(agregado, path)

    manifest_p = path.with_suffix(path.suffix + ".manifest.json")
    assert manifest_p.exists()
    import json

    manifest_data = json.loads(manifest_p.read_text(encoding="utf-8"))
    assert manifest_data["source_id"] == "ibge-sidra"
    assert load_agregado(path) == agregado


def test_load_agregado_empty_collections(tmp_path: Path):
    """Agregados mínimos (sem periodos/localidades/classificacoes) round-tripam."""
    agregado = Agregado(
        id=99,
        nome="Mínimo",
        url="",
        pesquisa=Pesquisa(id="1", nome="P"),
        assunto="A",
        periodicidade=Periodicidade(frequencia="mensal", inicio="p0", fim="p0"),
        nivel_territorial=AgregadoNivelTerritorial(
            administrativo=[], especial=[], ibge=[]
        ),
        variaveis=[],
        classificacoes=[],
        periodos=[],
        localidades=[],
    )
    path = tmp_path / "min.json"

    save_agregado(agregado, path)

    assert load_agregado(path) == agregado


def test_iter_dados_tabela_junta_pedacos_e_usa_a_ultima_versao(tmp_path: Path):
    """A mesclagem é etapa de leitura: cabeçalho uma vez, última versão vence."""
    import json

    from ibge_sidra_fetcher.reader import iter_dados_tabela, listar_arquivos_tabela

    pasta = tmp_path / "t-9"
    pasta.mkdir()

    def grava(nome: str, linhas: list) -> None:
        (pasta / nome).write_text(json.dumps(linhas), encoding="utf-8")

    cab = {"NC": "Nível Territorial (Código)", "V": "Valor"}
    grava(
        "t-9_p-201201_f-a_n3-all_v-all@20120301.json", [cab, {"NC": "3", "V": "velho"}]
    )
    grava(
        "t-9_p-201201_f-a_n3-all_v-all@20130101.json", [cab, {"NC": "3", "V": "novo"}]
    )
    grava("t-9_p-201202_f-a_n3-all_v-all@20120401.json", [cab, {"NC": "3", "V": "b"}])
    grava("t-9_p-201201_f-a_n6-all_v-all@20120301.json", [cab, {"NC": "6", "V": "m"}])
    grava("metadados@20260101.json", [])
    (pasta / "t-9_p-201201_f-a_n3-all_v-all@20130101.json.manifest.json").write_text(
        "{}"
    )

    assert len(listar_arquivos_tabela(tmp_path, 9)) == 3
    assert len(listar_arquivos_tabela(tmp_path, 9, ultima_versao=False)) == 4
    assert len(listar_arquivos_tabela(tmp_path, 9, nivel="N6")) == 1

    linhas = list(iter_dados_tabela(tmp_path, 9, nivel="N3"))
    assert linhas == [cab, {"NC": "3", "V": "novo"}, {"NC": "3", "V": "b"}]
    assert list(iter_dados_tabela(tmp_path, 404)) == []

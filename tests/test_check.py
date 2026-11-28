# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

"""Testes de check_agregado (verificação sem download) e do rename sync."""

from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from ibge_sidra_fetcher.agregados import Periodo
from ibge_sidra_fetcher.check import check_agregado
from ibge_sidra_fetcher.download import DownloadChunk, DownloadResult
from ibge_sidra_fetcher.plugin import app
from ibge_sidra_fetcher.sidra import Parametro

runner = CliRunner()


def _periodo(pid: str, dia: dt.date) -> Periodo:
    return Periodo(id=pid, literals=[pid], modificacao=dia)


def _agregado(periodos: list[Periodo], nome: str = "Agregado Fake") -> SimpleNamespace:
    return SimpleNamespace(
        id=1705, nome=nome, url="https://fake/1705", periodos=list(periodos)
    )


def _chunk(nivel: str, periodo: str) -> DownloadChunk:
    return DownloadChunk(
        nivel_territorial=nivel,
        parametro=Parametro(
            agregado="1705",
            territorios={nivel.removeprefix("N"): []},
            variaveis=[],
            periodos=[periodo],
            classificacoes={},
        ),
        n_valores=1,
    )


class _FakeClient:
    """Dublê de SidraClient com metadados enlatados."""

    def __init__(self, agregado, chunks: list) -> None:
        self._agregado = agregado
        self._chunks = chunks

    def get_agregado(self, agregado_id: int):
        assert agregado_id == self._agregado.id
        return self._agregado

    def plan_dados_agregado(self, agregado_id: int, agregado=None, **filtros):
        assert filtros.get("niveis_territoriais") in (None, ["N1"], ["N3"])
        return list(self._chunks)


def _client_2niveis() -> _FakeClient:
    periodos = [
        _periodo("202301", dt.date(2023, 6, 1)),
        _periodo("202302", dt.date(2024, 2, 1)),
    ]
    agregado = _agregado(periodos)
    chunks = [
        _chunk("N1", "202301"),
        _chunk("N1", "202302"),
        _chunk("N3", "202301"),
        _chunk("N3", "202302"),
    ]
    return _FakeClient(agregado, chunks)


def _grava(tmp_path: Path, nivel: str, periodo: str, data: str) -> Path:
    n = nivel.removeprefix("N")
    arq = tmp_path / "t-1705" / f"t-1705_p-{periodo}_f-a_n{n}-all_v-all@{data}.json"
    arq.parent.mkdir(parents=True, exist_ok=True)
    arq.write_text("[]", encoding="utf-8")
    return arq


def _por_nivel(veredictos) -> dict:
    return {v["partition"]: (v["action"], v["reason"]) for v in veredictos}


def test_check_tudo_ausente_baixa(tmp_path: Path) -> None:
    """Sem arquivos locais, todos os níveis são download/not-present."""
    veredictos, resumo = check_agregado(_client_2niveis(), 1705, tmp_path, {})
    assert resumo["n_niveis"] == 2
    assert [(v["partition"], v["action"], v["reason"]) for v in veredictos] == [
        ("N1", "download", "not-present"),
        ("N3", "download", "not-present"),
    ]
    assert veredictos[0]["remote_last_modified"] == "2024-02-01"
    assert veredictos[0]["entry"]["filtros"] == {}
    assert veredictos[0]["entry"]["nivel"] == "N1"
    assert veredictos[0]["entry"]["n_arquivos"] == 2
    assert not (tmp_path / "t-1705").exists()  # check não cria nada


def test_check_frescor_por_existencia_do_arquivo_com_a_data_da_api(
    tmp_path: Path,
) -> None:
    """Todos os arquivos com a data certa existem: skip-up-to-date."""
    _grava(tmp_path, "N1", "202301", "20230601")
    _grava(tmp_path, "N1", "202302", "20240201")
    veredictos, _ = check_agregado(_client_2niveis(), 1705, tmp_path, {})
    por_nivel = _por_nivel(veredictos)
    assert por_nivel["N1"] == ("skip-up-to-date", "local-fresh")
    assert por_nivel["N3"] == ("download", "not-present")


def test_check_ignora_mtime(tmp_path: Path) -> None:
    """Arquivo velho no disco, mas com o nome da data da API, continua fresco."""

    for periodo, data in (("202301", "20230601"), ("202302", "20240201")):
        arq = _grava(tmp_path, "N1", periodo, data)
        antigo = dt.datetime(2000, 1, 1).timestamp()
        os.utime(arq, (antigo, antigo))
    veredictos, _ = check_agregado(_client_2niveis(), 1705, tmp_path, {})
    assert _por_nivel(veredictos)["N1"] == ("skip-up-to-date", "local-fresh")


def test_check_revisao_do_ibge_pede_download(tmp_path: Path) -> None:
    """Arquivo com a data antiga: a data nova da API não existe, então baixa."""
    _grava(tmp_path, "N3", "202301", "20230601")
    antigo = _grava(tmp_path, "N3", "202302", "20230901")  # API diz 20240201
    veredictos, _ = check_agregado(_client_2niveis(), 1705, tmp_path, {})
    assert _por_nivel(veredictos)["N3"] == ("download", "remote-newer")
    assert antigo.exists()


def test_sync_e_download_produzem_mesmo_plano(monkeypatch, tmp_path: Path) -> None:
    """Alias depreciado delega ao sync (warning + mesmo comportamento)."""
    import ibge_sidra_fetcher.plugin as plugin_module

    client = _client_2niveis()
    monkeypatch.setattr(
        plugin_module, "SidraClient", lambda *a, **k: _NoNetClient(client)
    )
    r_sync = runner.invoke(app, ["sync", "1705", "-o", str(tmp_path), "--dry-run"])
    r_down = runner.invoke(app, ["download", "1705", "-o", str(tmp_path), "--dry-run"])
    assert r_sync.exit_code == 0, r_sync.output
    assert r_down.exit_code == 0, r_down.output
    assert "depreciado" in r_down.output
    assert "depreciado" not in r_sync.output


class _NoNetClient:
    """SidraClient fake sem rede para comandos Typer (plan + download vazio)."""

    def __init__(self, base: _FakeClient) -> None:
        self._base = base
        self.download_kwargs: dict = {}

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        return None

    def get_agregado(self, agregado_id: int):
        return self._base.get_agregado(agregado_id)

    def plan_dados_agregado(self, agregado_id: int, agregado=None, **filtros):
        return self._base.plan_dados_agregado(agregado_id, agregado=agregado, **filtros)

    def download_dados_agregado(self, agregado_id: int, output, **kwargs):
        self.download_kwargs = kwargs
        return DownloadResult()


def test_sync_from_plan_restringe_niveis(monkeypatch, tmp_path: Path) -> None:
    """sync --from-plan baixa só níveis com ação download."""
    import ibge_sidra_fetcher.plugin as plugin_module

    base = _client_2niveis()
    net = _NoNetClient(base)
    monkeypatch.setattr(plugin_module, "SidraClient", lambda *a, **k: net)

    veredictos, _ = check_agregado(base, 1705, tmp_path, {})
    plano = {
        "fetcher": "ibge-sidra-fetcher",
        "output_dir": str(tmp_path),
        "generated_at": "2026-10-07T00:00:00+00:00",
        "items": [
            {**v, "action": "skip-up-to-date", "reason": "local-fresh"}
            if v["partition"] == "N1"
            else v
            for v in veredictos
        ],
    }
    plan_file = tmp_path / "plan.json"
    plan_file.write_text(json.dumps(plano), encoding="utf-8")

    result = runner.invoke(
        app, ["sync", "1705", "-o", str(tmp_path), "--from-plan", str(plan_file)]
    )
    assert result.exit_code == 0, result.output
    assert net.download_kwargs.get("niveis_territoriais") == ["N3"]


def test_sync_liga_a_coleta_do_indice_de_pesquisas(monkeypatch, tmp_path: Path) -> None:
    """O comando de coleta (`sync`) pede o `indice_pesquisas@AAAAMMDD.json`."""
    import ibge_sidra_fetcher.plugin as plugin_module

    net = _NoNetClient(_client_2niveis())
    monkeypatch.setattr(plugin_module, "SidraClient", lambda *a, **k: net)
    result = runner.invoke(app, ["sync", "1705", "-o", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert net.download_kwargs.get("salvar_indice") is True


def test_sync_from_plan_rejeita_filtros_combinados(monkeypatch, tmp_path: Path) -> None:
    """--from-plan com filtros explícitos aborta com erro claro."""
    import ibge_sidra_fetcher.plugin as plugin_module

    net = _NoNetClient(_client_2niveis())
    monkeypatch.setattr(plugin_module, "SidraClient", lambda *a, **k: net)
    plan_file = tmp_path / "plan.json"
    plan_file.write_text(json.dumps({"items": []}), encoding="utf-8")
    result = runner.invoke(
        app,
        [
            "sync",
            "1705",
            "-o",
            str(tmp_path),
            "--from-plan",
            str(plan_file),
            "--niveis",
            "N1",
        ],
    )
    assert result.exit_code == 1
    assert "não combina com filtros" in result.output


def test_sync_from_plan_invalido_aborta(tmp_path: Path) -> None:
    """Plano inválido aborta com exit 1."""
    bad = tmp_path / "bad.json"
    bad.write_text('{"nao": "eh-plano"}', encoding="utf-8")
    result = runner.invoke(
        app, ["sync", "1705", "-o", str(tmp_path), "--from-plan", str(bad)]
    )
    assert result.exit_code == 1
    assert "Plano inválido" in result.output


def test_sync_sem_id_e_sem_plano_aborta(tmp_path: Path) -> None:
    """sync sem agregado e sem plano aborta com mensagem clara."""
    result = runner.invoke(app, ["sync", "-o", str(tmp_path)])
    assert result.exit_code == 1
    assert "informe o ID" in result.output


def test_check_json_tem_ids_completos(monkeypatch, tmp_path: Path) -> None:
    """check --json emite plano com ids sem truncamento."""
    import ibge_sidra_fetcher.plugin as plugin_module

    net = _NoNetClient(_client_2niveis())
    monkeypatch.setattr(plugin_module, "SidraClient", lambda *a, **k: net)
    result = runner.invoke(app, ["check", "1705", "-o", str(tmp_path), "--json"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert [it["id"] for it in data["items"]] == [
        "t-1705:N1",
        "t-1705:N3",
    ]


class _MultiClient:
    """SidraClient fake de vários agregados; registra os downloads pedidos."""

    def __init__(self, falha_em: set[int] | None = None) -> None:
        self.downloads: list[tuple[int, dict]] = []
        self._falha_em = falha_em or set()

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        return None

    def get_agregado(self, agregado_id: int):
        return SimpleNamespace(
            id=agregado_id, nome=f"Agregado {agregado_id}", url="https://fake"
        )

    def plan_dados_agregado(self, agregado_id: int, agregado=None, **filtros):
        return [_chunk("N1", "202301")]

    def download_dados_agregado(self, agregado_id: int, output, **kwargs):
        from quantilica.core.exceptions import FetchError

        if agregado_id in self._falha_em:
            raise FetchError(f"agregado {agregado_id} fora do ar")
        self.downloads.append((agregado_id, kwargs))
        return DownloadResult()


def _item(agregado_id: int, nivel: str, filtros: dict | None = None) -> dict:
    return {
        "dataset": f"t-{agregado_id}",
        "id": f"t-{agregado_id}:{nivel}",
        "url": "https://fake",
        "partition": nivel,
        "local_path": "/x",
        "remote_etag": None,
        "remote_last_modified": None,
        "remote_size": None,
        "local_exists": False,
        "action": "download",
        "reason": "not-present",
        "entry": {
            "id": f"t-{agregado_id}:{nivel}",
            "url": "https://fake",
            "dataset": f"t-{agregado_id}",
            "agregado_id": agregado_id,
            "nivel": nivel,
            "n_arquivos": 1,
            "n_presentes": 0,
            "filtros": filtros or {},
        },
    }


def _plano(tmp_path: Path, itens: list[dict]) -> Path:
    arq = tmp_path / "plan.json"
    arq.write_text(
        json.dumps(
            {
                "fetcher": "ibge-sidra-fetcher",
                "output_dir": str(tmp_path),
                "generated_at": "2026-11-28T00:00:00+00:00",
                "items": itens,
            }
        ),
        encoding="utf-8",
    )
    return arq


def test_sync_from_plan_com_agregados_diferentes_baixa_cada_um(
    monkeypatch, tmp_path: Path
) -> None:
    import ibge_sidra_fetcher.plugin as plugin_module

    net = _MultiClient()
    monkeypatch.setattr(plugin_module, "SidraClient", lambda *a, **k: net)
    plano = _plano(
        tmp_path,
        [_item(1705, "N1"), _item(1705, "N3"), _item(7060, "N1"), _item(7060, "N6")],
    )

    result = runner.invoke(
        app, ["sync", "-o", str(tmp_path), "--from-plan", str(plano)]
    )

    assert result.exit_code == 0, result.output
    assert [(a, k["niveis_territoriais"]) for a, k in net.downloads] == [
        (1705, ["N1", "N3"]),
        (7060, ["N1", "N6"]),
    ]


def test_sync_from_plan_mesmo_agregado_com_filtros_diferentes_vira_dois_pedidos(
    monkeypatch, tmp_path: Path
) -> None:
    import ibge_sidra_fetcher.plugin as plugin_module

    net = _MultiClient()
    monkeypatch.setattr(plugin_module, "SidraClient", lambda *a, **k: net)
    plano = _plano(
        tmp_path,
        [
            _item(1705, "N1", {"periodos": ["202301"]}),
            _item(1705, "N3", {"periodos": ["202302"]}),
        ],
    )

    result = runner.invoke(
        app, ["sync", "-o", str(tmp_path), "--from-plan", str(plano)]
    )

    assert result.exit_code == 0, result.output
    assert [(a, k["periodos"], k["niveis_territoriais"]) for a, k in net.downloads] == [
        (1705, ["202301"], ["N1"]),
        (1705, ["202302"], ["N3"]),
    ]


def test_sync_from_plan_falha_de_um_agregado_nao_impede_os_outros_e_sai_3(
    monkeypatch, tmp_path: Path
) -> None:
    import ibge_sidra_fetcher.plugin as plugin_module

    net = _MultiClient(falha_em={1705})
    monkeypatch.setattr(plugin_module, "SidraClient", lambda *a, **k: net)
    plano = _plano(tmp_path, [_item(1705, "N1"), _item(7060, "N1")])

    result = runner.invoke(
        app, ["sync", "-o", str(tmp_path), "--from-plan", str(plano)]
    )

    assert result.exit_code == 3  # parcial: um baixou, outro falhou
    assert [a for a, _ in net.downloads] == [7060]
    assert "1 de 2 pedido(s)" in result.output
    assert "1705" in result.output


def test_sync_from_plan_todos_os_agregados_falham_sai_1(
    monkeypatch, tmp_path: Path
) -> None:
    import ibge_sidra_fetcher.plugin as plugin_module

    net = _MultiClient(falha_em={1705, 7060})
    monkeypatch.setattr(plugin_module, "SidraClient", lambda *a, **k: net)
    plano = _plano(tmp_path, [_item(1705, "N1"), _item(7060, "N1")])

    result = runner.invoke(
        app, ["sync", "-o", str(tmp_path), "--from-plan", str(plano)]
    )

    assert result.exit_code == 1
    assert net.downloads == []
    assert "2 de 2 pedido(s)" in result.output


def test_sync_from_plan_agregado_invalido_aborta(monkeypatch, tmp_path: Path) -> None:
    import ibge_sidra_fetcher.plugin as plugin_module

    net = _MultiClient()
    monkeypatch.setattr(plugin_module, "SidraClient", lambda *a, **k: net)
    plano = _plano(tmp_path, [_item(1705, "N1"), _item("abc", "N1")])  # type: ignore[arg-type]

    result = runner.invoke(
        app, ["sync", "-o", str(tmp_path), "--from-plan", str(plano)]
    )

    assert result.exit_code == 1
    assert "Plano inválido" in result.output
    assert net.downloads == []


def test_cli_argparse_from_plan_com_agregados_diferentes(
    monkeypatch, tmp_path: Path
) -> None:
    import pytest

    import ibge_sidra_fetcher.cli as cli

    net = _MultiClient(falha_em={7060})
    monkeypatch.setattr(cli, "SidraClient", lambda *a, **k: net)
    plano = _plano(tmp_path, [_item(1705, "N1"), _item(7060, "N1")])

    with pytest.raises(SystemExit) as info:
        cli.main(["sync", "-o", str(tmp_path), "--from-plan", str(plano)])

    assert info.value.code == 3  # parcial: 1705 baixou, 7060 falhou
    assert [a for a, _ in net.downloads] == [1705]

# ibge-sidra-fetcher: Cliente Python para a API do IBGE/SIDRA

![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square) ![Python](https://img.shields.io/badge/python-3.13+-blue.svg?style=flat-square)

Biblioteca Python para buscar e processar dados e metadados das APIs oficiais do [IBGE](https://www.ibge.gov.br/) — [Agregados v3](https://servicodados.ibge.gov.br/api/docs/agregados?versao=3) e [SIDRA](https://apisidra.ibge.gov.br). Fornece acesso tipado via dataclasses a pesquisas, agregados, períodos, territórios, variáveis e classificações, com clientes síncrono e assíncrono.

Para a documentação completa, consulte [https://docs.quantilica.com](https://docs.quantilica.com).

## Layout do bruto

`sync` (e o alias `download`) grava **um arquivo por consulta** ao endpoint `/values`, com a resposta crua do servidor, sem mesclar:

```
<raiz>/t-<id>/t-<id>_p-<periodo>_f-<formato>_n<nivel>-<locs>_v-<vars>[_c<id>-<cats>...]@AAAAMMDD.json
<raiz>/t-<id>/metadados@AAAAMMDD.json
```

- O miolo do nome é o caminho da URL do `/values` (`/<chave>/<valor>` vira `_<chave>-<valor>`).
- `@AAAAMMDD` é a data de modificação do período no SIDRA (`@00010101` se a API não informa). Uma revisão do IBGE gera arquivo novo e o antigo fica.
- Pedaços cujo arquivo já existe são pulados; o `check` usa a mesma regra (existência do arquivo com a data da API).
- Listas longas (nome com mais de 200 caracteres) viram `h` + 8 hex do sha256 da lista ordenada; a lista completa fica em `metadata.parametros` do `.manifest.json`.
- `metadados@AAAAMMDD.json` leva a data da coleta e só é regravado quando o sha256 muda.
- `source_id` e diretório de saída padrão: `ibge-sidra` (`/data/ibge-sidra`).
- `reader.iter_dados_tabela(raiz, id)` junta os pedaços na leitura.

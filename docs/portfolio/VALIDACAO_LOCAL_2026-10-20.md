# Validação local da demonstração — 2026-10-20

Esta validação foi feita em um ambiente Python 3.11.17, com dados públicos da FiveThirtyEight. O commit base era `68e6ecbd11e71f3ca242aba06e00a043d83fa3a4`. As melhorias da vitrine ainda estavam sem commit quando o manifesto foi gerado; o SHA identifica a base do trabalho, não um estado Git limpo e publicável.

| Comando | Resultado observado |
| --- | --- |
| `make setup` | Dependências instaladas e kernel `notebook_lakehouse` registrado. |
| `make run-all` | Oito notebooks concluídos usando Python 3.11; dois CSVs locais reutilizados do cache. |
| `make dbt-test` | 14 testes aprovados; 0 erros, 0 avisos. |
| `make dq` | Checkpoint Great Expectations concluído; 7 verificações, 0 violações nesta amostra. |
| `make dbt-preview` | Seis tabelas consultáveis no DuckDB, incluindo dois marts de KPI. |
| `make portfolio-export` | Exportação final `20261020T184424Z`. |
| `make showcase-evidence` | Manifesto e duas imagens selecionadas gerados após conferir horários, contagens e resultados de qualidade. |
| `make lint`, `make fmt-check`, `python -m pytest -q` | Lint e formatação aprovados; 7 testes Python aprovados. |
| Showcase local | `/`, `/architecture`, `/lineage` e `/dq` responderam HTTP 200. |

O primeiro `make run-all` no terminal restrito parou antes do primeiro notebook porque o Jupyter não pôde abrir portas locais. A execução foi repetida com acesso a sockets e terminou com sucesso. Nenhum serviço Docker ou Mode 2 foi iniciado nesta validação.

O [manifesto selecionado](../../showcase/static/evidence/manifest.json) identifica a amostra. As contagens e aprovações se referem exclusivamente a ela. Os datasets são históricos e não industriais. O gráfico de datas derivadas do pacote completo não mede vazão real; o gráfico de tempos usa referência local de 20 segundos e não comprova SLA de produção. Esses dois gráficos ficaram fora da vitrine.

Antes de publicar externamente, revisar o diff, registrar as mudanças no Git conforme o fluxo do projeto e, se a página for atualizada com outra execução, regenerar as evidências e capturas.

## Revalidação local — 2026-10-21

Após as alterações da demonstração, os comandos foram repetidos no Python 3.11. `make lint`, `make fmt-check` e `.venv/bin/python -m pytest -q` passaram (7 testes). `make run-all` executou os notebooks do Mode 1 usando os CSVs locais em cache; os seis modelos dbt terminaram sem erros. `make dbt-test` aprovou 14/14 testes; `make dq` concluiu sete verificações sem violações. `make portfolio-export` gerou a execução `20261021T113451Z`, e `make showcase-evidence` gerou o manifesto e os gráficos selecionados para essa execução.

O showcase iniciado por `make showcase` respondeu HTTP 200 nas rotas `/`, `/architecture`, `/lineage`, `/dq` e nos recursos `/static/evidence/manifest.json`, os dois PNGs selecionados e `/static/favicon.svg`. A conexão local foi verificada fora do sandbox após o sandbox bloquear sockets localhost. O servidor foi encerrado após a checagem.

O campo `git_sha` do manifesto continua identificando o commit base `68e6ecbd11e71f3ca242aba06e00a043d83fa3a4`; ele não identifica as alterações ainda não commitadas usadas nesta revalidação. Todos os números do manifesto são resultados desta execução local em dados públicos históricos.

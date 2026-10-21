# Pacote de evidências da demonstração

O pacote exporta gráficos e um manifesto de uma execução **local** do Mode 1. Os dados são públicos e históricos. O material não descreve uma entrega a cliente nem um serviço em produção.

## Gerar e validar

Prepare Python 3.11 e execute `make setup PYTHON=python3.11` em um ambiente novo. Depois:

```bash
make run-all
make dbt-test
make dq
make portfolio-export
make showcase-evidence
make showcase
```

`make portfolio` executa setup, notebooks, dbt test, DQ e exportação numa sequência. O último passo, `make showcase-evidence`, seleciona e valida os arquivos que serão apresentados no showcase.

Cada exportação vai para `docs/portfolio/exports/<run_id>/`, com gráficos, relatório e `manifests/run.json`. `latest.txt` aponta para a exportação mais recente. `make portfolio-validate` verifica rapidamente a geração de exportações a partir de saídas locais já materializadas e não altera esse ponteiro.

O bundle de vitrine fica em `showcase/static/evidence/`: duas imagens, hashes e um manifesto compacto. Ele é versionável para que a página mostre um registro identificado mesmo sem os artefatos de runtime, que são ignorados pelo Git.

## Leitura correta dos números

- As contagens Bronze, Silver e Gold usam granularidades diferentes.
- Os testes e regras se referem à execução indicada no manifesto.
- O gráfico de filmes mostra uma série histórica do dataset público.
- O gráfico de tempo de etapa do pacote completo usa um limite local de referência de 20 segundos; não demonstra SLA de produção.
- O gráfico `throughput_trend_daily.png` atribui um dia artificial a cada filme para fins de visualização; não mede vazão diária do pipeline. Ambos ficam fora do showcase institucional.

Veja o [resumo público](DEMO_OVERVIEW.md), o [guia de publicação](WEBSITE_EMBED.md), o [registro da validação de 2026-10-20](VALIDACAO_LOCAL_2026-10-20.md) e o [detalhamento técnico](TECHNICAL_OVERVIEW.md). Os arquivos em `exports/sample/` são uma amostra antiga para teste da exportação, sem ligação com a execução escolhida para a vitrine.

# Demonstração técnica de engenharia de dados

Este repositório demonstra um fluxo local de dados em camadas: ingestão de CSVs públicos, armazenamento Bronze e Silver em Parquet, modelos Gold com dbt e DuckDB, testes dbt e verificações Great Expectations. O objetivo é mostrar a implementação e a rastreabilidade do processamento.

Os dados de entrada são os conjuntos históricos `recent_grads` e `bechdel_movies` da [FiveThirtyEight](https://github.com/fivethirtyeight/data), disponibilizados, salvo indicação específica, sob [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Os gráficos foram produzidos e traduzidos para esta demonstração. Nenhum dado representa operação industrial, cliente da PhaifferTech ou ambiente de produção.

## O que pode ser apresentado

- O percurso de Raw a Gold e os arquivos implementados em cada etapa.
- Um registro de execução local com data, commit base, datasets, contagens por camada e resultado dos testes: [manifesto da demonstração](../../showcase/static/evidence/manifest.json).
- Dois gráficos extraídos da mesma execução: [proporção de filmes por ano](../../showcase/static/evidence/bechdel_pass_rate_by_year.png) e [verificações de qualidade](../../showcase/static/evidence/quality_checks.png).
- Código e instruções para reproduzir o fluxo no [README principal](../../README.md).

As contagens de Bronze, Silver e Gold têm granularidades diferentes e não formam uma taxa de conversão. Os resultados de qualidade valem apenas para os dados e a execução registrados. O gráfico de filmes usa um conjunto histórico e não expressa tendência atual.

## Texto curto para a vitrine

> **Demonstração técnica de plataforma de dados.** Pipeline local com dados públicos, camadas Bronze, Silver e Gold, transformações com dbt e DuckDB, verificações de qualidade e visualização dos resultados. Projeto demonstrativo, sem operação em produção ou entrega a cliente.

## Links recomendados

1. [Captura da visão geral do showcase](../assets/showcase-overview.png).
2. [Manifesto da execução](../../showcase/static/evidence/manifest.json).
3. [Repositório e roteiro de reprodução](../../README.md).

O servidor Flask em `showcase/` é destinado à visualização local. Para o site institucional, publique conteúdo e imagens estáticas após revisar o registro de execução escolhido.

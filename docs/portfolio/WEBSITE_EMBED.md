# Guia para a vitrine institucional

Use a categoria **Demonstração técnica**. O [texto institucional curto](DEMO_OVERVIEW.md#texto-curto-para-a-vitrine) pode acompanhar uma captura da visão geral, um gráfico e links para o repositório e o manifesto.

## Imagens selecionadas

| Arquivo | Legenda factual |
| --- | --- |
| [`showcase/static/evidence/bechdel_pass_rate_by_year.png`](../../showcase/static/evidence/bechdel_pass_rate_by_year.png) | Proporção por ano de lançamento nos filmes do dataset público FiveThirtyEight; análise de dados históricos. |
| [`showcase/static/evidence/quality_checks.png`](../../showcase/static/evidence/quality_checks.png) | Quantidade de regras Great Expectations verificadas e violações observadas na execução local registrada. |
| [`docs/assets/showcase-overview.png`](../assets/showcase-overview.png) | Interface local do projeto com arquitetura e resultados da execução demonstrativa. |
| [`docs/assets/showcase-quality.png`](../assets/showcase-quality.png) | Página local que separa os resultados dos testes dbt e das verificações Great Expectations. |

O [manifesto publicado](../../showcase/static/evidence/manifest.json) contém data UTC, commit base, datasets, contagens, testes e hashes SHA-256 das duas imagens de dados. Identifique esses números como **resultado desta execução local**. Informe que os dados pertencem à FiveThirtyEight, sob [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), e que os gráficos foram elaborados e traduzidos neste projeto.

## Materiais que não devem aparecer como prova operacional

- `throughput_trend_daily.png`: o dia do ano é derivado por hash do identificador do filme; não representa o dia em que o pipeline processou registros.
- `freshness_latency_sla_compliance.png`: compara tempos de uma execução local a um limite de referência de 20 segundos; não demonstra SLA contratual, disponibilidade ou frescor dos dados.
- Arquivos de `exports/sample/`: são uma amostra de março de 2026, útil para testar a exportação, sem representar a execução selecionada para esta vitrine.
- Diagramas e telas do Mode 2: descrevem infraestrutura opcional e não fazem parte da execução validada neste conjunto de evidências.

## Atualização das evidências

Depois de preparar Python 3.11 e executar `make setup`, rode, nesta ordem:

```bash
make run-all
make dbt-test
make dq
make portfolio-export
make showcase-evidence
make showcase
```

Confira o novo manifesto e as páginas antes de substituir as capturas publicadas. `make showcase-evidence` recusa publicar se não encontrar testes dbt aprovados, verificações Great Expectations ou uma sequência temporal coerente entre pipeline, teste e exportação. A pasta `showcase/static/evidence/` contém o recorte estático que pode ser revisado antes de copiar arquivos para o site.

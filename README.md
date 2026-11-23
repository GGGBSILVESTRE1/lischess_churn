# lischess-churn

Análise de churn (jogadores que deixam de jogar) com partidas do Lichess, lidas do
espelho oficial em Parquet no Hugging Face (`Lichess/standard-chess-games`).

## Como rodar a ingestão

1. Crie um token **Read** em Hugging Face → Settings → Access Tokens.
2. Crie um `.env` na raiz (já está no `.gitignore`, nunca versione):
   ```
   HF_TOKEN=hf_xxxxxxxx
   ```
3. Rode da raiz do projeto:
   ```
   uv run --env-file .env lischess-churn 2024-01            # um mês
   uv run --env-file .env lischess-churn 2024-01 2024-03    # intervalo inclusivo
   ```

A saída fica em `data/raw/AAAA-MM/train-XXXXX-of-YYYYY.parquet` (um arquivo por arquivo
remoto). Se a execução cair, rode o mesmo comando: os arquivos prontos são pulados.
No notebook: `duckdb.sql("SELECT * FROM 'data/raw/*/*.parquet'")`.

## Decisões de projeto

### Amostragem por hash do username (e não por partida)
Para medir churn é preciso o histórico **completo** de cada jogador. Amostrar partidas
aleatórias deixaria históricos furados. Em vez disso, `md5_number(lower(username)) % 100 < sample_pct`
coloca cada jogador sempre no mesmo "balde" 0-99, em qualquer mês e execução: quem está na
amostra em janeiro continua em fevereiro, e dá para ver quando ele **para de aparecer**.
A partida é mantida se *qualquer* um dos lados (White ou Black) for da amostra; os
adversários fora da amostra aparecem de forma parcial, então as análises devem focar
nos jogadores amostrados.

### Sem a coluna `movetext`
Os lances são a maior parte do volume e não são necessários para churn. Como o Parquet é
colunar, basta não listar a coluna no `SELECT`: ela nunca é baixada. Não existe etapa de
"limpeza" posterior, porque não há nada a remover.

### Leitura remota direta com DuckDB
O DuckDB lê `hf://` com projeção de colunas, evitando baixar os arquivos inteiros
(um mês tem 421 arquivos, 71,7 GB com `movetext`).

### Rate limit do Hugging Face (HTTP 429) e ingestão por arquivo
A primeira versão fazia **um COPY por mês** e falhou com 429. Medições:

| Medida | Valor |
|---|---|
| Arquivos por mês | 421 (~170 MB cada) |
| Requisições HTTP por arquivo | ~241 (≈ 1 por row group; o arquivo tem 236) |
| Requisições por mês | ~101 mil |
| Limite sem token | 3000 / 5 min |
| Limite com token | 5000 / 5 min |

Mesmo com token, o ritmo natural do DuckDB estoura o limite em ~2 min (19 arquivos), e as
retentativas internas também consomem cota. Como o COPY era único por mês, um 429 no
arquivo 300 descartava os 299 anteriores.

Alternativas consideradas:

- **A. Um arquivo por vez, com checkpoint e ritmo controlado (escolhida).**
  Cada arquivo é gravado separadamente (`.tmp` + rename) e pulado se já existe. O script
  espera `min_seconds_per_file` (18 s ≈ 241 req × 300 s / 4000, com 20% de folga) entre
  arquivos e, ao receber 429, dorme o `Retry-After` indicado e repete aquele arquivo.
  Custo: ~2 a 2,5 h por mês. Baixa só as colunas de metadados.
- **B. Baixar os arquivos inteiros e filtrar localmente.** Seria 1 requisição por
  arquivo, mas 71,7 GB por mês (~860 GB/ano), incluindo o `movetext` inútil. Descartada
  por volume de banda e disco.
- **C. Reduzir o escopo.** Complementar à A: validar o modelo com 3 a 6 meses antes de
  gastar ~1 dia de máquina por ano.

As retentativas internas do DuckDB foram reduzidas (`http_retries = 3`) porque o 429 agora
é tratado de forma explícita e cada retry consome cota.

### Observação sobre churn
Um ano é pouco para definir churn com folga: são necessárias uma janela de observação e
uma de "sumiço", e os últimos meses ficam sem rótulo confiável (censura à direita).

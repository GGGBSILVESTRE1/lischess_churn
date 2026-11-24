-- Tabela longa: uma linha por jogador da amostra e partida.
-- Cada partida vira duas linhas (perspectiva das brancas e das negras) e depois
-- ficam só as linhas cujo `player` pertence à amostra da ingestão.
--
-- Rodar a partir da raiz do projeto (o caminho dos Parquet é relativo):
--   duckdb churn.duckdb < sql/01_jogadores.sql
-- Entrada: data/raw/games_AAAA-MM.parquet (um arquivo por mês, gerado pela ingestão).

CREATE OR REPLACE VIEW games AS
SELECT * FROM read_parquet('data/raw/games_*.parquet');

CREATE OR REPLACE TABLE player_games AS
WITH long AS (
    -- Perspectiva do jogador de brancas
    SELECT
        Site                                         AS game_id,      -- URL da partida, única por partida
        CAST(UTCDate AS DATE) + CAST(UTCTime AS TIME) AS played_at,   -- início da partida em UTC
        Event                                        AS event,        -- texto original, ex.: 'Rated Blitz tournament https://...'
        TimeControl                                  AS time_control,
        Termination                                  AS termination,
        Result                                       AS result,
        lower(White)                                 AS player,       -- usernames não diferenciam maiúsculas
        lower(Black)                                 AS opponent,
        WhiteElo                                     AS rating,
        BlackElo                                     AS opponent_rating,
        WhiteRatingDiff                              AS rating_diff,
        WhiteTitle                                   AS title,
        BlackTitle                                   AS opponent_title,
        'white'                                      AS color,
        CASE Result WHEN '1-0'     THEN 'win'
                    WHEN '0-1'     THEN 'loss'
                    WHEN '1/2-1/2' THEN 'draw'
                    WHEN '*'       THEN 'unfinished'
        END                                          AS outcome
    FROM games

    UNION ALL

    -- Perspectiva do jogador de negras (mesma ordem de colunas)
    SELECT
        Site,
        CAST(UTCDate AS DATE) + CAST(UTCTime AS TIME),
        Event, TimeControl, Termination, Result,
        lower(Black), lower(White),
        BlackElo, WhiteElo, BlackRatingDiff,
        BlackTitle, WhiteTitle,
        'black',
        CASE Result WHEN '0-1'     THEN 'win'
                    WHEN '1-0'     THEN 'loss'
                    WHEN '1/2-1/2' THEN 'draw'
                    WHEN '*'       THEN 'unfinished'
        END
    FROM games
),

enriched AS (
    SELECT
        *,
        -- Ritmo pela regra do Lichess: tempo estimado = base + 40 * incremento
        TRY_CAST(split_part(time_control, '+', 1) AS INTEGER)
          + 40 * TRY_CAST(split_part(time_control, '+', 2) AS INTEGER) AS est_seconds,
        -- Tipo de evento a partir do texto de Event
        starts_with(lower(event), 'rated')                             AS is_rated,
        CASE
            WHEN lower(event) LIKE '%tournament%' THEN 'arena'
            WHEN lower(event) LIKE '%swiss%'      THEN 'swiss'
            -- NULL = partida avulsa (pool, desafio, lobby)
        END                                                            AS competition
    FROM long
)

SELECT
    game_id,
    played_at,
    event,
    is_rated,
    competition,
    competition IS NOT NULL                          AS is_tournament,
    -- Resumo em uma coluna: torneio tem precedência sobre rated/casual
    CASE
        WHEN competition IS NOT NULL THEN 'tournament'
        WHEN is_rated                     THEN 'rated'
        ELSE 'casual'
    END                                              AS event_type,
    time_control,
    est_seconds,
    CASE
        WHEN est_seconds IS NULL THEN NULL           -- correspondência ('-')
        WHEN est_seconds < 30    THEN 'ultrabullet'
        WHEN est_seconds < 180   THEN 'bullet'
        WHEN est_seconds < 480   THEN 'blitz'
        WHEN est_seconds < 1500  THEN 'rapid'
        ELSE 'classical'
    END                                              AS speed,
    termination,
    result,
    outcome,
    -- '*' = partida sem resultado (abortada/abandonada antes de terminar).
    -- Mantida porque ainda é sinal de atividade; filtre por is_finished nas métricas de desempenho.
    outcome <> 'unfinished'                          AS is_finished,
    player,
    opponent,
    rating,
    opponent_rating,
    rating_diff,
    title,
    opponent_title,
    color,
    coalesce(opponent_title, '') = 'BOT'             AS vs_bot  -- humano contra bot: mantém, mas sinaliza
FROM enriched
-- Mesma regra de amostragem da ingestão (manter o "< 2" igual ao sample_pct de IngestConfig)
WHERE md5_number(player) % 100 < 2
  -- Contas de bot não são clientes
  AND coalesce(title, '') <> 'BOT';

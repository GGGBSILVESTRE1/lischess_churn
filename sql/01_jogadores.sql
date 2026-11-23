
-- uma linha por jogador e partida 




CREATE OR REPLACE VIEW jogadores AS
SELECT * FROM read_parquet('data/raw/*.parquet');

CREATE OR REPLACE TABLE palyer_games AS
WITH long as (
    -- Perspectiva do jogador de brancas 
    SELECT 
    
        Site                        AS game_id,      -- indentificador único da partida por site
        UTCDate + UTCTime           AS played_at,    -- tempo de jogo em UTC
        TimeControl                 AS time_control,
        Termination                 AS termination,
        lower(White)                AS player,       -- usernames não diferenciam maiúsculas
        lower(Black)                AS opponent,
        WhiteElo                    AS rating,
        BlackElo                    AS opponent_rating,
        WhiteRatingDiff             AS rating_diff,
        WhiteTitle                  AS title,
        BlackTitle                  AS opponent_title,
        'white'                     AS color,
        CASE Result WHEN '1-0' THEN 'win' WHEN '0-1' THEN 'loss'
                    WHEN '1/2-1/2' THEN 'draw' END AS outcome
     FROM games 

    UNION ALL

    -- perspectiva do jogador de negras
    SELECT
        Site, UTCDate + UTCTime, TimeControl, Termination,
        lower(Black), lower(White),
        BlackElo, WhiteElo, BlackRatingDiff,
        BlackTitle, WhiteTitle,
        'black',
        CASE Result WHEN '0-1' THEN 'win' WHEN '1-0' THEN 'loss'
                    WHEN '1/2-1/2' THEN 'draw' END
    FROM games 
)

SELECT
    *,
    -- Ritmo de jogo pela regra do Lichess: tempo estimado = base + 40 * incremento
    CASE
        WHEN est_seconds IS NULL THEN NULL          -- correspondência ('-')
        WHEN est_seconds < 30   THEN 'ultrabullet'
        WHEN est_seconds < 180  THEN 'bullet'
        WHEN est_seconds < 480  THEN 'blitz'
        WHEN est_seconds < 1500 THEN 'rapid'
        ELSE 'classical'
    END AS speed,
    coalesce(opponent_title, '') = 'BOT' AS vs_bot  -- humano contra bot: mantém, mas sinaliza
FROM (
    SELECT
        *,
        TRY_CAST(split_part(time_control, '+', 1) AS INTEGER)
          + 40 * TRY_CAST(split_part(time_control, '+', 2) AS INTEGER) AS est_seconds
    FROM long
)
-- Mesma regra de amostragem da ingestão (manter o "< 2" igual ao sample_pct usado lá)
WHERE md5_number(player) % 100 < 2
  -- Contas de bot não são clientes
  AND coalesce(title, '') <> 'BOT';


"""
Ingestão de partidas do Lichess a partir do espelho oficial em Parquet no Hugging Face.

Fluxo por mês (cada mês é dividido em ~420 arquivos Parquet remotos):
    1. Lista os arquivos do mês.
    2. Para CADA arquivo: lê só as colunas de metadados (o `movetext` nunca é lido),
       mantém só partidas de jogadores da amostra (hash do username) e grava
       data/raw/AAAA-MM/train-XXXXX-of-YYYYY.parquet.
    3. Arquivos já gravados são pulados, então uma execução interrompida é retomada
       de onde parou.
    4. O ritmo é controlado para não estourar o limite de requisições do Hugging Face.

Uso (a partir da raiz do projeto, pois output_dir é relativo):
    uv run --env-file .env lischess-churn 2024-01            # um mês
    uv run --env-file .env lischess-churn 2024-01 2024-03    # intervalo inclusivo
"""

from __future__ import annotations

import logging
import os
import re
import sys
import time
import shutil
from dataclasses import dataclass
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestConfig:
    # Caminho do dataset no Hugging Face; partições no formato year=AAAA/month=MM
    hf_base: str = "hf://datasets/Lichess/standard-chess-games/data"
    output_dir: Path = Path("data/raw")
    # Porcentagem de jogadores na amostra (0-100). Comece pequeno e meça.
    sample_pct: int = 2
    # Limite do HF: 5000 requisições / 5 min com token. Medido: ~241 requisições por arquivo.
    # Mínimo de segundos por arquivo = 300 * 241 / 4000 ≈ 18 s (folga de 20% sobre o limite).
    min_seconds_per_file: float = 18.0
    # Quantas vezes repetir um arquivo que levou 429 antes de desistir dele.
    max_rate_limit_retries: int = 5
    # Colunas mantidas: tudo que descreve a partida, exceto os lances.
    columns: tuple[str, ...] = (
        "Event",
        "Site",
        "White",
        "Black",
        "Result",
        "WhiteTitle",
        "BlackTitle",
        "WhiteElo",
        "BlackElo",
        "WhiteRatingDiff",
        "BlackRatingDiff",
        "UTCDate",
        "UTCTime",
        "Termination",
        "TimeControl",
    )


# --------------------------------------------------------------------------- #
# SQL
# --------------------------------------------------------------------------- #
# Amostragem determinística: o mesmo username cai sempre no mesmo "balde" (0-99),
# em qualquer mês e em qualquer execução. Isso garante que um jogador da amostra
# continue na amostra nos meses seguintes — condição necessária para medir churn.
# lower(): usernames do Lichess não diferenciam maiúsculas/minúsculas.
IN_SAMPLE = "md5_number(lower({col})) % 100 < {pct}"

INGEST_SQL = """
COPY (
    SELECT {columns}
    FROM read_parquet('{source}')
    WHERE ({white_in_sample}) OR ({black_in_sample})
) TO '{target}' (FORMAT parquet, COMPRESSION zstd)
"""


# --------------------------------------------------------------------------- #
# Funções
# --------------------------------------------------------------------------- #
def connect() -> duckdb.DuckDBPyConnection:
    """Abre uma conexão DuckDB em memória com acesso a arquivos remotos."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    # Sem autenticação o Hugging Face limita a 3000 requisições / 5 min por IP (HTTP 429).
    # Com HF_TOKEN no ambiente o limite sobe para 5000.
    if os.environ.get("HF_TOKEN"):
        con.execute("CREATE SECRET hf (TYPE huggingface, PROVIDER credential_chain);")
    else:
        log.warning("HF_TOKEN não definido: sujeito a rate limit (429) do Hugging Face")
    # Poucas retentativas internas: cada retry também consome a cota. O 429 é tratado
    # em ingest_file (espera o Retry-After), não aqui.
    con.execute(
        "SET http_retries = 3; SET http_retry_wait_ms = 1000; SET http_retry_backoff = 2;"
    )
    return con


def list_month_files(
    con: duckdb.DuckDBPyConnection, cfg: IngestConfig, month: str
) -> list[str]:
    """'2024-01' -> URLs ordenadas dos arquivos Parquet daquele mês."""
    year, mm = month.split("-")
    pattern = f"{cfg.hf_base}/year={year}/month={mm}/*.parquet"
    rows = con.execute("SELECT file FROM glob(?) ORDER BY file", [pattern]).fetchall()
    return [r[0] for r in rows]


def retry_after_seconds(exc: Exception, default: int = 180) -> int | None:
    """Se o erro for um 429, devolve quantos segundos esperar; senão None."""
    text = str(exc)
    if "429" not in text:
        return None
    match = re.search(r'Retry-After":"(\d+)"', text)
    return int(match.group(1)) + 5 if match else default


def ingest_file(
    con: duckdb.DuckDBPyConnection, cfg: IngestConfig, month: str, source: str
) -> bool:
    """Filtra um arquivo remoto. Idempotente. Devolve True se baixou, False se pulou."""
    month_dir = cfg.output_dir / month
    target = month_dir / source.rsplit("/", 1)[-1]
    if target.exists():
        return False

    month_dir.mkdir(parents=True, exist_ok=True)
    # Grava em arquivo temporário e renomeia no fim: se a execução cair no meio,
    # não sobra um Parquet pela metade que seria "pulado" na próxima execução.
    tmp = target.with_suffix(".tmp")
    sql = INGEST_SQL.format(
        columns=", ".join(cfg.columns),
        source=source,
        white_in_sample=IN_SAMPLE.format(col="White", pct=cfg.sample_pct),
        black_in_sample=IN_SAMPLE.format(col="Black", pct=cfg.sample_pct),
        target=tmp.as_posix(),
    )
    for attempt in range(cfg.max_rate_limit_retries + 1):
        try:
            con.execute(sql)
            tmp.replace(target)
            return True
        except duckdb.Error as exc:
            tmp.unlink(missing_ok=True)
            wait = retry_after_seconds(exc)
            if wait is None or attempt == cfg.max_rate_limit_retries:
                raise
            log.warning(
                "429 em %s; aguardando %ss (tentativa %d)",
                target.name,
                wait,
                attempt + 1,
            )
            time.sleep(wait)
    return False


def compacted_path(cfg: IngestConfig, month: str) -> Path:
    """Arquivo único do mês, depois de compactado: data/raw/games_AAAA-MM.parquet."""
    return cfg.output_dir / f"games_{month}.parquet"


def compact_month(
    con: duckdb.DuckDBPyConnection, cfg: IngestConfig, month: str
) -> None:
    """Junta os Parquet do mês em um arquivo e apaga a pasta, só se a contagem de linhas bater."""
    month_dir = cfg.output_dir / month
    target = compacted_path(cfg, month)
    tmp = target.with_suffix(".tmp")
    src = f"{month_dir.as_posix()}/*.parquet"

    con.execute(
        f"COPY (SELECT * FROM read_parquet('{src}')) "
        f"TO '{tmp.as_posix()}' (FORMAT parquet, COMPRESSION zstd)"
    )
    n_src = con.execute(f"SELECT count(*) FROM read_parquet('{src}')").fetchone()[0]
    n_out = con.execute(
        f"SELECT count(*) FROM read_parquet('{tmp.as_posix()}')"
    ).fetchone()[0]
    if n_src != n_out:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(
            f"{month}: contagem divergente ({n_src} vs {n_out}); pasta mantida"
        )

    tmp.replace(target)
    shutil.rmtree(month_dir)
    log.info("%s: compactado em %s (%s partidas)", month, target.name, f"{n_out:,}")


def ingest_month(con: duckdb.DuckDBPyConnection, cfg: IngestConfig, month: str) -> None:
    """Ingere todos os arquivos de um mês, no ritmo permitido pelo Hugging Face."""

    if compacted_path(cfg, month).exists():
        log.info("%s: já compactado, pulando", month)
        return

    sources = list_month_files(con, cfg, month)
    if not sources:
        log.error("%s: nenhum arquivo encontrado (mês ausente no espelho?)", month)
        return

    log.info("%s: %d arquivos", month, len(sources))
    done = failed = 0
    for i, source in enumerate(sources, start=1):
        started = time.monotonic()
        try:
            downloaded = ingest_file(con, cfg, month, source)
        except duckdb.Error as exc:
            failed += 1
            log.error(
                "%s: falha em %s: %s", month, source.rsplit("/", 1)[-1], str(exc)[:300]
            )
            continue
        done += 1
        if not downloaded:
            continue  # arquivo já existia: não gastou cota, não precisa esperar
        elapsed = time.monotonic() - started
        log.info("%s: %d/%d ok (%.0fs)", month, i, len(sources), elapsed)
        if elapsed < cfg.min_seconds_per_file:
            time.sleep(cfg.min_seconds_per_file - elapsed)

    summary = f"{month}: {done}/{len(sources)} arquivos, {failed} falhas"
    if failed:
        log.warning("%s — rode o mesmo comando de novo para completar", summary)
    else:
        compact_month(con, cfg, month)


def month_range(start: str, end: str) -> list[str]:
    """Lista de meses 'AAAA-MM' de start até end, inclusive."""
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    months = []
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) not in (1, 2):
        sys.exit("uso: lischess-churn AAAA-MM [AAAA-MM]")
    cfg = IngestConfig()
    con = connect()
    for month in month_range(argv[0], argv[-1]):
        try:
            ingest_month(con, cfg, month)
        except duckdb.Error as exc:
            # Falha ao listar o mês (rede, mês ausente): registra e segue.
            log.error("Falha em %s: %s", month, exc)


if __name__ == "__main__":
    main()

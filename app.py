from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template, request

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional dependency in local sqlite mode
    psycopg = None
    dict_row = None

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "crm.db"
POSTGRES_URL = os.getenv("POSTGRES_URL")
DB_BACKEND = "postgres" if POSTGRES_URL else "sqlite"

DB_INTEGRITY_ERRORS: tuple[type[BaseException], ...] = (sqlite3.IntegrityError,)
if psycopg is not None:
    DB_INTEGRITY_ERRORS = (sqlite3.IntegrityError, psycopg.IntegrityError)

app = Flask(__name__)

MASTER_CONFIG: dict[str, dict[str, Any]] = {
    "clientes": {
        "table": "clientes",
        "pk": "cliente_id",
        "fields": ["nome"],
        "list_fields": ["nome", "status_atual"],
        "required": ["nome"],
    },
    "pessoa_cliente": {
        "table": "pessoa_cliente",
        "pk": "pessoa_cliente_id",
        "fields": ["cliente_id", "nome"],
        "list_fields": ["cliente_id", "cliente_nome", "nome"],
        "required": ["cliente_id", "nome"],
        "display_name": "pessoa_cliente",
    },
    "status_cliente": {
        "table": "status_cliente",
        "pk": "status_cliente_id",
        "fields": ["nome"],
        "required": ["nome"],
    },
    "segmentos": {
        "table": "segmentos",
        "pk": "segmento_id",
        "fields": ["nome"],
        "required": ["nome"],
    },
    "canais": {
        "table": "canais",
        "pk": "canal_id",
        "fields": ["nome"],
        "required": ["nome"],
    },
    "resposaveis_pessoaTema": {
        "table": "responsaveis",
        "pk": "responsavel_id",
        "fields": ["nome"],
        "required": ["nome"],
        "display_name": "resposaveis_pessoaTema",
    },
    "categorias": {
        "table": "categorias",
        "pk": "categoria_id",
        "fields": ["nome"],
        "required": ["nome"],
    },
    "areas_negocio": {
        "table": "areas_negocio",
        "pk": "area_negocio_id",
        "fields": ["nome"],
        "required": ["nome"],
    },
    "estados": {
        "table": "estados",
        "pk": "estado_id",
        "fields": ["uf"],
        "required": ["uf"],
    },
    "decisoes": {
        "table": "decisoes",
        "pk": "decisao_id",
        "fields": ["nome"],
        "required": ["nome"],
    },
    "justificativas": {
        "table": "justificativas",
        "pk": "justificativa_id",
        "fields": ["nome"],
        "required": ["nome"],
    },
    "status_proposta": {
        "table": "status_proposta",
        "pk": "status_proposta_id",
        "fields": ["nome"],
        "required": ["nome"],
    },
}


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


class DbCursor:
    def __init__(self, cursor: Any, lastrowid: int | None = None) -> None:
        self._cursor = cursor
        self.lastrowid = lastrowid

    def fetchone(self) -> Any:
        return self._cursor.fetchone()

    def fetchall(self) -> list[Any]:
        return self._cursor.fetchall()


class DbConnection:
    def __init__(self, conn: Any, backend: str) -> None:
        self._conn = conn
        self._backend = backend

    def _transform_sql(self, sql: str) -> tuple[str, bool]:
        if self._backend != "postgres":
            return sql, False

        text = sql
        used_insert_or_ignore = False
        marker = "INSERT OR IGNORE INTO"
        if marker in text.upper():
            used_insert_or_ignore = True
            upper_text = text.upper()
            idx = upper_text.find(marker)
            if idx >= 0:
                text = text[:idx] + "INSERT INTO" + text[idx + len(marker):]

        text = text.replace("?", "%s")
        if used_insert_or_ignore and "ON CONFLICT" not in text.upper():
            text = text.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
        return text, used_insert_or_ignore

    def execute(self, sql: str, params: tuple[Any, ...] | list[Any] = ()) -> DbCursor:
        text, _ = self._transform_sql(sql)
        cur = self._conn.cursor()
        cur.execute(text, tuple(params))

        lastrowid: int | None = None
        if self._backend == "postgres" and text.lstrip().upper().startswith("INSERT"):
            try:
                aux = self._conn.cursor()
                aux.execute("SELECT LASTVAL()")
                row = aux.fetchone()
                if row is not None:
                    lastrowid = int(row[0])
            except Exception:
                lastrowid = None

        return DbCursor(cur, lastrowid=lastrowid)

    def executescript(self, sql_script: str) -> None:
        if self._backend == "sqlite":
            self._conn.executescript(sql_script)
            return

        statements = [stmt.strip() for stmt in sql_script.split(";") if stmt.strip()]
        for stmt in statements:
            self.execute(stmt)

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "DbConnection":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc is not None:
            self.rollback()
        else:
            self.commit()
        self.close()


def get_connection() -> DbConnection:
    if DB_BACKEND == "postgres":
        if psycopg is None:
            raise RuntimeError("psycopg is required when POSTGRES_URL is configured.")
        conn = psycopg.connect(POSTGRES_URL, row_factory=dict_row)
        return DbConnection(conn, backend="postgres")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return DbConnection(conn, backend="sqlite")


def init_db_postgres(conn: DbConnection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS status_cliente (
            status_cliente_id BIGSERIAL PRIMARY KEY,
            nome TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS clientes (
            cliente_id BIGSERIAL PRIMARY KEY,
            nome TEXT NOT NULL UNIQUE,
            status_cliente_id BIGINT REFERENCES status_cliente (status_cliente_id)
        );

        CREATE TABLE IF NOT EXISTS pessoa_cliente (
            pessoa_cliente_id BIGSERIAL PRIMARY KEY,
            cliente_id BIGINT NOT NULL REFERENCES clientes (cliente_id),
            nome TEXT NOT NULL,
            UNIQUE (cliente_id, nome)
        );

        CREATE TABLE IF NOT EXISTS segmentos (
            segmento_id BIGSERIAL PRIMARY KEY,
            nome TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS canais (
            canal_id BIGSERIAL PRIMARY KEY,
            nome TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS responsaveis (
            responsavel_id BIGSERIAL PRIMARY KEY,
            nome TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS categorias (
            categoria_id BIGSERIAL PRIMARY KEY,
            nome TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS areas_negocio (
            area_negocio_id BIGSERIAL PRIMARY KEY,
            nome TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS estados (
            estado_id BIGSERIAL PRIMARY KEY,
            uf TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS decisoes (
            decisao_id BIGSERIAL PRIMARY KEY,
            nome TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS justificativas (
            justificativa_id BIGSERIAL PRIMARY KEY,
            nome TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS status_proposta (
            status_proposta_id BIGSERIAL PRIMARY KEY,
            nome TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS oportunidades_erp (
            oportunidade_id BIGSERIAL PRIMARY KEY,
            id_oportunidade TEXT NOT NULL UNIQUE,
            data_entrada TEXT NOT NULL,
            cliente_id BIGINT NOT NULL REFERENCES clientes (cliente_id),
            pessoa_cliente_id BIGINT REFERENCES pessoa_cliente (pessoa_cliente_id),
            status_cliente_id BIGINT REFERENCES status_cliente (status_cliente_id),
            segmento TEXT,
            segmento_id BIGINT REFERENCES segmentos (segmento_id),
            decisao_id BIGINT REFERENCES decisoes (decisao_id),
            justificativa_id BIGINT REFERENCES justificativas (justificativa_id),
            canal_id BIGINT NOT NULL REFERENCES canais (canal_id),
            responsavel_1_id BIGINT NOT NULL REFERENCES responsaveis (responsavel_id),
            codigo_proposta TEXT,
            objeto TEXT,
            categoria_id BIGINT NOT NULL REFERENCES categorias (categoria_id),
            area_negocio_id BIGINT REFERENCES areas_negocio (area_negocio_id),
            estado_id BIGINT NOT NULL REFERENCES estados (estado_id),
            valor DOUBLE PRECISION,
            prazo_execucao_meses INTEGER,
            data_envio TEXT,
            status_proposta_id BIGINT REFERENCES status_proposta (status_proposta_id),
            responsavel_2_id BIGINT REFERENCES responsaveis (responsavel_id),
            ultimo_contato_data TEXT,
            observacoes_acompanhamento TEXT,
            versao_atual INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            tipo_evento TEXT,
            data_evento TEXT,
            valor_evento DOUBLE PRECISION,
            observacao_proposta TEXT,
            nome_contato TEXT,
            proxima_acao TEXT,
            farol TEXT,
            evento_encerrador TEXT
        );

        CREATE TABLE IF NOT EXISTS oportunidades_erp_historico (
            historico_id BIGSERIAL PRIMARY KEY,
            oportunidade_id_ref BIGINT NOT NULL REFERENCES oportunidades_erp (oportunidade_id),
            versao INTEGER NOT NULL,
            snapshot_at TEXT NOT NULL,
            id_oportunidade TEXT NOT NULL,
            codigo_proposta TEXT,
            data_entrada TEXT NOT NULL,
            data_envio TEXT,
            cliente_id BIGINT NOT NULL REFERENCES clientes (cliente_id),
            pessoa_cliente_id BIGINT,
            status_cliente_id BIGINT,
            segmento_id BIGINT,
            decisao_id BIGINT,
            justificativa_id BIGINT,
            canal_id BIGINT NOT NULL,
            responsavel_1_id BIGINT NOT NULL,
            responsavel_2_id BIGINT,
            categoria_id BIGINT NOT NULL,
            area_negocio_id BIGINT,
            estado_id BIGINT NOT NULL,
            status_proposta_id BIGINT,
            valor DOUBLE PRECISION,
            prazo_execucao_meses INTEGER,
            ultimo_contato_data TEXT,
            objeto TEXT,
            observacoes_acompanhamento TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            tipo_evento TEXT,
            data_evento TEXT,
            valor_evento DOUBLE PRECISION,
            observacao_proposta TEXT,
            nome_contato TEXT,
            proxima_acao TEXT,
            farol TEXT,
            evento_encerrador TEXT,
            origem_historico TEXT,
            status_datas_json TEXT
        );

        CREATE TABLE IF NOT EXISTS proposta_status_datas (
            proposta_status_data_id BIGSERIAL PRIMARY KEY,
            oportunidade_id_ref BIGINT NOT NULL REFERENCES oportunidades_erp (oportunidade_id),
            status_proposta_id BIGINT NOT NULL REFERENCES status_proposta (status_proposta_id),
            data_status TEXT NOT NULL,
            UNIQUE (oportunidade_id_ref, status_proposta_id)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS ux_oportunidades_codigo_proposta
        ON oportunidades_erp (codigo_proposta)
        WHERE codigo_proposta IS NOT NULL;

        CREATE INDEX IF NOT EXISTS idx_proposta_status_datas_opp
        ON proposta_status_datas (oportunidade_id_ref);
        """
    )

    conn.execute("INSERT OR IGNORE INTO status_cliente (nome) VALUES ('Novo')")
    conn.execute("INSERT OR IGNORE INTO status_cliente (nome) VALUES ('Ativo')")


def init_db() -> None:
    if DB_BACKEND == "postgres":
        with get_connection() as conn:
            init_db_postgres(conn)
        return

    ensure_dirs()
    with get_connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS clientes (
                cliente_id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS status_cliente (
                status_cliente_id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS pessoa_cliente (
                pessoa_cliente_id INTEGER PRIMARY KEY AUTOINCREMENT,
                cliente_id INTEGER NOT NULL,
                nome TEXT NOT NULL,
                UNIQUE (cliente_id, nome),
                FOREIGN KEY (cliente_id) REFERENCES clientes (cliente_id)
            );

            CREATE TABLE IF NOT EXISTS segmentos (
                segmento_id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS canais (
                canal_id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS responsaveis (
                responsavel_id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS categorias (
                categoria_id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT NOT NULL,
                UNIQUE (nome)
            );

            CREATE TABLE IF NOT EXISTS areas_negocio (
                area_negocio_id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS estados (
                estado_id INTEGER PRIMARY KEY AUTOINCREMENT,
                uf TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS decisoes (
                decisao_id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS justificativas (
                justificativa_id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS status_proposta (
                status_proposta_id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS oportunidades_erp (
                oportunidade_id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_oportunidade INTEGER NOT NULL UNIQUE,
                data_entrada TEXT NOT NULL,
                cliente_id INTEGER NOT NULL,
                pessoa_cliente_id INTEGER,
                status_cliente_id INTEGER,
                segmento TEXT,
                segmento_id INTEGER,
                decisao_id INTEGER,
                justificativa_id INTEGER,
                canal_id INTEGER NOT NULL,
                responsavel_1_id INTEGER NOT NULL,
                codigo_proposta TEXT,
                objeto TEXT,
                categoria_id INTEGER NOT NULL,
                area_negocio_id INTEGER,
                estado_id INTEGER NOT NULL,
                valor REAL,
                prazo_execucao_meses INTEGER,
                data_envio TEXT,
                status_proposta_id INTEGER,
                responsavel_2_id INTEGER,
                ultimo_contato_data TEXT,
                observacoes_acompanhamento TEXT,
                versao_atual INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (cliente_id) REFERENCES clientes (cliente_id),
                FOREIGN KEY (pessoa_cliente_id) REFERENCES pessoa_cliente (pessoa_cliente_id),
                FOREIGN KEY (status_cliente_id) REFERENCES status_cliente (status_cliente_id),
                FOREIGN KEY (segmento_id) REFERENCES segmentos (segmento_id),
                FOREIGN KEY (decisao_id) REFERENCES decisoes (decisao_id),
                FOREIGN KEY (justificativa_id) REFERENCES justificativas (justificativa_id),
                FOREIGN KEY (canal_id) REFERENCES canais (canal_id),
                FOREIGN KEY (responsavel_1_id) REFERENCES responsaveis (responsavel_id),
                FOREIGN KEY (categoria_id) REFERENCES categorias (categoria_id),
                FOREIGN KEY (estado_id) REFERENCES estados (estado_id),
                FOREIGN KEY (status_proposta_id) REFERENCES status_proposta (status_proposta_id),
                FOREIGN KEY (responsavel_2_id) REFERENCES responsaveis (responsavel_id)
            );

            CREATE TABLE IF NOT EXISTS oportunidades_erp_historico (
                historico_id INTEGER PRIMARY KEY AUTOINCREMENT,
                oportunidade_id_ref INTEGER NOT NULL,
                versao INTEGER NOT NULL,
                snapshot_at TEXT NOT NULL,
                id_oportunidade TEXT NOT NULL,
                codigo_proposta TEXT,
                data_entrada TEXT NOT NULL,
                data_envio TEXT,
                cliente_id INTEGER NOT NULL,
                pessoa_cliente_id INTEGER,
                status_cliente_id INTEGER,
                segmento_id INTEGER,
                decisao_id INTEGER,
                justificativa_id INTEGER,
                canal_id INTEGER NOT NULL,
                responsavel_1_id INTEGER NOT NULL,
                responsavel_2_id INTEGER,
                categoria_id INTEGER NOT NULL,
                area_negocio_id INTEGER,
                estado_id INTEGER NOT NULL,
                status_proposta_id INTEGER,
                valor REAL,
                prazo_execucao_meses INTEGER,
                ultimo_contato_data TEXT,
                objeto TEXT,
                observacoes_acompanhamento TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (oportunidade_id_ref) REFERENCES oportunidades_erp (oportunidade_id)
            );

            CREATE TABLE IF NOT EXISTS proposta_status_datas (
                proposta_status_data_id INTEGER PRIMARY KEY AUTOINCREMENT,
                oportunidade_id_ref INTEGER NOT NULL,
                status_proposta_id INTEGER NOT NULL,
                data_status TEXT NOT NULL,
                UNIQUE (oportunidade_id_ref, status_proposta_id),
                FOREIGN KEY (oportunidade_id_ref) REFERENCES oportunidades_erp (oportunidade_id),
                FOREIGN KEY (status_proposta_id) REFERENCES status_proposta (status_proposta_id)
            );

            CREATE UNIQUE INDEX IF NOT EXISTS ux_oportunidades_codigo_proposta
            ON oportunidades_erp (codigo_proposta)
            WHERE codigo_proposta IS NOT NULL;

            CREATE INDEX IF NOT EXISTS idx_proposta_status_datas_opp
            ON proposta_status_datas (oportunidade_id_ref);
            """
        )

        # Lightweight migration for existing databases.
        cliente_cols = [row[1] for row in conn.execute("PRAGMA table_info(clientes)").fetchall()]
        if "status_cliente_id" not in cliente_cols:
            conn.execute("ALTER TABLE clientes ADD COLUMN status_cliente_id INTEGER")

        opp_cols = [row[1] for row in conn.execute("PRAGMA table_info(oportunidades_erp)").fetchall()]
        if "area_negocio_id" not in opp_cols:
            conn.execute("ALTER TABLE oportunidades_erp ADD COLUMN area_negocio_id INTEGER")
        if "segmento_id" not in opp_cols:
            conn.execute("ALTER TABLE oportunidades_erp ADD COLUMN segmento_id INTEGER")
        if "pessoa_cliente_id" not in opp_cols:
            conn.execute("ALTER TABLE oportunidades_erp ADD COLUMN pessoa_cliente_id INTEGER")
        if "versao_atual" not in opp_cols:
            conn.execute("ALTER TABLE oportunidades_erp ADD COLUMN versao_atual INTEGER NOT NULL DEFAULT 1")
        # Propostas extra fields
        for coldef in [
            ("tipo_evento", "TEXT"),
            ("data_evento", "TEXT"),
            ("valor_evento", "REAL"),
            ("observacao_proposta", "TEXT"),
            ("nome_contato", "TEXT"),
            ("proxima_acao", "TEXT"),
            ("farol", "TEXT"),
            ("evento_encerrador", "TEXT")
        ]:
            if coldef[0] not in opp_cols:
                conn.execute(f"ALTER TABLE oportunidades_erp ADD COLUMN {coldef[0]} {coldef[1]}")

        hist_cols = [row[1] for row in conn.execute("PRAGMA table_info(oportunidades_erp_historico)").fetchall()]
        for coldef in [
            ("tipo_evento", "TEXT"),
            ("data_evento", "TEXT"),
            ("valor_evento", "REAL"),
            ("observacao_proposta", "TEXT"),
            ("nome_contato", "TEXT"),
            ("proxima_acao", "TEXT"),
            ("farol", "TEXT"),
            ("evento_encerrador", "TEXT"),
            ("status_datas_json", "TEXT")
        ]:
            if coldef[0] not in hist_cols:
                conn.execute(f"ALTER TABLE oportunidades_erp_historico ADD COLUMN {coldef[0]} {coldef[1]}")

        hist_cols = [row[1] for row in conn.execute("PRAGMA table_info(oportunidades_erp_historico)").fetchall()]
        if "pessoa_cliente_id" not in hist_cols:
            conn.execute("ALTER TABLE oportunidades_erp_historico ADD COLUMN pessoa_cliente_id INTEGER")
        if "origem_historico" not in hist_cols:
            conn.execute("ALTER TABLE oportunidades_erp_historico ADD COLUMN origem_historico TEXT")

        # Decision now starts as NULL on creation. Rebuild legacy tables if decisao_id is NOT NULL.
        opp_info = conn.execute("PRAGMA table_info(oportunidades_erp)").fetchall()
        opp_decisao_notnull = any(row[1] == "decisao_id" and int(row[3]) == 1 for row in opp_info)
        if opp_decisao_notnull:
            conn.execute("PRAGMA foreign_keys = OFF")
            conn.execute("ALTER TABLE oportunidades_erp RENAME TO oportunidades_erp_old")
            conn.execute(
                """
                CREATE TABLE oportunidades_erp (
                    oportunidade_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    id_oportunidade INTEGER NOT NULL UNIQUE,
                    data_entrada TEXT NOT NULL,
                    cliente_id INTEGER NOT NULL,
                    pessoa_cliente_id INTEGER,
                    status_cliente_id INTEGER,
                    segmento TEXT,
                    segmento_id INTEGER,
                    decisao_id INTEGER,
                    justificativa_id INTEGER,
                    canal_id INTEGER NOT NULL,
                    responsavel_1_id INTEGER NOT NULL,
                    codigo_proposta TEXT,
                    objeto TEXT,
                    categoria_id INTEGER NOT NULL,
                    area_negocio_id INTEGER,
                    estado_id INTEGER NOT NULL,
                    valor REAL,
                    prazo_execucao_meses INTEGER,
                    data_envio TEXT,
                    status_proposta_id INTEGER,
                    responsavel_2_id INTEGER,
                    ultimo_contato_data TEXT,
                    observacoes_acompanhamento TEXT,
                    versao_atual INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    tipo_evento TEXT,
                    data_evento TEXT,
                    valor_evento REAL,
                    observacao_proposta TEXT,
                    nome_contato TEXT,
                    proxima_acao TEXT,
                    farol TEXT,
                    evento_encerrador TEXT,
                    FOREIGN KEY (cliente_id) REFERENCES clientes (cliente_id),
                    FOREIGN KEY (pessoa_cliente_id) REFERENCES pessoa_cliente (pessoa_cliente_id),
                    FOREIGN KEY (status_cliente_id) REFERENCES status_cliente (status_cliente_id),
                    FOREIGN KEY (segmento_id) REFERENCES segmentos (segmento_id),
                    FOREIGN KEY (decisao_id) REFERENCES decisoes (decisao_id),
                    FOREIGN KEY (justificativa_id) REFERENCES justificativas (justificativa_id),
                    FOREIGN KEY (canal_id) REFERENCES canais (canal_id),
                    FOREIGN KEY (responsavel_1_id) REFERENCES responsaveis (responsavel_id),
                    FOREIGN KEY (categoria_id) REFERENCES categorias (categoria_id),
                    FOREIGN KEY (estado_id) REFERENCES estados (estado_id),
                    FOREIGN KEY (status_proposta_id) REFERENCES status_proposta (status_proposta_id),
                    FOREIGN KEY (responsavel_2_id) REFERENCES responsaveis (responsavel_id)
                )
                """
            )
            conn.execute(
                """
                INSERT INTO oportunidades_erp (
                    oportunidade_id, id_oportunidade, data_entrada, cliente_id, pessoa_cliente_id,
                    status_cliente_id, segmento, segmento_id, decisao_id, justificativa_id,
                    canal_id, responsavel_1_id, codigo_proposta, objeto, categoria_id,
                    area_negocio_id, estado_id, valor, prazo_execucao_meses, data_envio,
                    status_proposta_id, responsavel_2_id, ultimo_contato_data,
                    observacoes_acompanhamento, versao_atual, created_at, updated_at,
                    tipo_evento, data_evento, valor_evento, observacao_proposta,
                    nome_contato, proxima_acao, farol, evento_encerrador
                )
                SELECT
                    oportunidade_id, id_oportunidade, data_entrada, cliente_id, pessoa_cliente_id,
                    status_cliente_id, segmento, segmento_id, decisao_id, justificativa_id,
                    canal_id, responsavel_1_id, codigo_proposta, objeto, categoria_id,
                    area_negocio_id, estado_id, valor, prazo_execucao_meses, data_envio,
                    status_proposta_id, responsavel_2_id, ultimo_contato_data,
                    observacoes_acompanhamento, versao_atual, created_at, updated_at,
                    tipo_evento, data_evento, valor_evento, observacao_proposta,
                    nome_contato, proxima_acao, farol, evento_encerrador
                FROM oportunidades_erp_old
                """
            )
            conn.execute("DROP TABLE oportunidades_erp_old")
            conn.execute("PRAGMA foreign_keys = ON")

        hist_info = conn.execute("PRAGMA table_info(oportunidades_erp_historico)").fetchall()
        hist_decisao_notnull = any(row[1] == "decisao_id" and int(row[3]) == 1 for row in hist_info)
        if hist_decisao_notnull:
            conn.execute("PRAGMA foreign_keys = OFF")
            conn.execute("ALTER TABLE oportunidades_erp_historico RENAME TO oportunidades_erp_historico_old")
            conn.execute(
                """
                CREATE TABLE oportunidades_erp_historico (
                    historico_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    oportunidade_id_ref INTEGER NOT NULL,
                    versao INTEGER NOT NULL,
                    snapshot_at TEXT NOT NULL,
                    id_oportunidade TEXT NOT NULL,
                    codigo_proposta TEXT,
                    data_entrada TEXT NOT NULL,
                    data_envio TEXT,
                    cliente_id INTEGER NOT NULL,
                    pessoa_cliente_id INTEGER,
                    status_cliente_id INTEGER,
                    segmento_id INTEGER,
                    decisao_id INTEGER,
                    justificativa_id INTEGER,
                    canal_id INTEGER NOT NULL,
                    responsavel_1_id INTEGER NOT NULL,
                    responsavel_2_id INTEGER,
                    categoria_id INTEGER NOT NULL,
                    area_negocio_id INTEGER,
                    estado_id INTEGER NOT NULL,
                    status_proposta_id INTEGER,
                    valor REAL,
                    tipo_evento TEXT,
                    data_evento TEXT,
                    valor_evento REAL,
                    prazo_execucao_meses INTEGER,
                    ultimo_contato_data TEXT,
                    objeto TEXT,
                    observacao_proposta TEXT,
                    nome_contato TEXT,
                    proxima_acao TEXT,
                    farol TEXT,
                    evento_encerrador TEXT,
                    observacoes_acompanhamento TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (oportunidade_id_ref) REFERENCES oportunidades_erp (oportunidade_id)
                )
                """
            )
            conn.execute(
                """
                INSERT INTO oportunidades_erp_historico (
                    historico_id, oportunidade_id_ref, versao, snapshot_at,
                    id_oportunidade, codigo_proposta, data_entrada, data_envio,
                    cliente_id, pessoa_cliente_id, status_cliente_id, segmento_id,
                    decisao_id, justificativa_id, canal_id, responsavel_1_id, responsavel_2_id,
                    categoria_id, area_negocio_id, estado_id, status_proposta_id,
                    valor, tipo_evento, data_evento, valor_evento,
                    prazo_execucao_meses, ultimo_contato_data, objeto,
                    observacao_proposta, nome_contato, proxima_acao, farol, evento_encerrador,
                    observacoes_acompanhamento, created_at, updated_at
                )
                SELECT
                    historico_id, oportunidade_id_ref, versao, snapshot_at,
                    id_oportunidade, codigo_proposta, data_entrada, data_envio,
                    cliente_id, pessoa_cliente_id, status_cliente_id, segmento_id,
                    decisao_id, justificativa_id, canal_id, responsavel_1_id, responsavel_2_id,
                    categoria_id, area_negocio_id, estado_id, status_proposta_id,
                    valor, tipo_evento, data_evento, valor_evento,
                    prazo_execucao_meses, ultimo_contato_data, objeto,
                    observacao_proposta, nome_contato, proxima_acao, farol, evento_encerrador,
                    observacoes_acompanhamento, created_at, updated_at
                FROM oportunidades_erp_historico_old
                """
            )
            conn.execute("DROP TABLE oportunidades_erp_historico_old")
            conn.execute("PRAGMA foreign_keys = ON")

        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_oportunidades_codigo_proposta
            ON oportunidades_erp (codigo_proposta)
            WHERE codigo_proposta IS NOT NULL
            """
        )

        # Base statuses used by workflow.
        conn.execute("INSERT OR IGNORE INTO status_cliente (nome) VALUES ('Novo')")
        conn.execute("INSERT OR IGNORE INTO status_cliente (nome) VALUES ('Ativo')")
        conn.commit()


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if text == "":
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text == "":
        return None
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def parse_iso_date(value: Any) -> str | None:
    text = normalize_text(value)
    if text is None:
        return None
    try:
        dt = datetime.strptime(text, "%Y-%m-%d")
        return dt.date().isoformat()
    except ValueError:
        return None


def format_version_label(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None

    if text.upper().startswith("V"):
        suffix = text[1:]
        if suffix.isdigit():
            return f"V{int(suffix)}"

    numeric = to_int(text)
    if numeric is None or numeric <= 0:
        return None
    return f"V{numeric}"


def next_proposta_version(conn: sqlite3.Connection, oportunidade_id: int) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(MAX(versao), 0)
        FROM oportunidades_erp_historico
        WHERE oportunidade_id_ref = ?
          AND origem_historico = 'proposta'
        """,
        (oportunidade_id,),
    ).fetchone()
    current = 0
    if row is not None and row[0] is not None:
        current = int(row[0])
    return current + 1


def get_proposta_status_dates_by_oportunidade(
    conn: sqlite3.Connection,
    oportunidade_ids: list[int],
) -> dict[int, dict[str, str]]:
    if not oportunidade_ids:
        return {}

    placeholders = ", ".join(["?" for _ in oportunidade_ids])
    query = f"""
        SELECT
            oportunidade_id_ref,
            status_proposta_id,
            data_status
        FROM proposta_status_datas
        WHERE oportunidade_id_ref IN ({placeholders})
    """
    rows = conn.execute(query, tuple(oportunidade_ids)).fetchall()

    result: dict[int, dict[str, str]] = {}
    for row in rows:
        opp_id = int(row["oportunidade_id_ref"])
        status_id = str(int(row["status_proposta_id"]))
        result.setdefault(opp_id, {})[status_id] = str(row["data_status"])
    return result


def get_proposta_status_dates_for_oportunidade(
    conn: sqlite3.Connection,
    oportunidade_id: int,
) -> dict[str, str]:
    result = get_proposta_status_dates_by_oportunidade(conn, [oportunidade_id])
    return result.get(oportunidade_id, {})


def get_or_create_status_cliente_id(conn: sqlite3.Connection, status_nome: str) -> int:
    row = conn.execute(
        "SELECT status_cliente_id FROM status_cliente WHERE UPPER(nome) = UPPER(?)",
        (status_nome,),
    ).fetchone()
    if row is not None:
        return int(row[0])

    cur = conn.execute("INSERT INTO status_cliente (nome) VALUES (?)", (status_nome,))
    return int(cur.lastrowid)


def next_id_oportunidade(conn: sqlite3.Connection) -> str:
    year = datetime.now().year
    prefix = f"OPP-{year}-"

    rows = conn.execute(
        "SELECT id_oportunidade FROM oportunidades_erp WHERE id_oportunidade LIKE ?",
        (f"{prefix}%",),
    ).fetchall()

    max_seq = 0
    for row in rows:
        value = row[0]
        if value is None:
            continue
        text = str(value)
        if not text.startswith(prefix):
            continue
        seq_part = text[len(prefix):]
        if seq_part.isdigit():
            max_seq = max(max_seq, int(seq_part))

    next_seq = max_seq + 1
    return f"{prefix}{next_seq:06d}"


def next_codigo_proposta(conn: sqlite3.Connection) -> str:
    year = datetime.now().year
    prefix = f"PROP-{year}-"

    rows = conn.execute(
        "SELECT codigo_proposta FROM oportunidades_erp WHERE codigo_proposta LIKE ?",
        (f"{prefix}%",),
    ).fetchall()

    max_seq = 0
    for row in rows:
        value = row[0]
        if value is None:
            continue
        text = str(value)
        if not text.startswith(prefix):
            continue
        seq_part = text[len(prefix):]
        if seq_part.isdigit():
            max_seq = max(max_seq, int(seq_part))

    next_seq = max_seq + 1
    return f"{prefix}{next_seq:06d}"


def is_go_decisao(decisao_nome: str) -> bool:
    key = decisao_nome.strip().upper()
    return key in {"GO", "ON GO", "ONGO", "ON-GO"}


def is_no_go_decisao(decisao_nome: str) -> bool:
    key = decisao_nome.strip().upper()
    return key in {"NO-GO", "NO GO", "NOGO"}


@app.get("/")
def index() -> str:
    return render_template("index.html")


@app.get("/propostas")
def propostas() -> str:
    return render_template("propostas.html")


@app.get("/api/meta/cadastros")
def cadastros_meta() -> Any:
    return jsonify(MASTER_CONFIG)


@app.get("/api/cadastros/<name>")
def list_cadastro(name: str) -> Any:
    cfg = MASTER_CONFIG.get(name)
    if cfg is None:
        return jsonify({"error": "Cadastro invalido."}), 404

    with get_connection() as conn:
        if name == "clientes":
            rows = conn.execute(
                """
                SELECT
                    c.cliente_id AS id,
                    c.nome,
                    COALESCE(sc.nome, 'Novo') AS status_atual
                FROM clientes c
                LEFT JOIN status_cliente sc ON sc.status_cliente_id = c.status_cliente_id
                ORDER BY c.cliente_id DESC
                """
            ).fetchall()
        elif name == "pessoa_cliente":
            rows = conn.execute(
                """
                SELECT
                    pc.pessoa_cliente_id AS id,
                    pc.cliente_id,
                    c.nome AS cliente_nome,
                    pc.nome
                FROM pessoa_cliente pc
                JOIN clientes c ON c.cliente_id = pc.cliente_id
                ORDER BY pc.pessoa_cliente_id DESC
                """
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT {cfg['pk']} AS id, {', '.join(cfg['fields'])} FROM {cfg['table']} ORDER BY {cfg['pk']} DESC"
            ).fetchall()

    payload: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["versao_atual"] = format_version_label(item.get("versao_atual"))
        payload.append(item)
    return jsonify(payload)


@app.post("/api/cadastros/<name>")
def create_cadastro(name: str) -> Any:
    cfg = MASTER_CONFIG.get(name)
    if cfg is None:
        return jsonify({"error": "Cadastro invalido."}), 404

    data = request.get_json(silent=True) or {}
    payload: dict[str, Any] = {}
    for field in cfg["fields"]:
        value = normalize_text(data.get(field))
        payload[field] = value

    for req_field in cfg["required"]:
        if payload.get(req_field) is None:
            return jsonify({"error": f"Campo obrigatorio: {req_field}"}), 400

    try:
        with get_connection() as conn:
            if name == "clientes":
                novo_id = get_or_create_status_cliente_id(conn, "Novo")
                cur = conn.execute(
                    "INSERT INTO clientes (nome, status_cliente_id) VALUES (?, ?)",
                    (payload["nome"], novo_id),
                )
            elif name == "pessoa_cliente":
                cliente_id = to_int(payload.get("cliente_id"))
                if cliente_id is None:
                    return jsonify({"error": "Campo obrigatorio: cliente_id"}), 400

                cliente_exists = conn.execute(
                    "SELECT cliente_id FROM clientes WHERE cliente_id = ?",
                    (cliente_id,),
                ).fetchone()
                if cliente_exists is None:
                    return jsonify({"error": "Cliente informado nao existe."}), 400

                nome = normalize_text(payload.get("nome"))
                if nome is None:
                    return jsonify({"error": "Campo obrigatorio: nome"}), 400

                cur = conn.execute(
                    "INSERT INTO pessoa_cliente (cliente_id, nome) VALUES (?, ?)",
                    (cliente_id, nome),
                )
            elif name == "categorias":
                # Enforce categoria uniqueness by name even on legacy schema.
                existing = conn.execute(
                    "SELECT categoria_id FROM categorias WHERE UPPER(nome) = UPPER(?)",
                    (payload["nome"],),
                ).fetchone()
                if existing is not None:
                    return jsonify({"error": "Categoria ja cadastrada."}), 400
                cur = conn.execute("INSERT INTO categorias (nome) VALUES (?)", (payload["nome"],))
            else:
                columns = ", ".join(payload.keys())
                placeholders = ", ".join(["?" for _ in payload])
                cur = conn.execute(
                    f"INSERT INTO {cfg['table']} ({columns}) VALUES ({placeholders})",
                    tuple(payload.values()),
                )
            conn.commit()
            inserted_id = cur.lastrowid
    except DB_INTEGRITY_ERRORS as exc:
        return jsonify({"error": f"Registro duplicado ou invalido: {exc}"}), 400

    return jsonify({"message": "Cadastro salvo.", "id": inserted_id}), 201


@app.put("/api/cadastros/<name>/<int:record_id>")
def update_cadastro(name: str, record_id: int) -> Any:
    cfg = MASTER_CONFIG.get(name)
    if cfg is None:
        return jsonify({"error": "Cadastro invalido."}), 404

    data = request.get_json(silent=True) or {}
    payload: dict[str, Any] = {}
    for field in cfg["fields"]:
        payload[field] = normalize_text(data.get(field))

    for req_field in cfg["required"]:
        if payload.get(req_field) is None:
            return jsonify({"error": f"Campo obrigatorio: {req_field}"}), 400

    try:
        with get_connection() as conn:
            exists = conn.execute(
                f"SELECT {cfg['pk']} FROM {cfg['table']} WHERE {cfg['pk']} = ?",
                (record_id,),
            ).fetchone()
            if exists is None:
                return jsonify({"error": "Registro nao encontrado."}), 404

            if name == "categorias":
                dup = conn.execute(
                    "SELECT categoria_id FROM categorias WHERE UPPER(nome) = UPPER(?) AND categoria_id <> ?",
                    (payload["nome"], record_id),
                ).fetchone()
                if dup is not None:
                    return jsonify({"error": "Categoria ja cadastrada."}), 400

            if name == "pessoa_cliente":
                cliente_id = to_int(payload.get("cliente_id"))
                if cliente_id is None:
                    return jsonify({"error": "Campo obrigatorio: cliente_id"}), 400

                cliente_exists = conn.execute(
                    "SELECT cliente_id FROM clientes WHERE cliente_id = ?",
                    (cliente_id,),
                ).fetchone()
                if cliente_exists is None:
                    return jsonify({"error": "Cliente informado nao existe."}), 400

                nome = normalize_text(payload.get("nome"))
                if nome is None:
                    return jsonify({"error": "Campo obrigatorio: nome"}), 400

                dup = conn.execute(
                    """
                    SELECT pessoa_cliente_id
                    FROM pessoa_cliente
                    WHERE cliente_id = ?
                      AND UPPER(nome) = UPPER(?)
                      AND pessoa_cliente_id <> ?
                    """,
                    (cliente_id, nome, record_id),
                ).fetchone()
                if dup is not None:
                    return jsonify({"error": "Pessoa do cliente ja cadastrada para este cliente."}), 400

                conn.execute(
                    "UPDATE pessoa_cliente SET cliente_id = ?, nome = ? WHERE pessoa_cliente_id = ?",
                    (cliente_id, nome, record_id),
                )
                conn.commit()
                return jsonify({"message": "Cadastro atualizado com sucesso."})

            assignments = ", ".join([f"{field} = ?" for field in payload.keys()])
            values = list(payload.values()) + [record_id]
            conn.execute(
                f"UPDATE {cfg['table']} SET {assignments} WHERE {cfg['pk']} = ?",
                values,
            )
            conn.commit()
    except DB_INTEGRITY_ERRORS as exc:
        return jsonify({"error": f"Falha ao atualizar cadastro: {exc}"}), 400

    return jsonify({"message": "Cadastro atualizado com sucesso."})


@app.get("/api/oportunidades")
def list_oportunidades() -> Any:
    limit = request.args.get("limit", default=100, type=int)
    limit = max(1, min(limit, 5000))

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                o.oportunidade_id,
                o.id_oportunidade,
                o.codigo_proposta,
                o.data_entrada,
                o.data_envio,
                o.decisao_id,
                o.justificativa_id,
                c.nome AS cliente_nome,
                pc.nome AS pessoa_cliente_nome,
                sg.nome AS segmento_nome,
                d.nome AS decisao_nome,
                j.nome AS justificativa_nome,
                ca.nome AS canal_nome,
                r1.nome AS responsavel_1_nome,
                r2.nome AS responsavel_2_nome,
                ct.nome AS categoria_nome,
                o.valor,
                o.prazo_execucao_meses,
                sp.nome AS status_proposta_nome,
                an.nome AS area_negocio_nome,
                e.uf,
                o.objeto,
                o.ultimo_contato_data,
                o.observacoes_acompanhamento,
                o.versao_atual,
                o.created_at,
                o.updated_at
            FROM oportunidades_erp o
            JOIN clientes c ON c.cliente_id = o.cliente_id
            LEFT JOIN pessoa_cliente pc ON pc.pessoa_cliente_id = o.pessoa_cliente_id
            LEFT JOIN segmentos sg ON sg.segmento_id = o.segmento_id
            LEFT JOIN decisoes d ON d.decisao_id = o.decisao_id
            LEFT JOIN justificativas j ON j.justificativa_id = o.justificativa_id
            JOIN canais ca ON ca.canal_id = o.canal_id
            JOIN responsaveis r1 ON r1.responsavel_id = o.responsavel_1_id
            LEFT JOIN responsaveis r2 ON r2.responsavel_id = o.responsavel_2_id
            JOIN categorias ct ON ct.categoria_id = o.categoria_id
            LEFT JOIN status_proposta sp ON sp.status_proposta_id = o.status_proposta_id
            LEFT JOIN areas_negocio an ON an.area_negocio_id = o.area_negocio_id
            JOIN estados e ON e.estado_id = o.estado_id
            ORDER BY o.oportunidade_id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    payload: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["versao_atual"] = format_version_label(item.get("versao_atual"))
        payload.append(item)
    return jsonify(payload)


@app.get("/api/oportunidades/go")
def list_oportunidades_go() -> Any:
    limit = request.args.get("limit", default=100, type=int)
    limit = max(1, min(limit, 5000))

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                o.oportunidade_id,
                o.id_oportunidade,
                o.codigo_proposta,
                o.versao_atual,
                COALESCE(
                    (
                        SELECT MAX(hp.versao)
                        FROM oportunidades_erp_historico hp
                        WHERE hp.oportunidade_id_ref = o.oportunidade_id
                          AND hp.origem_historico = 'proposta'
                    ),
                    1
                ) AS versao_proposta_atual,
                o.data_entrada,
                o.data_envio,
                c.nome AS cliente_nome,
                pc.nome AS pessoa_cliente_nome,
                sg.nome AS segmento_nome,
                d.nome AS decisao_nome,
                j.nome AS justificativa_nome,
                ca.nome AS canal_nome,
                r1.nome AS responsavel_1_nome,
                r2.nome AS responsavel_2_nome,
                ct.nome AS categoria_nome,
                o.valor,
                o.status_proposta_id,
                o.responsavel_1_id,
                o.responsavel_2_id,
                o.prazo_execucao_meses,
                sp.nome AS status_proposta_nome,
                an.nome AS area_negocio_nome,
                e.uf,
                o.objeto,
                o.observacao_proposta,
                o.ultimo_contato_data,
                o.observacoes_acompanhamento,
                o.created_at,
                o.updated_at
            FROM oportunidades_erp o
            JOIN clientes c ON c.cliente_id = o.cliente_id
            LEFT JOIN pessoa_cliente pc ON pc.pessoa_cliente_id = o.pessoa_cliente_id
            LEFT JOIN segmentos sg ON sg.segmento_id = o.segmento_id
            JOIN decisoes d ON d.decisao_id = o.decisao_id
            LEFT JOIN justificativas j ON j.justificativa_id = o.justificativa_id
            JOIN canais ca ON ca.canal_id = o.canal_id
            JOIN responsaveis r1 ON r1.responsavel_id = o.responsavel_1_id
            LEFT JOIN responsaveis r2 ON r2.responsavel_id = o.responsavel_2_id
            JOIN categorias ct ON ct.categoria_id = o.categoria_id
            LEFT JOIN status_proposta sp ON sp.status_proposta_id = o.status_proposta_id
            LEFT JOIN areas_negocio an ON an.area_negocio_id = o.area_negocio_id
            JOIN estados e ON e.estado_id = o.estado_id
            WHERE UPPER(TRIM(d.nome)) IN ('GO', 'ON GO', 'ONGO', 'ON-GO')
            ORDER BY o.oportunidade_id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        opp_ids = [int(row["oportunidade_id"]) for row in rows]
        status_dates_by_opp = get_proposta_status_dates_by_oportunidade(conn, opp_ids)

    payload: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["versao_proposta_atual"] = format_version_label(item.get("versao_proposta_atual"))
        item["status_datas"] = status_dates_by_opp.get(int(item["oportunidade_id"]), {})
        payload.append(item)
    return jsonify(payload)


@app.put("/api/propostas/<int:oportunidade_id>")
def update_proposta_campos(oportunidade_id: int) -> Any:
    data = request.get_json(silent=True) or {}

    data_envio_text = normalize_text(data.get("data_envio"))
    data_envio = parse_iso_date(data_envio_text)
    if data_envio_text is not None and data_envio is None:
        return jsonify({"error": "Campo invalido: data_envio (yyyy-mm-dd)."}), 400

    parsed_status_dates: dict[int, str] | None = None
    if "status_datas" in data:
        raw_status_dates = data.get("status_datas")
        if raw_status_dates is None:
            raw_status_dates = {}
        if not isinstance(raw_status_dates, dict):
            return jsonify({"error": "Campo invalido: status_datas."}), 400

        parsed_status_dates = {}
        for raw_status_id, raw_date in raw_status_dates.items():
            status_id = to_int(raw_status_id)
            if status_id is None:
                continue

            date_text = normalize_text(raw_date)
            if date_text is None:
                continue

            parsed_date = parse_iso_date(date_text)
            if parsed_date is None:
                return jsonify({"error": f"Data invalida para status {status_id}: use yyyy-mm-dd."}), 400

            parsed_status_dates[status_id] = parsed_date

    try:
        with get_connection() as conn:
            exists = conn.execute(
                """
                SELECT
                    oportunidade_id, id_oportunidade, codigo_proposta,
                    data_entrada, data_envio, cliente_id, status_cliente_id,
                    pessoa_cliente_id, segmento_id, decisao_id, justificativa_id, canal_id,
                    responsavel_1_id, responsavel_2_id, categoria_id,
                    area_negocio_id, estado_id, status_proposta_id,
                    valor, tipo_evento, data_evento, valor_evento,
                    prazo_execucao_meses, ultimo_contato_data,
                    objeto, observacao_proposta, nome_contato, proxima_acao, farol, evento_encerrador,
                    observacoes_acompanhamento,
                    versao_atual, created_at, updated_at
                FROM oportunidades_erp
                WHERE oportunidade_id = ?
                """,
                (oportunidade_id,),
            ).fetchone()
            if exists is None:
                return jsonify({"error": "Proposta nao encontrada."}), 404

            decisao_row = conn.execute(
                "SELECT nome FROM decisoes WHERE decisao_id = ?",
                (exists["decisao_id"],),
            ).fetchone()
            if decisao_row is None or not is_go_decisao(str(decisao_row[0])):
                return jsonify({"error": "Apenas propostas GO podem ser editadas nesta tela."}), 400

            conn.execute(
                """
                UPDATE oportunidades_erp
                SET
                    status_proposta_id = ?,
                    responsavel_1_id = ?,
                    responsavel_2_id = ?,
                    valor = ?,
                    data_envio = ?,
                    observacao_proposta = ?,
                    versao_atual = ?,
                    updated_at = ?
                WHERE oportunidade_id = ?
                """,
                (
                    to_int(data.get("status_proposta_id")),
                    to_int(data.get("responsavel_1_id")) or int(exists["responsavel_1_id"]),
                    to_int(data.get("responsavel_2_id")),
                    to_float(data.get("valor")),
                    data_envio,
                    normalize_text(data.get("observacao_proposta")),
                    int(exists["versao_atual"]) + 1,
                    datetime.now().isoformat(timespec="seconds"),
                    oportunidade_id,
                ),
            )

            if parsed_status_dates is not None:
                conn.execute(
                    "DELETE FROM proposta_status_datas WHERE oportunidade_id_ref = ?",
                    (oportunidade_id,),
                )
                if parsed_status_dates:
                    for status_id, status_date in parsed_status_dates.items():
                        conn.execute(
                            """
                            INSERT INTO proposta_status_datas (oportunidade_id_ref, status_proposta_id, data_status)
                            VALUES (?, ?, ?)
                            """,
                            (oportunidade_id, status_id, status_date),
                        )

            current_status_dates = get_proposta_status_dates_for_oportunidade(conn, oportunidade_id)
            status_dates_snapshot_json = json.dumps(current_status_dates, ensure_ascii=True)

            proposta_versao = next_proposta_version(conn, oportunidade_id)

            updated = conn.execute(
                """
                SELECT
                    oportunidade_id, id_oportunidade, codigo_proposta,
                    data_entrada, data_envio, cliente_id, status_cliente_id,
                    pessoa_cliente_id, segmento_id, decisao_id, justificativa_id, canal_id,
                    responsavel_1_id, responsavel_2_id, categoria_id,
                    area_negocio_id, estado_id, status_proposta_id,
                    valor, tipo_evento, data_evento, valor_evento,
                    prazo_execucao_meses, ultimo_contato_data,
                    objeto, observacao_proposta, nome_contato, proxima_acao, farol, evento_encerrador,
                    observacoes_acompanhamento,
                    versao_atual, created_at, updated_at
                FROM oportunidades_erp
                WHERE oportunidade_id = ?
                """,
                (oportunidade_id,),
            ).fetchone()

            if updated is not None:
                conn.execute(
                    """
                    INSERT INTO oportunidades_erp_historico (
                        oportunidade_id_ref, versao, snapshot_at,
                        id_oportunidade, codigo_proposta,
                        data_entrada, data_envio,
                        cliente_id, pessoa_cliente_id, status_cliente_id, segmento_id,
                        decisao_id, justificativa_id,
                        canal_id, responsavel_1_id, responsavel_2_id,
                        categoria_id, area_negocio_id, estado_id, status_proposta_id,
                        valor, tipo_evento, data_evento, valor_evento,
                        prazo_execucao_meses, ultimo_contato_data,
                        objeto, observacao_proposta, nome_contato, proxima_acao, farol, evento_encerrador,
                        observacoes_acompanhamento,
                        status_datas_json,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        updated["oportunidade_id"],
                        proposta_versao,
                        datetime.now().isoformat(timespec="seconds"),
                        updated["id_oportunidade"],
                        updated["codigo_proposta"],
                        updated["data_entrada"],
                        updated["data_envio"],
                        updated["cliente_id"],
                        updated["pessoa_cliente_id"],
                        updated["status_cliente_id"],
                        updated["segmento_id"],
                        updated["decisao_id"],
                        updated["justificativa_id"],
                        updated["canal_id"],
                        updated["responsavel_1_id"],
                        updated["responsavel_2_id"],
                        updated["categoria_id"],
                        updated["area_negocio_id"],
                        updated["estado_id"],
                        updated["status_proposta_id"],
                        updated["valor"],
                        updated["tipo_evento"],
                        updated["data_evento"],
                        updated["valor_evento"],
                        updated["prazo_execucao_meses"],
                        updated["ultimo_contato_data"],
                        updated["objeto"],
                        updated["observacao_proposta"],
                        updated["nome_contato"],
                        updated["proxima_acao"],
                        updated["farol"],
                        updated["evento_encerrador"],
                        updated["observacoes_acompanhamento"],
                        status_dates_snapshot_json,
                        updated["created_at"],
                        updated["updated_at"],
                    ),
                )
                conn.execute(
                    "UPDATE oportunidades_erp_historico SET origem_historico = 'proposta' WHERE historico_id = last_insert_rowid()"
                )
            conn.commit()
    except DB_INTEGRITY_ERRORS as exc:
        return jsonify({"error": f"Falha ao atualizar proposta: {exc}"}), 400

    return jsonify({"message": "Proposta atualizada com sucesso."})


@app.put("/api/oportunidades/<int:oportunidade_id>/decisao")
def update_oportunidade_decisao(oportunidade_id: int) -> Any:
    data = request.get_json(silent=True) or {}

    decisao_id = to_int(data.get("decisao_id"))
    justificativa_id = to_int(data.get("justificativa_id"))
    if decisao_id is None:
        return jsonify({"error": "Campo obrigatorio: decisao_id"}), 400

    try:
        with get_connection() as conn:
            exists = conn.execute(
                """
                SELECT
                    oportunidade_id, id_oportunidade, codigo_proposta,
                    data_entrada, data_envio, cliente_id, status_cliente_id,
                    pessoa_cliente_id, segmento_id, decisao_id, justificativa_id, canal_id,
                    responsavel_1_id, responsavel_2_id, categoria_id,
                    area_negocio_id, estado_id, status_proposta_id,
                    valor, tipo_evento, data_evento, valor_evento,
                    prazo_execucao_meses, ultimo_contato_data,
                    objeto, observacao_proposta, nome_contato, proxima_acao, farol, evento_encerrador,
                    observacoes_acompanhamento,
                    versao_atual, created_at, updated_at
                FROM oportunidades_erp
                WHERE oportunidade_id = ?
                """,
                (oportunidade_id,),
            ).fetchone()
            if exists is None:
                return jsonify({"error": "Oportunidade nao encontrada."}), 404

            decisao_row = conn.execute(
                "SELECT nome FROM decisoes WHERE decisao_id = ?",
                (decisao_id,),
            ).fetchone()
            if decisao_row is None:
                return jsonify({"error": "Decisao invalida."}), 400

            decisao_nome = str(decisao_row[0]).strip().upper()
            decisao_antiga_nome = ""
            if exists["decisao_id"] is not None:
                antiga_row = conn.execute(
                    "SELECT nome FROM decisoes WHERE decisao_id = ?",
                    (exists["decisao_id"],),
                ).fetchone()
                if antiga_row is not None and antiga_row[0] is not None:
                    decisao_antiga_nome = str(antiga_row[0]).strip().upper()
            if is_no_go_decisao(decisao_nome):
                if justificativa_id is None:
                    return jsonify({"error": "Para decisao NO-GO, justificativa e obrigatoria."}), 400
            else:
                justificativa_id = None

            conn.execute(
                """
                INSERT INTO oportunidades_erp_historico (
                    oportunidade_id_ref, versao, snapshot_at,
                    id_oportunidade, codigo_proposta,
                    data_entrada, data_envio,
                    cliente_id, pessoa_cliente_id, status_cliente_id, segmento_id,
                    decisao_id, justificativa_id,
                    canal_id, responsavel_1_id, responsavel_2_id,
                    categoria_id, area_negocio_id, estado_id, status_proposta_id,
                    valor, tipo_evento, data_evento, valor_evento,
                    prazo_execucao_meses, ultimo_contato_data,
                    objeto, observacao_proposta, nome_contato, proxima_acao, farol, evento_encerrador,
                    observacoes_acompanhamento,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    exists["oportunidade_id"],
                    exists["versao_atual"],
                    datetime.now().isoformat(timespec="seconds"),
                    exists["id_oportunidade"],
                    exists["codigo_proposta"],
                    exists["data_entrada"],
                    exists["data_envio"],
                    exists["cliente_id"],
                    exists["pessoa_cliente_id"],
                    exists["status_cliente_id"],
                    exists["segmento_id"],
                    exists["decisao_id"],
                    exists["justificativa_id"],
                    exists["canal_id"],
                    exists["responsavel_1_id"],
                    exists["responsavel_2_id"],
                    exists["categoria_id"],
                    exists["area_negocio_id"],
                    exists["estado_id"],
                    exists["status_proposta_id"],
                    exists["valor"],
                    exists["tipo_evento"],
                    exists["data_evento"],
                    exists["valor_evento"],
                    exists["prazo_execucao_meses"],
                    exists["ultimo_contato_data"],
                    exists["objeto"],
                    exists["observacao_proposta"],
                    exists["nome_contato"],
                    exists["proxima_acao"],
                    exists["farol"],
                    exists["evento_encerrador"],
                    exists["observacoes_acompanhamento"],
                    exists["created_at"],
                    exists["updated_at"],
                ),
            )
            conn.execute(
                "UPDATE oportunidades_erp_historico SET origem_historico = 'oportunidade' WHERE historico_id = last_insert_rowid()"
            )

            conn.execute(
                """
                UPDATE oportunidades_erp
                SET
                    decisao_id = ?,
                    justificativa_id = ?,
                    versao_atual = ?,
                    updated_at = ?
                WHERE oportunidade_id = ?
                """,
                (
                    decisao_id,
                    justificativa_id,
                    int(exists["versao_atual"]) + 1,
                    datetime.now().isoformat(timespec="seconds"),
                    oportunidade_id,
                ),
            )

            became_go = is_go_decisao(decisao_nome) and not is_go_decisao(decisao_antiga_nome)

            atual = conn.execute(
                """
                SELECT
                    oportunidade_id, id_oportunidade, codigo_proposta,
                    data_entrada, data_envio, cliente_id, status_cliente_id,
                    pessoa_cliente_id, segmento_id, decisao_id, justificativa_id, canal_id,
                    responsavel_1_id, responsavel_2_id, categoria_id,
                    area_negocio_id, estado_id, status_proposta_id,
                    valor, tipo_evento, data_evento, valor_evento,
                    prazo_execucao_meses, ultimo_contato_data,
                    objeto, observacao_proposta, nome_contato, proxima_acao, farol, evento_encerrador,
                    observacoes_acompanhamento,
                    versao_atual, created_at, updated_at
                FROM oportunidades_erp
                WHERE oportunidade_id = ?
                """,
                (oportunidade_id,),
            ).fetchone()

            if atual is not None:
                conn.execute(
                    """
                    INSERT INTO oportunidades_erp_historico (
                        oportunidade_id_ref, versao, snapshot_at,
                        id_oportunidade, codigo_proposta,
                        data_entrada, data_envio,
                        cliente_id, pessoa_cliente_id, status_cliente_id, segmento_id,
                        decisao_id, justificativa_id,
                        canal_id, responsavel_1_id, responsavel_2_id,
                        categoria_id, area_negocio_id, estado_id, status_proposta_id,
                        valor, tipo_evento, data_evento, valor_evento,
                        prazo_execucao_meses, ultimo_contato_data,
                        objeto, observacao_proposta, nome_contato, proxima_acao, farol, evento_encerrador,
                        observacoes_acompanhamento,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        atual["oportunidade_id"],
                        atual["versao_atual"],
                        datetime.now().isoformat(timespec="seconds"),
                        atual["id_oportunidade"],
                        atual["codigo_proposta"],
                        atual["data_entrada"],
                        atual["data_envio"],
                        atual["cliente_id"],
                        atual["pessoa_cliente_id"],
                        atual["status_cliente_id"],
                        atual["segmento_id"],
                        atual["decisao_id"],
                        atual["justificativa_id"],
                        atual["canal_id"],
                        atual["responsavel_1_id"],
                        atual["responsavel_2_id"],
                        atual["categoria_id"],
                        atual["area_negocio_id"],
                        atual["estado_id"],
                        atual["status_proposta_id"],
                        atual["valor"],
                        atual["tipo_evento"],
                        atual["data_evento"],
                        atual["valor_evento"],
                        atual["prazo_execucao_meses"],
                        atual["ultimo_contato_data"],
                        atual["objeto"],
                        atual["observacao_proposta"],
                        atual["nome_contato"],
                        atual["proxima_acao"],
                        atual["farol"],
                        atual["evento_encerrador"],
                        atual["observacoes_acompanhamento"],
                        atual["created_at"],
                        atual["updated_at"],
                    ),
                )
                conn.execute(
                    "UPDATE oportunidades_erp_historico SET origem_historico = 'oportunidade' WHERE historico_id = last_insert_rowid()"
                )

                # On the first transition to GO, register proposal baseline as V1
                # so proposal history starts at the transition moment.
                if became_go:
                    proposta_versao = next_proposta_version(conn, oportunidade_id)
                    status_dates_snapshot_json = json.dumps(
                        get_proposta_status_dates_for_oportunidade(conn, oportunidade_id),
                        ensure_ascii=True,
                    )
                    conn.execute(
                        """
                        INSERT INTO oportunidades_erp_historico (
                            oportunidade_id_ref, versao, snapshot_at,
                            id_oportunidade, codigo_proposta,
                            data_entrada, data_envio,
                            cliente_id, pessoa_cliente_id, status_cliente_id, segmento_id,
                            decisao_id, justificativa_id,
                            canal_id, responsavel_1_id, responsavel_2_id,
                            categoria_id, area_negocio_id, estado_id, status_proposta_id,
                            valor, tipo_evento, data_evento, valor_evento,
                            prazo_execucao_meses, ultimo_contato_data,
                            objeto, observacao_proposta, nome_contato, proxima_acao, farol, evento_encerrador,
                            observacoes_acompanhamento,
                            status_datas_json,
                            created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            atual["oportunidade_id"],
                            proposta_versao,
                            datetime.now().isoformat(timespec="seconds"),
                            atual["id_oportunidade"],
                            atual["codigo_proposta"],
                            atual["data_entrada"],
                            atual["data_envio"],
                            atual["cliente_id"],
                            atual["pessoa_cliente_id"],
                            atual["status_cliente_id"],
                            atual["segmento_id"],
                            atual["decisao_id"],
                            atual["justificativa_id"],
                            atual["canal_id"],
                            atual["responsavel_1_id"],
                            atual["responsavel_2_id"],
                            atual["categoria_id"],
                            atual["area_negocio_id"],
                            atual["estado_id"],
                            atual["status_proposta_id"],
                            atual["valor"],
                            atual["tipo_evento"],
                            atual["data_evento"],
                            atual["valor_evento"],
                            atual["prazo_execucao_meses"],
                            atual["ultimo_contato_data"],
                            atual["objeto"],
                            atual["observacao_proposta"],
                            atual["nome_contato"],
                            atual["proxima_acao"],
                            atual["farol"],
                            atual["evento_encerrador"],
                            atual["observacoes_acompanhamento"],
                            status_dates_snapshot_json,
                            atual["created_at"],
                            atual["updated_at"],
                        ),
                    )
                    conn.execute(
                        "UPDATE oportunidades_erp_historico SET origem_historico = 'proposta' WHERE historico_id = last_insert_rowid()"
                    )
            conn.commit()
    except DB_INTEGRITY_ERRORS as exc:
        return jsonify({"error": f"Falha ao atualizar decisao: {exc}"}), 400

    return jsonify({"message": "Decisao atualizada com sucesso."})


@app.post("/api/oportunidades")
def create_oportunidade() -> Any:
    data = request.get_json(silent=True) or {}

    data_entrada = parse_iso_date(data.get("data_entrada"))

    if data_entrada is None:
        return jsonify({"error": "Campo obrigatorio: data_entrada (yyyy-mm-dd)"}), 400

    required_fk_fields = [
        "cliente_id",
        "canal_id",
        "responsavel_1_id",
        "categoria_id",
        "area_negocio_id",
        "estado_id",
    ]
    fk_values: dict[str, int | None] = {
        "cliente_id": to_int(data.get("cliente_id")),
        "pessoa_cliente_id": to_int(data.get("pessoa_cliente_id")),
        "status_cliente_id": to_int(data.get("status_cliente_id")),
        "segmento_id": to_int(data.get("segmento_id")),
        "canal_id": to_int(data.get("canal_id")),
        "responsavel_1_id": to_int(data.get("responsavel_1_id")),
        "responsavel_2_id": to_int(data.get("responsavel_2_id")),
        "categoria_id": to_int(data.get("categoria_id")),
        "area_negocio_id": to_int(data.get("area_negocio_id")),
        "estado_id": to_int(data.get("estado_id")),
        "justificativa_id": to_int(data.get("justificativa_id")),
        "decisao_id": to_int(data.get("decisao_id")),
        "status_proposta_id": to_int(data.get("status_proposta_id")),
    }

    for field in required_fk_fields:
        if fk_values[field] is None:
            return jsonify({"error": f"Campo obrigatorio: {field}"}), 400

    data_envio = parse_iso_date(data.get("data_envio"))
    ultimo_contato_data = parse_iso_date(data.get("ultimo_contato_data"))
    data_evento = parse_iso_date(data.get("data_evento"))

    try:
        with get_connection() as conn:
            id_oportunidade = next_id_oportunidade(conn)
            codigo_proposta = next_codigo_proposta(conn)

            # Pull customer current status and decision name to apply business transition.
            cliente_row = conn.execute(
                "SELECT status_cliente_id FROM clientes WHERE cliente_id = ?",
                (fk_values["cliente_id"],),
            ).fetchone()
            if cliente_row is None:
                return jsonify({"error": "Cliente invalido."}), 400

            status_cliente_id = cliente_row[0]
            if status_cliente_id is None:
                status_cliente_id = get_or_create_status_cliente_id(conn, "Novo")

            if fk_values["pessoa_cliente_id"] is not None:
                pessoa_ok = conn.execute(
                    "SELECT pessoa_cliente_id FROM pessoa_cliente WHERE pessoa_cliente_id = ? AND cliente_id = ?",
                    (fk_values["pessoa_cliente_id"], fk_values["cliente_id"]),
                ).fetchone()
                if pessoa_ok is None:
                    return jsonify({"error": "Pessoa do cliente invalida para o cliente selecionado."}), 400

            fk_values["decisao_id"] = None
            fk_values["justificativa_id"] = None

            payload = {
                "id_oportunidade": id_oportunidade,
                "data_entrada": data_entrada,
                "cliente_id": fk_values["cliente_id"],
                "pessoa_cliente_id": fk_values["pessoa_cliente_id"],
                "status_cliente_id": status_cliente_id,
                "segmento": normalize_text(data.get("segmento")),
                "segmento_id": fk_values["segmento_id"],
                "decisao_id": fk_values["decisao_id"],
                "justificativa_id": fk_values["justificativa_id"],
                "canal_id": fk_values["canal_id"],
                "responsavel_1_id": fk_values["responsavel_1_id"],
                "codigo_proposta": codigo_proposta,
                "objeto": normalize_text(data.get("objeto")),
                "categoria_id": fk_values["categoria_id"],
                "area_negocio_id": fk_values["area_negocio_id"],
                "estado_id": fk_values["estado_id"],
                "valor": to_float(data.get("valor")),
                "data_evento": data_evento,
                "prazo_execucao_meses": to_int(data.get("prazo_execucao_meses")),
                "data_envio": data_envio,
                "status_proposta_id": fk_values["status_proposta_id"],
                "responsavel_2_id": fk_values["responsavel_2_id"],
                "observacao_proposta": normalize_text(data.get("observacao_proposta")),
                "ultimo_contato_data": ultimo_contato_data,
                "observacoes_acompanhamento": normalize_text(data.get("observacoes_acompanhamento")),
                "versao_atual": 1,
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }

            columns = ", ".join(payload.keys())
            placeholders = ", ".join(["?" for _ in payload])
            conn.execute(
                f"INSERT INTO oportunidades_erp ({columns}) VALUES ({placeholders})",
                tuple(payload.values()),
            )
            conn.commit()
    except DB_INTEGRITY_ERRORS as exc:
        return jsonify({"error": f"Falha ao cadastrar oportunidade: {exc}"}), 400

    return jsonify(
        {
            "message": "Oportunidade cadastrada com sucesso.",
            "id_oportunidade": id_oportunidade,
            "codigo_proposta": codigo_proposta,
        }
    ), 201


@app.get("/api/oportunidades/<int:oportunidade_id>")
def get_oportunidade(oportunidade_id: int) -> Any:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                oportunidade_id,
                id_oportunidade,
                codigo_proposta,
                data_entrada,
                data_envio,
                cliente_id,
                pessoa_cliente_id,
                segmento_id,
                decisao_id,
                justificativa_id,
                canal_id,
                responsavel_1_id,
                responsavel_2_id,
                categoria_id,
                area_negocio_id,
                estado_id,
                status_proposta_id,
                valor,
                tipo_evento,
                data_evento,
                valor_evento,
                prazo_execucao_meses,
                ultimo_contato_data,
                objeto,
                observacao_proposta,
                nome_contato,
                proxima_acao,
                farol,
                evento_encerrador,
                observacoes_acompanhamento
            FROM oportunidades_erp
            WHERE oportunidade_id = ?
            """,
            (oportunidade_id,),
        ).fetchone()

    if row is None:
        return jsonify({"error": "Oportunidade nao encontrada."}), 404

    return jsonify(dict(row))


@app.get("/api/oportunidades/<int:oportunidade_id>/historico")
def get_oportunidade_historico(oportunidade_id: int) -> Any:
    with get_connection() as conn:
        exists = conn.execute(
            "SELECT oportunidade_id FROM oportunidades_erp WHERE oportunidade_id = ?",
            (oportunidade_id,),
        ).fetchone()
        if exists is None:
            return jsonify({"error": "Oportunidade nao encontrada."}), 404

        rows = conn.execute(
            """
            SELECT
                h.historico_id,
                h.versao,
                h.snapshot_at,
                h.id_oportunidade,
                h.codigo_proposta,
                h.data_entrada,
                h.data_envio,
                c.nome AS cliente_nome,
                pc.nome AS pessoa_cliente_nome,
                sg.nome AS segmento_nome,
                d.nome AS decisao_nome,
                j.nome AS justificativa_nome,
                ca.nome AS canal_nome,
                r1.nome AS responsavel_1_nome,
                r2.nome AS responsavel_2_nome,
                ct.nome AS categoria_nome,
                an.nome AS area_negocio_nome,
                e.uf,
                h.valor,
                h.prazo_execucao_meses,
                sp.nome AS status_proposta_nome,
                h.ultimo_contato_data,
                h.objeto,
                h.observacao_proposta,
                h.observacoes_acompanhamento,
                h.created_at,
                h.updated_at
            FROM oportunidades_erp_historico h
            JOIN clientes c ON c.cliente_id = h.cliente_id
            LEFT JOIN pessoa_cliente pc ON pc.pessoa_cliente_id = h.pessoa_cliente_id
            LEFT JOIN segmentos sg ON sg.segmento_id = h.segmento_id
            LEFT JOIN decisoes d ON d.decisao_id = h.decisao_id
            LEFT JOIN justificativas j ON j.justificativa_id = h.justificativa_id
            JOIN canais ca ON ca.canal_id = h.canal_id
            JOIN responsaveis r1 ON r1.responsavel_id = h.responsavel_1_id
            LEFT JOIN responsaveis r2 ON r2.responsavel_id = h.responsavel_2_id
            JOIN categorias ct ON ct.categoria_id = h.categoria_id
            LEFT JOIN areas_negocio an ON an.area_negocio_id = h.area_negocio_id
            JOIN estados e ON e.estado_id = h.estado_id
            LEFT JOIN status_proposta sp ON sp.status_proposta_id = h.status_proposta_id
            WHERE h.oportunidade_id_ref = ?
                            AND h.origem_historico = 'oportunidade'
            ORDER BY h.versao DESC, h.historico_id DESC
            """,
            (oportunidade_id,),
        ).fetchall()

    payload: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["versao"] = format_version_label(item.get("versao"))
        payload.append(item)
    return jsonify(payload)


@app.get("/api/propostas/<int:oportunidade_id>/historico")
def get_proposta_historico(oportunidade_id: int) -> Any:
    with get_connection() as conn:
        exists = conn.execute(
            "SELECT oportunidade_id FROM oportunidades_erp WHERE oportunidade_id = ?",
            (oportunidade_id,),
        ).fetchone()
        if exists is None:
            return jsonify({"error": "Oportunidade nao encontrada."}), 404

        rows = conn.execute(
            """
            SELECT
                h.historico_id,
                h.versao,
                h.snapshot_at,
                h.id_oportunidade,
                h.codigo_proposta,
                h.data_entrada,
                h.data_envio,
                c.nome AS cliente_nome,
                pc.nome AS pessoa_cliente_nome,
                sg.nome AS segmento_nome,
                d.nome AS decisao_nome,
                j.nome AS justificativa_nome,
                ca.nome AS canal_nome,
                r1.nome AS responsavel_1_nome,
                r2.nome AS responsavel_2_nome,
                ct.nome AS categoria_nome,
                an.nome AS area_negocio_nome,
                e.uf,
                h.valor,
                h.data_evento,
                h.prazo_execucao_meses,
                sp.nome AS status_proposta_nome,
                h.ultimo_contato_data,
                h.objeto,
                h.observacao_proposta,
                h.status_datas_json,
                h.observacoes_acompanhamento,
                h.created_at,
                h.updated_at
            FROM oportunidades_erp_historico h
            JOIN clientes c ON c.cliente_id = h.cliente_id
            LEFT JOIN pessoa_cliente pc ON pc.pessoa_cliente_id = h.pessoa_cliente_id
            LEFT JOIN segmentos sg ON sg.segmento_id = h.segmento_id
            LEFT JOIN decisoes d ON d.decisao_id = h.decisao_id
            LEFT JOIN justificativas j ON j.justificativa_id = h.justificativa_id
            JOIN canais ca ON ca.canal_id = h.canal_id
            JOIN responsaveis r1 ON r1.responsavel_id = h.responsavel_1_id
            LEFT JOIN responsaveis r2 ON r2.responsavel_id = h.responsavel_2_id
            JOIN categorias ct ON ct.categoria_id = h.categoria_id
            LEFT JOIN areas_negocio an ON an.area_negocio_id = h.area_negocio_id
            JOIN estados e ON e.estado_id = h.estado_id
            LEFT JOIN status_proposta sp ON sp.status_proposta_id = h.status_proposta_id
            WHERE h.oportunidade_id_ref = ?
                            AND h.origem_historico = 'proposta'
            ORDER BY h.versao DESC, h.historico_id DESC
            """,
            (oportunidade_id,),
        ).fetchall()

    payload: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        status_datas_json = item.get("status_datas_json")
        status_datas: dict[str, str] = {}
        if status_datas_json:
            try:
                parsed = json.loads(str(status_datas_json))
                if isinstance(parsed, dict):
                    status_datas = {
                        str(key): str(value)
                        for key, value in parsed.items()
                        if value is not None and str(value).strip() != ""
                    }
            except json.JSONDecodeError:
                status_datas = {}
        item["status_datas"] = status_datas
        payload.append(item)

    return jsonify(payload)


@app.put("/api/oportunidades/<int:oportunidade_id>")
def update_oportunidade(oportunidade_id: int) -> Any:
    data = request.get_json(silent=True) or {}

    data_entrada = parse_iso_date(data.get("data_entrada"))
    if data_entrada is None:
        return jsonify({"error": "Campo obrigatorio: data_entrada (yyyy-mm-dd)"}), 400

    required_fk_fields = [
        "cliente_id",
        "canal_id",
        "responsavel_1_id",
        "categoria_id",
        "area_negocio_id",
        "estado_id",
        "decisao_id",
    ]
    fk_values: dict[str, int | None] = {
        "cliente_id": to_int(data.get("cliente_id")),
        "pessoa_cliente_id": to_int(data.get("pessoa_cliente_id")),
        "segmento_id": to_int(data.get("segmento_id")),
        "canal_id": to_int(data.get("canal_id")),
        "responsavel_1_id": to_int(data.get("responsavel_1_id")),
        "responsavel_2_id": to_int(data.get("responsavel_2_id")),
        "categoria_id": to_int(data.get("categoria_id")),
        "area_negocio_id": to_int(data.get("area_negocio_id")),
        "estado_id": to_int(data.get("estado_id")),
        "justificativa_id": to_int(data.get("justificativa_id")),
        "decisao_id": to_int(data.get("decisao_id")),
        "status_proposta_id": to_int(data.get("status_proposta_id")),
    }

    for field in required_fk_fields:
        if fk_values[field] is None:
            return jsonify({"error": f"Campo obrigatorio: {field}"}), 400

    data_envio = parse_iso_date(data.get("data_envio"))
    ultimo_contato_data = parse_iso_date(data.get("ultimo_contato_data"))
    data_evento = parse_iso_date(data.get("data_evento"))

    try:
        with get_connection() as conn:
            exists = conn.execute(
                """
                SELECT
                    oportunidade_id, id_oportunidade, codigo_proposta,
                    data_entrada, data_envio, cliente_id, status_cliente_id,
                    pessoa_cliente_id, segmento_id, decisao_id, justificativa_id, canal_id,
                    responsavel_1_id, responsavel_2_id, categoria_id,
                    area_negocio_id, estado_id, status_proposta_id,
                    valor, tipo_evento, data_evento, valor_evento,
                    prazo_execucao_meses, ultimo_contato_data,
                    objeto, observacao_proposta, nome_contato, proxima_acao, farol, evento_encerrador,
                    observacoes_acompanhamento,
                    versao_atual, created_at, updated_at
                FROM oportunidades_erp
                WHERE oportunidade_id = ?
                """,
                (oportunidade_id,),
            ).fetchone()
            if exists is None:
                return jsonify({"error": "Oportunidade nao encontrada."}), 404

            # Store previous state in history before applying changes.
            conn.execute(
                """
                INSERT INTO oportunidades_erp_historico (
                    oportunidade_id_ref, versao, snapshot_at,
                    id_oportunidade, codigo_proposta,
                    data_entrada, data_envio,
                    cliente_id, pessoa_cliente_id, status_cliente_id, segmento_id,
                    decisao_id, justificativa_id,
                    canal_id, responsavel_1_id, responsavel_2_id,
                    categoria_id, area_negocio_id, estado_id, status_proposta_id,
                    valor, tipo_evento, data_evento, valor_evento,
                    prazo_execucao_meses, ultimo_contato_data,
                    objeto, observacao_proposta, nome_contato, proxima_acao, farol, evento_encerrador,
                    observacoes_acompanhamento,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    exists["oportunidade_id"],
                    exists["versao_atual"],
                    datetime.now().isoformat(timespec="seconds"),
                    exists["id_oportunidade"],
                    exists["codigo_proposta"],
                    exists["data_entrada"],
                    exists["data_envio"],
                    exists["cliente_id"],
                    exists["pessoa_cliente_id"],
                    exists["status_cliente_id"],
                    exists["segmento_id"],
                    exists["decisao_id"],
                    exists["justificativa_id"],
                    exists["canal_id"],
                    exists["responsavel_1_id"],
                    exists["responsavel_2_id"],
                    exists["categoria_id"],
                    exists["area_negocio_id"],
                    exists["estado_id"],
                    exists["status_proposta_id"],
                    exists["valor"],
                    exists["tipo_evento"],
                    exists["data_evento"],
                    exists["valor_evento"],
                    exists["prazo_execucao_meses"],
                    exists["ultimo_contato_data"],
                    exists["objeto"],
                    exists["observacao_proposta"],
                    exists["nome_contato"],
                    exists["proxima_acao"],
                    exists["farol"],
                    exists["evento_encerrador"],
                    exists["observacoes_acompanhamento"],
                    exists["created_at"],
                    exists["updated_at"],
                ),
            )

            cliente_row = conn.execute(
                "SELECT status_cliente_id FROM clientes WHERE cliente_id = ?",
                (fk_values["cliente_id"],),
            ).fetchone()
            if cliente_row is None:
                return jsonify({"error": "Cliente invalido."}), 400

            status_cliente_id = cliente_row[0]
            if status_cliente_id is None:
                status_cliente_id = get_or_create_status_cliente_id(conn, "Novo")

            if fk_values["pessoa_cliente_id"] is not None:
                pessoa_ok = conn.execute(
                    "SELECT pessoa_cliente_id FROM pessoa_cliente WHERE pessoa_cliente_id = ? AND cliente_id = ?",
                    (fk_values["pessoa_cliente_id"], fk_values["cliente_id"]),
                ).fetchone()
                if pessoa_ok is None:
                    return jsonify({"error": "Pessoa do cliente invalida para o cliente selecionado."}), 400

            decisao_row = conn.execute(
                "SELECT nome FROM decisoes WHERE decisao_id = ?",
                (fk_values["decisao_id"],),
            ).fetchone()
            if decisao_row is None:
                return jsonify({"error": "Decisao invalida."}), 400

            decisao_nome = str(decisao_row[0]).strip().upper()

            if is_no_go_decisao(decisao_nome):
                if fk_values["justificativa_id"] is None:
                    return jsonify({"error": "Para decisao NO-GO, justificativa e obrigatoria."}), 400
            else:
                fk_values["justificativa_id"] = None

            conn.execute(
                """
                UPDATE oportunidades_erp
                SET
                    data_entrada = ?,
                    data_envio = ?,
                    cliente_id = ?,
                    pessoa_cliente_id = ?,
                    status_cliente_id = ?,
                    segmento_id = ?,
                    decisao_id = ?,
                    justificativa_id = ?,
                    canal_id = ?,
                    responsavel_1_id = ?,
                    responsavel_2_id = ?,
                    categoria_id = ?,
                    area_negocio_id = ?,
                    estado_id = ?,
                    status_proposta_id = ?,
                    valor = ?,
                    data_evento = ?,
                    prazo_execucao_meses = ?,
                    ultimo_contato_data = ?,
                    objeto = ?,
                    observacao_proposta = ?,
                    observacoes_acompanhamento = ?,
                    versao_atual = ?,
                    updated_at = ?
                WHERE oportunidade_id = ?
                """,
                (
                    data_entrada,
                    data_envio,
                    fk_values["cliente_id"],
                    fk_values["pessoa_cliente_id"],
                    status_cliente_id,
                    fk_values["segmento_id"],
                    fk_values["decisao_id"],
                    fk_values["justificativa_id"],
                    fk_values["canal_id"],
                    fk_values["responsavel_1_id"],
                    fk_values["responsavel_2_id"],
                    fk_values["categoria_id"],
                    fk_values["area_negocio_id"],
                    fk_values["estado_id"],
                    fk_values["status_proposta_id"],
                    to_float(data.get("valor")),
                    data_evento,
                    to_int(data.get("prazo_execucao_meses")),
                    ultimo_contato_data,
                    normalize_text(data.get("objeto")),
                    normalize_text(data.get("observacao_proposta")),
                    normalize_text(data.get("observacoes_acompanhamento")),
                    int(exists["versao_atual"]) + 1,
                    datetime.now().isoformat(timespec="seconds"),
                    oportunidade_id,
                ),
            )
            conn.commit()
    except DB_INTEGRITY_ERRORS as exc:
        return jsonify({"error": f"Falha ao atualizar oportunidade: {exc}"}), 400

    return jsonify({"message": "Oportunidade atualizada com sucesso."})


@app.get("/api/summary")
def summary() -> Any:
    with get_connection() as conn:
        total_cadastros = {}
        for name, cfg in MASTER_CONFIG.items():
            count = conn.execute(f"SELECT COUNT(*) FROM {cfg['table']}").fetchone()[0]
            total_cadastros[name] = count

        total_oportunidades = conn.execute("SELECT COUNT(*) FROM oportunidades_erp").fetchone()[0]

    return jsonify({"cadastros": total_cadastros, "oportunidades": total_oportunidades})


init_db()


if __name__ == "__main__":
    app.run(debug=True)

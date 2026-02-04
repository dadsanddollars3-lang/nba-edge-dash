import os
import re
import hashlib
import socket
import ipaddress
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, unquote

import requests
import pandas as pd
import psycopg
from psycopg.types.json import Json
import streamlit as st

st.set_page_config(page_title="NBA Edge Dash", layout="wide")

# =======================
# Secrets / Config
# =======================
def S(key: str, default=None):
    try:
        if hasattr(st, "secrets") and key in st.secrets:
            return st.secrets.get(key)
    except Exception:
        pass
    return os.environ.get(key, default)

ODDS_API_KEY = S("ODDS_API_KEY")
DATABASE_URL_RAW = S("DATABASE_URL")

SPORT_KEY = "basketball_nba"
REGIONS = "us"
MARKETS = "h2h,spreads,totals"
ODDS_FORMAT = "american"

TARGET_BOOKS = ["betonlineag", "caesars"]
MODEL_ID = "BASELINE"
MODEL_VERSION = S("MODEL_VERSION", "v1")

# =======================
# DATABASE_URL parsing + sanitizing
# =======================
def _clean_scalar(v: str | None) -> str | None:
    if v is None:
        return None
    v = str(v).strip().strip('"').strip("'").strip()
    lines = [ln.strip() for ln in v.splitlines() if ln.strip()]
    if len(lines) > 1:
        v = lines[-1]
    return v.strip() or None

def _normalize_db_url(url: str) -> str:
    url = _clean_scalar(url) or ""
    url = re.sub(r"^(database_url|db_url|url)\s*:\s*", "", url, flags=re.IGNORECASE).strip()
    if not url or "://" not in url:
        raise RuntimeError("DATABASE_URL must be a full URL starting with postgresql://")
    u = urlparse(url)
    if u.scheme not in ("postgresql", "postgres"):
        raise RuntimeError("DATABASE_URL must start with postgresql://")
    qs = parse_qs(u.query)
    if "sslmode" not in qs:
        qs["sslmode"] = ["require"]
    new_query = urlencode(qs, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))

def _parse_database_url(url: str) -> dict:
    u = urlparse(url)
    host = u.hostname
    if not host:
        raise RuntimeError("DATABASE_URL hostname could not be parsed.")
    user = unquote(u.username) if u.username else None
    password = unquote(u.password) if u.password else None
    port = u.port or 5432
    dbname = (u.path or "").lstrip("/")
    if not all([host, user, password, dbname]):
        raise RuntimeError("DATABASE_URL missing host/user/password/dbname.")
    return {"host": host.strip(), "port": int(port), "dbname": dbname, "user": user, "password": password, "url": url}

# =======================
# IPv4 forcing
# =======================
def _is_ipv4_literal(x: str) -> bool:
    try:
        return isinstance(ipaddress.ip_address(str(x)), ipaddress.IPv4Address)
    except Exception:
        return False

def _doh_lookup_a(host: str) -> str | None:
    resolvers = [
        ("https://dns.google/resolve", {"name": host, "type": "A"}),
        ("https://cloudflare-dns.com/dns-query", {"name": host, "type": "A"}),
    ]
    for url, params in resolvers:
        try:
            headers = {}
            if "cloudflare-dns.com" in url:
                headers["accept"] = "application/dns-json"
            r = requests.get(url, params=params, headers=headers, timeout=6)
            r.raise_for_status()
            j = r.json()
            for ans in (j.get("Answer") or []):
                data = ans.get("data")
                if data and _is_ipv4_literal(data):
                    return data
        except Exception:
            continue
    return None

def _resolve_ipv4(host: str) -> str | None:
    if not host:
        return None
    host = host.strip()
    if _is_ipv4_literal(host):
        return host

    try:
        infos = socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)
        if infos:
            return infos[0][4][0]
    except Exception:
        pass

    return _doh_lookup_a(host)

@st.cache_resource(show_spinner=False)
def _db_plan():
    if not DATABASE_URL_RAW:
        raise RuntimeError("Missing DATABASE_URL in Streamlit secrets.")
    url = _normalize_db_url(DATABASE_URL_RAW)
    cfg = _parse_database_url(url)
    ipv4 = _resolve_ipv4(cfg["host"])
    if not ipv4:
        raise RuntimeError(
            f"Could not resolve IPv4 A-record for '{cfg['host']}'. "
            "If Streamlit DNS is broken, DoH should resolve it; if this still fails, the hostname is wrong."
        )
    return cfg, ipv4

def db_conn():
    cfg, ipv4 = _db_plan()
    # keep host for TLS/SNI; force socket with hostaddr
    return psycopg.connect(
        cfg["url"],
        host=cfg["host"],
        hostaddr=ipv4,
        connect_timeout=12,
    )

def run_sql(sql: str, params=None):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
        conn.commit()

def init_db():
    ddl = """
    create table if not exists events (
        event_id text primary key,
        league text not null default 'NBA',
        home_team text not null,
        away_team text not null,
        start_time timestamptz not null
    );

    create table if not exists odds_snapshots (
        ts timestamptz not null default now(),
        event_id text not null references events(event_id) on delete cascade,
        book text not null,
        market text not null,
        selection text not null,
        line numeric null,
        price_american integer not null
    );
    create index if not exists idx_odds_snapshots_ts on odds_snapshots(ts desc);
    create index if not exists idx_odds_snapshots_key on odds_snapshots(event_id, book, market, selection, line, ts desc);

    create table if not exists runs (
        run_id bigserial primary key,
        created_at timestamptz not null default now(),
        model_id text not null,
        model_version text not null,
        notes text not null default '',
        inputs_hash text not null
    );
    create index if not exists idx_runs_created_at on runs(created_at desc);

    create table if not exists model_outputs (
        run_id bigint not null references runs(run_id) on delete cascade,
        event_id text not null references events(event_id) on delete cascade,
        market text not null,
        selection text not null,
        line numeric null,
        p_fair double precision not null,
        fair_price_american integer not null
    );
    create index if not exists idx_model_outputs_run on model_outputs(run_id);

    create table if not exists bets (
        bet_id bigserial primary key,
        book text not null,
        bet_type text not null,
        event_id text not null references events(event_id) on delete cascade,
        bet_ts timestamptz not null,
        model_id text not null,
        model_version text not null,
        run_id bigint not null references runs(run_id) on delete restrict,
        legs_json jsonb not null,
        stake numeric not null,
        price_taken_american integer not null,
        p_fair_at_bet double precision not null,
        edge_at_bet double precision not null
    );
    create index if not exists idx_bets_bet_ts on bets(bet_ts desc);
    """
    run_sql(ddl)

# =======================
# Odds API
# =======================
def fetch_odds():
    if not ODDS_API_KEY:
        raise RuntimeError("Missing ODDS_API_KEY in Streamlit secrets.")
    url = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/odds"
    params = {"regions": REGIONS, "markets": MARKETS, "oddsFormat": ODDS_FORMAT, "apiKey": ODDS_API_KEY}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def normalize_book(bookmaker: dict) -> str:
    k = (bookmaker.get("key") or "").strip().lower()
    t = (bookmaker.get("title") or "").strip().lower()
    if "caesars" in t:
        return "caesars"
    return k or t.replace(" ", "_")

# =======================
# Odds math
# =======================
def american_to_prob(odds: int) -> float:
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return (-odds) / ((-odds) + 100.0)

def devig_two_way(p1: float, p2: float):
    s = p1 + p2
    if s <= 0:
        return (float("nan"), float("nan"))
    return (p1 / s, p2 / s)

def prob_to_american(p: float) -> int:
    if p <= 0 or p >= 1:
        return 0
    if p >= 0.5:
        return int(round(-100.0 * p / (1.0 - p)))
    return int(round(100.0 * (1.0 - p) / p))

# =======================
# Load latest snapshots
# =======================
def load_latest_snapshots(lookback_hours: int) -> pd.DataFrame:
    q = """
    select s.ts, e.start_time, e.home_team, e.away_team,
           s.event_id, s.book, s.market, s.selection, s.line, s.price_american
    from odds_snapshots s
    join events e on e.event_id = s.event_id
    where s.ts >= now() - (%s || ' hours')::interval
    order by s.ts desc
    limit 30000
    """
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (int(lookback_hours),))
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df["start_time"] = pd.to_datetime(df["start_time"], utc=True)
    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    return df

def latest_per_key(df: pd.DataFrame) -> pd.DataFrame:
    keys = ["event_id", "book", "market", "selection", "line"]
    return df.sort_values("ts").groupby(keys, as_index=False).tail(1)

# =======================
# Runs / Outputs
# =======================
def create_run(inputs_hash: str, notes: str = "") -> int:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into runs(model_id, model_version, notes, inputs_hash)
                values (%s, %s, %s, %s)
                returning run_id
                """,
                (MODEL_ID, MODEL_VERSION, notes, inputs_hash),
            )
            run_id = int(cur.fetchone()[0])
        conn.commit()
    return run_id

def store_model_outputs(run_id: int, df_outputs: pd.DataFrame):
    if df_outputs.empty:
        return
    rows = []
    for _, r in df_outputs.iterrows():
        rows.append(
            (
                int(run_id),
                str(r["event_id"]),
                str(r["market"]),
                str(r["selection"]),
                None if pd.isna(r["line"]) else float(r["line"]),
                float(r["p_fair"]),
                int(r["fair_price_american"]),
            )
        )
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                insert into model_outputs(run_id, event_id, market, selection, line, p_fair, fair_price_american)
                values (%s,%s,%s,%s,%s,%s,%s)
                """,
                rows,
            )
        conn.commit()

def load_recent_runs(limit: int = 10) -> pd.DataFrame:
    q = """
    select run_id, created_at, model_id, model_version, notes, inputs_hash
    from runs
    order by created_at desc
    limit %s
    """
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (int(limit),))
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=cols)
    if not df.empty:
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    return df

def build_baseline_outputs(df_latest: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (event_id, market, line), g in df_latest.groupby(["event_id", "market", "line"]):
        per_book = []
        for book, gb in g.groupby("book"):
            outs = gb[["selection", "price_american"]].drop_duplicates()
            if len(outs) != 2:
                continue
            sel1, o1 = str(outs.iloc[0]["selection"]), int(outs.iloc[0]["price_american"])
            sel2, o2 = str(outs.iloc[1]["selection"]), int(outs.iloc[1]["price_american"])
            p1, p2 = american_to_prob(o1), american_to_prob(o2)
            p1f, p2f = devig_two_way(p1, p2)
            if pd.isna(p1f) or pd.isna(p2f):
                continue
            per_book.append((sel1, float(p1f)))
            per_book.append((sel2, float(p2f)))

        if not per_book:
            continue

        dfb = pd.DataFrame(per_book, columns=["selection", "p_book_fair"])
        base = dfb.groupby("selection", as_index=False)["p_book_fair"].mean()

        for _, r in base.iterrows():
            p = float(r["p_book_fair"])
            rows.append([str(event_id), str(market), None if pd.isna(line) else float(line), str(r["selection"]), p, prob_to_american(p)])

    return pd.DataFrame(rows, columns=["event_id", "market", "line", "selection", "p_fair", "fair_price_american"])

def insert_bet(
    book: str,
    bet_type: str,
    event_id: str,
    bet_ts: datetime,
    model_id: str,
    model_version: str,
    run_id: int,
    legs_json: dict,
    stake: float,
    price_taken_american: int,
    p_fair_at_bet: float,
    edge_at_bet: float,
):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into bets(book, bet_type, event_id, bet_ts, model_id, model_version, run_id,
                                 legs_json, stake, price_taken_american, p_fair_at_bet, edge_at_bet)
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    str(book),
                    str(bet_type),
                    str(event_id),
                    bet_ts,
                    str(model_id),
                    str(model_version),
                    int(run_id),
                    Json(legs_json),
                    float(stake),
                    int(price_taken_american),
                    float(p_fair_at_bet),
                    float(edge_at_bet),
                ),
            )
        conn.commit()

# =======================
# Boot
# =======================
try:
    init_db()
except Exception as e:
    st.error(f"DB init failed: {e}")
    st.stop()

# =======================
# UI
# =======================
st.title("NBA Edge Dash (V1) — Odds Spine + Runs + Bet Attribution")

col1, col2, col3, col4 = st.columns([1.1, 1, 1, 1.2])

with col1:
    if st.button("1) Update Odds Now", use_container_width=True):
        try:
            data = fetch_odds()
            n_events, n_rows = 0, 0
            with db_conn() as conn:
                with conn.cursor() as cur:
                    for ev in data:
                        event_id = ev.get("id")
                        home = ev.get("home_team")
                        away = ev.get("away_team")
                        start_time = ev.get("commence_time")
                        if not all([event_id, home, away, start_time]):
                            continue

                        cur.execute(
                            """
                            insert into events(event_id, league, home_team, away_team, start_time)
                            values (%s,'NBA',%s,%s,%s)
                            on conflict (event_id) do update set
                              home_team=excluded.home_team,
                              away_team=excluded.away_team,
                              start_time=excluded.start_time
                            """,
                            (event_id, home, away, start_time),
                        )
                        n_events += 1

                        for bm in ev.get("bookmakers", []):
                            book = normalize_book(bm)
                            for mkt in bm.get("markets", []):
                                mk = mkt.get("key")
                                if mk not in ["h2h", "spreads", "totals"]:
                                    continue
                                for out in mkt.get("outcomes", []):
                                    name = out.get("name")
                                    price = out.get("price")
                                    point = out.get("point", None)
                                    if name is None or price is None:
                                        continue
                                    cur.execute(
                                        """
                                        insert into odds_snapshots(event_id, book, market, selection, line, price_american)
                                        values (%s,%s,%s,%s,%s,%s)
                                        """,
                                        (event_id, book, mk, name, point, int(price)),
                                    )
                                    n_rows += 1
                conn.commit()
            st.success(f"Saved {n_rows} odds rows across {n_events} events.")
        except Exception as e:
            st.error(f"Odds update failed: {e}")

with col2:
    lookback_hours = st.number_input("Lookback (hours)", min_value=1, max_value=168, value=6, step=1)

with col3:
    target_books = st.multiselect("Target books", options=TARGET_BOOKS, default=["betonlineag", "caesars"])

with col4:
    if st.button("2) Generate Baseline Run", use_container_width=True):
        try:
            df = load_latest_snapshots(int(lookback_hours))
            if df.empty:
                st.warning("No snapshots yet. Click Update Odds Now first.")
            else:
                dfL = latest_per_key(df)
                h = hashlib.sha256(
                    pd.util.hash_pandas_object(
                        dfL[["event_id", "book", "market", "selection", "line", "price_american"]],
                        index=False
                    ).values.tobytes()
                ).hexdigest()
                run_id = create_run(inputs_hash=h, notes=f"Baseline devig from last {lookback_hours}h")
                outputs = build_baseline_outputs(dfL)
                store_model_outputs(run_id, outputs)
                st.success(f"Created run_id={run_id} and stored {len(outputs)} baseline outputs.")
        except Exception as e:
            st.error(f"Run generation failed: {e}")

df = load_latest_snapshots(int(lookback_hours))
if df.empty:
    st.info("No odds snapshots yet. Click **Update Odds Now**.")
    st.stop()

dfL = latest_per_key(df)
st.subheader("Latest Odds (latest per key)")
st.dataframe(dfL.sort_values(["start_time", "event_id", "market", "book"]), use_container_width=True, height=320)

runs_df = load_recent_runs(limit=10)
st.subheader("Latest Run Outputs (Baseline)")
if runs_df.empty:
    st.info("No runs yet. Click **Generate Baseline Run**.")
    st.stop()

latest_run = int(runs_df.iloc[0]["run_id"])
q_out = """
select mo.run_id, mo.event_id, e.start_time, e.away_team, e.home_team,
       mo.market, mo.line, mo.selection, mo.p_fair, mo.fair_price_american
from model_outputs mo
join events e on e.event_id = mo.event_id
where mo.run_id = %s
order by e.start_time, mo.event_id, mo.market, mo.line, mo.selection
"""
with db_conn() as conn:
    with conn.cursor() as cur:
        cur.execute(q_out, (latest_run,))
        cols = [d[0] for d in cur.description]
        out_df = pd.DataFrame(cur.fetchall(), columns=cols)

if out_df.empty:
    st.warning("Latest run has no outputs yet (need enough 2-way markets).")
    st.stop()

out_df["start_time"] = pd.to_datetime(out_df["start_time"], utc=True)
best_prices = dfL[dfL["book"].isin(target_books)].copy()
bp = best_prices.groupby(["event_id", "market", "line", "selection", "book"], as_index=False)["price_american"].max()
merged = out_df.merge(bp, on=["event_id", "market", "line", "selection"], how="left")
st.dataframe(
    merged[["start_time", "away_team", "home_team", "market", "line", "selection", "p_fair", "fair_price_american", "book", "price_american"]],
    use_container_width=True,
    height=360,
)

with st.expander("DB Diagnostics (safe)"):
    try:
        cfg, ipv4 = _db_plan()
        st.write({
            "DATABASE_URL_present": bool(DATABASE_URL_RAW),
            "parsed_host": cfg["host"],
            "forced_ipv4": ipv4,
            "sslmode_in_url": "sslmode=" in cfg["url"],
        })
    except Exception as e:
        st.error(str(e))

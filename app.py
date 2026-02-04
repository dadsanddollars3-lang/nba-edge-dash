import os
import hashlib
from datetime import datetime, timezone

import requests
import pandas as pd
import psycopg
from psycopg.types.json import Json
import streamlit as st
import socket
import ipaddress

st.set_page_config(page_title="NBA Edge Dash", layout="wide")

# -----------------------
# Secrets / Config
# -----------------------
def S(key: str, default=None):
    return st.secrets.get(key, os.environ.get(key, default))

ODDS_API_KEY = S("ODDS_API_KEY")

DB_HOST = S("DB_HOST")
DB_HOSTADDR = S("DB_HOSTADDR")  # optional: hard-force IPv4 literal if you ever need it
DB_NAME = S("DB_NAME", "postgres")
DB_USER = S("DB_USER", "postgres")
DB_PASSWORD = S("DB_PASSWORD")
DB_PORT = int(S("DB_PORT", 5432))

SPORT_KEY = "basketball_nba"
REGIONS = "us"
MARKETS = "h2h,spreads,totals"
ODDS_FORMAT = "american"

TARGET_BOOKS = ["betonlineag", "caesars"]

MODEL_ID = "BASELINE"
MODEL_VERSION = S("MODEL_VERSION", "v1")

# -----------------------
# DB helpers
# -----------------------
def _is_ipv4_literal(x: str) -> bool:
    try:
        return isinstance(ipaddress.ip_address(x), ipaddress.IPv4Address)
    except Exception:
        return False

def _doh_lookup_a(host: str) -> str | None:
    """
    DNS-over-HTTPS A-record lookup (IPv4) using public resolvers.
    Returns first IPv4 if present, else None.
    """
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
            answers = j.get("Answer") or []
            for a in answers:
                data = a.get("data")
                if data and _is_ipv4_literal(data):
                    return data
        except Exception:
            continue
    return None

def _resolve_ipv4(host: str) -> str | None:
    """
    Returns an IPv4 for host if any exists.
    We *must* do this because Streamlit's runtime often can't route IPv6,
    and Supabase/Pooler hostnames frequently resolve to AAAA first.
    """
    if not host:
        return None

    # If user explicitly provided an IPv4 literal in DB_HOST, just use it.
    if _is_ipv4_literal(host):
        return host

    # If user provided an explicit hostaddr override, use it.
    if DB_HOSTADDR and _is_ipv4_literal(DB_HOSTADDR):
        return DB_HOSTADDR

    # Try system resolver for A records only.
    try:
        infos = socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)
        if infos:
            return infos[0][4][0]
    except Exception:
        pass

    # Fall back to DNS-over-HTTPS A lookup
    return _doh_lookup_a(host)

def db_conn():
    if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
        raise RuntimeError("Missing DB credentials (DB_HOST/DB_USER/DB_PASSWORD/DB_NAME). Check Streamlit secrets.")

    ipv4 = _resolve_ipv4(DB_HOST)

    # We keep 'host' for TLS/SNI; we set 'hostaddr' to force the IPv4 socket.
    conn_kwargs = dict(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        sslmode="require",
        connect_timeout=12,
    )

    if ipv4:
        conn_kwargs["hostaddr"] = ipv4
    else:
        # If we cannot find an A-record at all, this will almost certainly fail in Streamlit.
        # Raise a loud, clear error instead of mysterious psycopg IPv6 failures.
        raise RuntimeError(
            f"Could not resolve an IPv4 address (A record) for DB_HOST='{DB_HOST}'. "
            f"Streamlit runtime here cannot use IPv6. Use an IPv4-capable host or set DB_HOSTADDR to an IPv4 literal."
        )

    return psycopg.connect(**conn_kwargs)

def run_sql(sql: str, params=None):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
        conn.commit()

def init_db():
    """
    Create tables if they don't exist.
    This keeps your app from dying on a fresh DB.
    """
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

# -----------------------
# Odds API
# -----------------------
def fetch_odds():
    if not ODDS_API_KEY:
        raise RuntimeError("Missing ODDS_API_KEY")
    url = f"https://api.the-odds-api.com/v4/sports/{SPORT_KEY}/odds"
    params = {
        "regions": REGIONS,
        "markets": MARKETS,
        "oddsFormat": ODDS_FORMAT,
        "apiKey": ODDS_API_KEY,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def normalize_book(bookmaker: dict) -> str:
    k = (bookmaker.get("key") or "").strip().lower()
    t = (bookmaker.get("title") or "").strip().lower()
    if "caesars" in t:
        return "caesars"
    return k or t.replace(" ", "_")

# -----------------------
# Odds math
# -----------------------
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
        return int(round(-100 * p / (1 - p)))
    return int(round(100 * (1 - p) / p))

# -----------------------
# Load latest snapshots
# -----------------------
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

# -----------------------
# Runs
# -----------------------
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
        rows.append((
            run_id,
            r["event_id"],
            r["market"],
            r["selection"],
            None if pd.isna(r["line"]) else float(r["line"]),
            float(r["p_fair"]),
            int(r["fair_price_american"]),
        ))

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                insert into model_outputs(run_id, event_id, market, selection, line, p_fair, fair_price_american)
                values (%s,%s,%s,%s,%s,%s,%s)
                """,
                rows
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

# -----------------------
# Baseline model
# -----------------------
def build_baseline_outputs(df_latest: pd.DataFrame) -> pd.DataFrame:
    rows = []
    group_cols = ["event_id", "market", "line"]
    for (event_id, market, line), g in df_latest.groupby(group_cols):
        per_book = []
        for book, gb in g.groupby("book"):
            outs = gb[["selection", "price_american"]].drop_duplicates()
            if len(outs) != 2:
                continue
            sel1, o1 = outs.iloc[0]["selection"], int(outs.iloc[0]["price_american"])
            sel2, o2 = outs.iloc[1]["selection"], int(outs.iloc[1]["price_american"])
            p1, p2 = american_to_prob(o1), american_to_prob(o2)
            p1f, p2f = devig_two_way(p1, p2)
            per_book.append((sel1, p1f))
            per_book.append((sel2, p2f))

        if not per_book:
            continue

        dfb = pd.DataFrame(per_book, columns=["selection", "p_book_fair"])
        base = dfb.groupby("selection", as_index=False)["p_book_fair"].mean()
        for _, r in base.iterrows():
            p = float(r["p_book_fair"])
            rows.append([event_id, market, line, r["selection"], p, prob_to_american(p)])

    return pd.DataFrame(rows, columns=["event_id", "market", "line", "selection", "p_fair", "fair_price_american"])

# -----------------------
# Bet logging
# -----------------------
def insert_bet(book: str, bet_type: str, event_id: str, bet_ts: datetime,
               model_id: str, model_version: str, run_id: int,
               legs_json: dict, stake: float, price_taken_american: int,
               p_fair_at_bet: float, edge_at_bet: float):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into bets(book, bet_type, event_id, bet_ts, model_id, model_version, run_id,
                                 legs_json, stake, price_taken_american, p_fair_at_bet, edge_at_bet)
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    book,
                    bet_type,
                    event_id,
                    bet_ts.isoformat(),
                    model_id,
                    model_version,
                    int(run_id),
                    Json(legs_json),
                    float(stake),
                    int(price_taken_american),
                    float(p_fair_at_bet),
                    float(edge_at_bet),
                ),
            )
        conn.commit()

# -----------------------
# Boot: init DB
# -----------------------
try:
    init_db()
except Exception as e:
    st.error(f"DB init failed: {e}")
    st.stop()

# -----------------------
# UI
# -----------------------
st.title("NBA Edge Dash (V1) — Odds Spine + Runs + Bet Attribution")
st.caption("V1 is the plumbing: odds snapshots, baseline devig run outputs, and bet logging with model/run tags. Your quantile + simulation models come next.")

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

# Data display
df = load_latest_snapshots(int(lookback_hours))
if df.empty:
    st.info("No odds snapshots yet. Click **Update Odds Now**.")
    st.stop()

dfL = latest_per_key(df)

st.subheader("Latest Odds (raw snapshots, latest per key)")
st.dataframe(dfL.sort_values(["start_time", "event_id", "market", "book"]), use_container_width=True, height=320)

st.subheader("Latest Run Outputs (Baseline for now)")
runs_df = load_recent_runs(limit=10)
if runs_df.empty:
    st.info("No runs yet. Click **Generate Baseline Run**.")
    st.stop()

latest_run = int(runs_df.iloc[0]["run_id"])
st.caption(f"Showing outputs for latest run_id={latest_run}. (In V2, this becomes your quantile/simulation model run.)")

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

out_df["start_time"] = pd.to_datetime(out_df["start_time"], utc=True)

if out_df.empty:
    st.warning("Latest run has no outputs yet (need enough 2-way markets).")
    st.stop()

best_prices = dfL[dfL["book"].isin(target_books)].copy()
bp = best_prices.groupby(["event_id", "market", "line", "selection", "book"], as_index=False)["price_american"].max()
merged = out_df.merge(bp, on=["event_id", "market", "line", "selection"], how="left")

st.dataframe(
    merged[["start_time", "away_team", "home_team", "market", "line", "selection", "p_fair", "fair_price_american", "book", "price_american"]],
    use_container_width=True,
    height=360,
)

st.subheader("Quick Log Bet (auto-tags model/run)")
st.caption("Use this when you click a recommendation and want it attributed automatically. Works for on-the-fly logging too.")

with st.form("log_bet_form"):
    cols = st.columns([1.2, 1, 1, 1, 1])
    event_id = cols[0].selectbox("Event", options=sorted(out_df["event_id"].unique()))
    book = cols[1].selectbox("Book", options=target_books)
    bet_type = cols[2].selectbox("Bet type", options=["straight", "sgp_2leg"])
    stake = cols[3].number_input("Stake", min_value=1.0, value=10.0, step=1.0)
    bet_ts = cols[4].text_input("Bet time (optional)", value="now")

    ev_rows = out_df[out_df["event_id"] == event_id].copy()
    labels = list(ev_rows.apply(lambda r: f"{r['market']} | line={r['line']} | {r['selection']} | p={r['p_fair']:.3f}", axis=1))
    leg_pick = st.selectbox("Pick a market/selection (from latest run outputs) to prefill leg", options=labels)
    picked = ev_rows.iloc[labels.index(leg_pick)]

    price_taken = st.number_input("Price taken (American)", value=-110, step=1)

    submit = st.form_submit_button("Save Bet")
    if submit:
        if bet_ts.strip().lower() == "now" or bet_ts.strip() == "":
            bt = datetime.now(timezone.utc)
        else:
            try:
                bt = datetime.fromisoformat(bet_ts).astimezone(timezone.utc)
            except Exception:
                bt = datetime.now(timezone.utc)

        legs = [{
            "market": str(picked["market"]),
            "selection": str(picked["selection"]),
            "line": None if pd.isna(picked["line"]) else float(picked["line"]),
        }]

        p_fair = float(picked["p_fair"])
        edge = 0.0

        insert_bet(
            book=book,
            bet_type=bet_type,
            event_id=event_id,
            bet_ts=bt,
            model_id=MODEL_ID,
            model_version=MODEL_VERSION,
            run_id=latest_run,
            legs_json={"legs": legs},
            stake=float(stake),
            price_taken_american=int(price_taken),
            p_fair_at_bet=p_fair,
            edge_at_bet=edge
        )
        st.success("Bet logged with automatic model/run attribution.")

# app.py
# NBA Edge Dash: Odds snapshots + baseline devig runs + bet logging
# Fixes:
# 1) Forces IPv4 sockets in Streamlit (common IPv6 routing failure)
# 2) Correct parameterized SQL usage in load_latest_snapshots
# 3) Adds safer connection handling and clearer error messages

import os
import hashlib
import socket
import ipaddress
from datetime import datetime, timezone

import requests
import pandas as pd
import psycopg
from psycopg.types.json import Json
import streamlit as st

st.set_page_config(page_title="NBA Edge Dash", layout="wide")

# -----------------------
# Secrets / Config
# -----------------------
# .streamlit/secrets.toml

ODDS_API_KEY = "YOUR_ODDS_API_KEY"

DB_HOST = "aws-0-us-west-2.pooler.supabase.com"
DB_PORT = "5432"
DB_NAME = "postgres"
DB_USER = "postgres.koorfwagibchkrhcdqhi"
DB_PASSWORD = "PASTE_YOUR_ROTATED_PASSWORD_HERE"

# Optional: only set this if IPv4 resolution still fails in Streamlit
# DB_HOSTADDR = "A.B.C.D"

# -----------------------
# DB helpers (IPv4 enforced)
# -----------------------
def _is_ipv4_literal(x: str) -> bool:
    try:
        return isinstance(ipaddress.ip_address(x), ipaddress.IPv4Address)
    except Exception:
        return False

def _doh_lookup_a(host: str) -> str | None:
    """
    DNS-over-HTTPS A record lookup for an IPv4 address.
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
    Return an IPv4 address for host if available.
    We do this because Streamlit hosting often cannot route IPv6.
    """
    if not host:
        return None

    # If DB_HOST itself is an IPv4 literal
    if _is_ipv4_literal(host):
        return host

    # If override is provided
    if DB_HOSTADDR and _is_ipv4_literal(DB_HOSTADDR):
        return DB_HOSTADDR

    # System resolver for A records only
    try:
        infos = socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)
        if infos:
            return infos[0][4][0]
    except Exception:
        pass

    # DoH fallback
    return _doh_lookup_a(host)

def db_conn():
    if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
        raise RuntimeError(
            "Missing DB credentials. Require DB_HOST, DB_USER, DB_PASSWORD, DB_NAME in Streamlit secrets or env."
        )

    ipv4 = _resolve_ipv4(DB_HOST)
    if not ipv4:
        raise RuntimeError(
            f"Could not resolve an IPv4 A record for DB_HOST='{DB_HOST}'. "
            "This Streamlit runtime cannot use IPv6 routes. "
            "Set DB_HOSTADDR to an IPv4 literal or use an IPv4-capable pooler host."
        )

    # Keep 'host' for TLS SNI, but force the socket to IPv4 via hostaddr
    conn_kwargs = dict(
        host=DB_HOST,
        hostaddr=ipv4,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        sslmode="require",
        connect_timeout=12,
    )
    return psycopg.connect(**conn_kwargs)

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

# -----------------------
# Odds API
# -----------------------
def fetch_odds():
    if not ODDS_API_KEY:
        raise RuntimeError("Missing ODDS_API_KEY in secrets or env.")
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
        return int(round(-100.0 * p / (1.0 - p)))
    return int(round(100.0 * (1.0 - p) / p))

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
    df["price_american"] = pd.to_numeric(df["price_american"], errors="coerce").astype("Int64")
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

# -----------------------
# Baseline model
# -----------------------
def build_baseline_outputs(df_latest: pd.DataFrame) -> pd.DataFrame:
    """
    For each (event_id, market, line), devig each book's 2-way market and average fair probs across books.
    """
    rows = []
    group_cols = ["event_id", "market", "line"]

    for (event_id, market, line), g in df_latest.groupby(group_cols):
        per_book = []
        for book, gb in g.groupby("book"):
            outs = gb[["selection", "price_american"]].drop_duplicates()
            if len(outs) != 2:
                continue

            sel1 = str(outs.iloc[0]["selection"])
            sel2 = str(outs.iloc[1]["selection"])
            o1 = int(outs.iloc[0]["price_american"])
            o2 = int(outs.iloc[1]["price_american"])

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
            rows.append(
                [
                    str(event_id),
                    str(market),
                    None if pd.isna(line) else float(line),
                    str(r["selection"]),
                    p,
                    prob_to_american(p),
                ]
            )

    return pd.DataFrame(
        rows,
        columns=["event_id", "market", "line", "selection", "p_fair", "fair_price_american"],
    )

# -----------------------
# Bet logging
# -----------------------
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
                insert into bets(
                    book, bet_type, event_id, bet_ts, model_id, model_version, run_id,
                    legs_json, stake, price_taken_american, p_fair_at_bet, edge_at_bet
                )
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    str(book),
                    str(bet_type),
                    str(event_id),
                    bet_ts,  # pass datetime directly
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
st.title("NBA Edge Dash (V1)")
st.caption("Odds snapshots, baseline devig run outputs, and bet logging with model/run tags.")

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
                        index=False,
                    ).values.tobytes()
                ).hexdigest()

                run_id = create_run(inputs_hash=h, notes=f"Baseline devig from last {lookback_hours}h")
                outputs = build_baseline_outputs(dfL)
                store_model_outputs(run_id, outputs)

                st.success(f"Created run_id={run_id} and stored {len(outputs)} baseline outputs.")
        except Exception as e:
            st.error(f"Run generation failed: {e}")

# -----------------------
# Data display
# -----------------------
df = load_latest_snapshots(int(lookback_hours))
if df.empty:
    st.info("No odds snapshots yet. Click Update Odds Now.")
    st.stop()

dfL = latest_per_key(df)

st.subheader("Latest Odds (latest per key)")
st.dataframe(
    dfL.sort_values(["start_time", "event_id", "market", "book"]),
    use_container_width=True,
    height=320,
)

st.subheader("Latest Run Outputs (Baseline)")
runs_df = load_recent_runs(limit=10)
if runs_df.empty:
    st.info("No runs yet. Click Generate Baseline Run.")
    st.stop()

latest_run = int(runs_df.iloc[0]["run_id"])
st.caption(f"Showing outputs for latest run_id={latest_run}.")

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
    st.warning("Latest run has no outputs yet. Need enough 2-way markets per event.")
    st.stop()

out_df["start_time"] = pd.to_datetime(out_df["start_time"], utc=True)
out_df["line"] = pd.to_numeric(out_df["line"], errors="coerce")

best_prices = dfL[dfL["book"].isin(target_books)].copy()
bp = best_prices.groupby(["event_id", "market", "line", "selection", "book"], as_index=False)["price_american"].max()
merged = out_df.merge(bp, on=["event_id", "market", "line", "selection"], how="left")

st.dataframe(
    merged[
        [
            "start_time",
            "away_team",
            "home_team",
            "market",
            "line",
            "selection",
            "p_fair",
            "fair_price_american",
            "book",
            "price_american",
        ]
    ],
    use_container_width=True,
    height=360,
)

# -----------------------
# Quick Log Bet
# -----------------------
st.subheader("Quick Log Bet (auto-tags model/run)")
st.caption("Select an output row to prefill, then log your bet with run attribution.")

with st.form("log_bet_form"):
    c = st.columns([1.2, 1, 1, 1, 1])

    event_id = c[0].selectbox("Event", options=sorted(out_df["event_id"].unique()))
    book = c[1].selectbox("Book", options=target_books)
    bet_type = c[2].selectbox("Bet type", options=["straight", "sgp_2leg"])
    stake = c[3].number_input("Stake", min_value=1.0, value=10.0, step=1.0)
    bet_ts = c[4].text_input("Bet time (optional ISO8601 or 'now')", value="now")

    ev_rows = out_df[out_df["event_id"] == event_id].copy()
    labels = list(
        ev_rows.apply(
            lambda r: f"{r['market']} | line={r['line']} | {r['selection']} | p={float(r['p_fair']):.3f}",
            axis=1,
        )
    )

    leg_pick = st.selectbox("Pick a market/selection (from latest run outputs)", options=labels)
    picked = ev_rows.iloc[labels.index(leg_pick)]

    price_taken = st.number_input("Price taken (American)", value=-110, step=1)

    submit = st.form_submit_button("Save Bet")
    if submit:
        if bet_ts.strip().lower() in ["now", ""]:
            bt = datetime.now(timezone.utc)
        else:
            try:
                # fromisoformat can parse "YYYY-MM-DDTHH:MM:SS+00:00"
                bt = datetime.fromisoformat(bet_ts)
                if bt.tzinfo is None:
                    bt = bt.replace(tzinfo=timezone.utc)
                else:
                    bt = bt.astimezone(timezone.utc)
            except Exception:
                bt = datetime.now(timezone.utc)

        legs = [
            {
                "market": str(picked["market"]),
                "selection": str(picked["selection"]),
                "line": None if pd.isna(picked["line"]) else float(picked["line"]),
            }
        ]

        p_fair = float(picked["p_fair"])
        edge = 0.0  # placeholder, compute later if you want

        try:
            insert_bet(
                book=book,
                bet_type=bet_type,
                event_id=str(event_id),
                bet_ts=bt,
                model_id=MODEL_ID,
                model_version=MODEL_VERSION,
                run_id=latest_run,
                legs_json={"legs": legs},
                stake=float(stake),
                price_taken_american=int(price_taken),
                p_fair_at_bet=p_fair,
                edge_at_bet=edge,
            )
            st.success("Bet logged with automatic model/run attribution.")
        except Exception as e:
            st.error(f"Bet insert failed: {e}")

# -----------------------
# Footer: Diagnostics
# -----------------------
with st.expander("DB Diagnostic (shows resolved IPv4)"):
    try:
        ipv4 = _resolve_ipv4(DB_HOST) if DB_HOST else None
        st.write(
            {
                "DB_HOST": DB_HOST,
                "DB_HOSTADDR_override": DB_HOSTADDR,
                "resolved_ipv4": ipv4,
                "DB_NAME": DB_NAME,
                "DB_USER": DB_USER,
                "DB_PORT": DB_PORT,
            }
        )
    except Exception as e:
        st.error(str(e))

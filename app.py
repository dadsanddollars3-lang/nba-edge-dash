import os
import hashlib
from datetime import datetime, timezone, timedelta

import requests
import pandas as pd
import psycopg
import streamlit as st

st.set_page_config(page_title="NBA Edge Dash", layout="wide")

# -----------------------
# Secrets / Config
# -----------------------
def S(key: str, default=None):
    return st.secrets.get(key, os.environ.get(key, default))

ODDS_API_KEY = S("ODDS_API_KEY")
DB_HOST = S("DB_HOST")
DB_NAME = S("DB_NAME", "postgres")
DB_USER = S("DB_USER", "postgres")
DB_PASSWORD = S("DB_PASSWORD")
DB_PORT = int(S("DB_PORT", 5432))

SPORT_KEY = "basketball_nba"
REGIONS = "us"
MARKETS = "h2h,spreads,totals"
ODDS_FORMAT = "american"

# You can add more later
TARGET_BOOKS = ["betonlineag", "caesars"]

MODEL_ID = "BASELINE"  # V1 baseline model (devig reference)
MODEL_VERSION = S("MODEL_VERSION", "v1")  # set to git hash later if you want

# -----------------------
# DB helpers
# -----------------------
def db_conn():
    return psycopg.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
    )

def run_sql(sql: str, params=None):
    conn = db_conn()
    cur = conn.cursor()
    cur.execute(sql, params or ())
    conn.commit()
    cur.close()
    conn.close()

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
    q = f"""
    select s.ts, e.start_time, e.home_team, e.away_team,
           s.event_id, s.book, s.market, s.selection, s.line, s.price_american
    from odds_snapshots s
    join events e on e.event_id = s.event_id
    where s.ts >= now() - interval '{lookback_hours} hours'
    order by s.ts desc
    limit 30000
    """
    conn = db_conn()
    with conn.cursor() as cur:
        cur.execute(q)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    conn.close()

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
# Create a run + store model outputs
# -----------------------
def create_run(inputs_hash: str, notes: str = "") -> int:
    conn = db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        insert into runs(model_id, model_version, notes, inputs_hash)
        values (%s, %s, %s, %s)
        returning run_id
        """,
        (MODEL_ID, MODEL_VERSION, notes, inputs_hash),
    )
    run_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return int(run_id)

def store_model_outputs(run_id: int, df_outputs: pd.DataFrame):
    if df_outputs.empty:
        return
    conn = db_conn()
    cur = conn.cursor()
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
    cur.executemany(
        """
        insert into model_outputs(run_id, event_id, market, selection, line, p_fair, fair_price_american)
        values (%s,%s,%s,%s,%s,%s,%s)
        """,
        rows
    )
    conn.commit()
    cur.close()
    conn.close()

# -----------------------
# Baseline "model": devig per-book and average devig across books
# (This is only a reference until your quantile model replaces it.)
# -----------------------
def build_baseline_outputs(df_latest: pd.DataFrame) -> pd.DataFrame:
    # For each event/market/line, compute devig probs per book if exactly 2 outcomes,
    # then average devig probs across books to create baseline p_fair.
    rows = []
    group_cols = ["event_id", "market", "line"]
    for (event_id, market, line), g in df_latest.groupby(group_cols):
        # per book
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
    conn = db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        insert into bets(book, bet_type, event_id, bet_ts, model_id, model_version, run_id,
                         legs_json, stake, price_taken_american, p_fair_at_bet, edge_at_bet)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            book, bet_type, event_id,
            bet_ts.isoformat(),
            model_id, model_version, run_id,
            legs_json,
            stake, int(price_taken_american),
            float(p_fair_at_bet), float(edge_at_bet),
        ),
    )
    conn.commit()
    cur.close()
    conn.close()



# -----------------------
# UI
# -----------------------
st.title("NBA Edge Dash (V1) — Odds Spine + Runs + Bet Attribution")
st.caption("V1 is the plumbing: odds snapshots, baseline devig run outputs, and bet logging with model/run tags. Your quantile + simulation models come next.")

# Controls
col1, col2, col3, col4 = st.columns([1.1, 1, 1, 1.2])
with col1:
    if st.button("1) Update Odds Now", use_container_width=True):
        try:
            data = fetch_odds()
            conn = db_conn()
            cur = conn.cursor()
            n_events, n_rows = 0, 0
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
            cur.close()
            conn.close()
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
                # hash inputs for traceability
                h = hashlib.sha256(pd.util.hash_pandas_object(dfL[["event_id","book","market","selection","line","price_american"]], index=False).values.tobytes()).hexdigest()
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
st.dataframe(dfL.sort_values(["start_time","event_id","market","book"]), use_container_width=True, height=320)

# Show latest run outputs for decision + quick log
st.subheader("Latest Run Outputs (Baseline for now)")
runs_df = load_recent_runs(limit=10)
if runs_df.empty:
    st.info("No runs yet. Click **Generate Baseline Run**.")
    st.stop()

latest_run = int(runs_df.iloc[0]["run_id"])
st.caption(f"Showing outputs for latest run_id={latest_run}. (In V2, this becomes your quantile/simulation model run.)")

q_out = f"""
select mo.run_id, mo.event_id, e.start_time, e.away_team, e.home_team,
       mo.market, mo.line, mo.selection, mo.p_fair, mo.fair_price_american
from model_outputs mo
join events e on e.event_id = mo.event_id
where mo.run_id = {latest_run}
order by e.start_time, mo.event_id, mo.market, mo.line, mo.selection
"""
conn = db_conn()
with conn.cursor() as cur:
    cur.execute(q_out)
    cols = [d[0] for d in cur.description]
    out_df = pd.DataFrame(cur.fetchall(), columns=cols)
conn.close()
out_df["start_time"] = pd.to_datetime(out_df["start_time"], utc=True)

if out_df.empty:
    st.warning("Latest run has no outputs yet (need enough 2-way markets).")
    st.stop()

# Join with best available target book prices so you can act
best_prices = dfL[dfL["book"].isin(target_books)].copy()
# For each event/market/line/selection pick the best price for the bettor (higher is better for positive odds, closer to 0 is better for negative)
# We'll just display all and you can pick; in V2 we'll compute EV properly.
bp = best_prices.groupby(["event_id","market","line","selection","book"], as_index=False)["price_american"].max()
merged = out_df.merge(bp, on=["event_id","market","line","selection"], how="left")

st.dataframe(
    merged[["start_time","away_team","home_team","market","line","selection","p_fair","fair_price_american","book","price_american"]],
    use_container_width=True,
    height=360,
)

# -----------------------
# Quick Log Bet (auto tags)
# -----------------------
st.subheader("Quick Log Bet (auto-tags model/run)")
st.caption("Use this when you click a recommendation and want it attributed automatically. Works for on-the-fly logging too.")

with st.form("log_bet_form"):
    cols = st.columns([1.2,1,1,1,1])
    event_id = cols[0].selectbox("Event", options=sorted(out_df["event_id"].unique()))
    book = cols[1].selectbox("Book", options=target_books)
    bet_type = cols[2].selectbox("Bet type", options=["straight","sgp_2leg"])
    stake = cols[3].number_input("Stake", min_value=1.0, value=10.0, step=1.0)
    bet_ts = cols[4].text_input("Bet time (optional)", value="now")

    # pick a leg from outputs for convenience
    ev_rows = out_df[out_df["event_id"] == event_id].copy()
    leg_pick = st.selectbox(
        "Pick a market/selection (from latest run outputs) to prefill leg",
        options=list(ev_rows.apply(lambda r: f"{r['market']} | line={r['line']} | {r['selection']} | p={r['p_fair']:.3f}", axis=1))
    )
    picked = ev_rows.iloc[list(ev_rows.apply(lambda r: f"{r['market']} | line={r['line']} | {r['selection']} | p={r['p_fair']:.3f}", axis=1)).index(leg_pick)]

    price_taken = st.number_input("Price taken (American)", value=-110, step=1)

    submit = st.form_submit_button("Save Bet")
    if submit:
        # parse bet time
        if bet_ts.strip().lower() == "now" or bet_ts.strip() == "":
            bt = datetime.now(timezone.utc)
        else:
            # expects ISO, but we keep it forgiving: try parse as YYYY-MM-DD HH:MM
            try:
                bt = datetime.fromisoformat(bet_ts).astimezone(timezone.utc)
            except Exception:
                bt = datetime.now(timezone.utc)

        # Build legs JSON (V1: store one leg for straight; V2 SGP stores two)
        legs = [{
            "market": str(picked["market"]),
            "selection": str(picked["selection"]),
            "line": None if pd.isna(picked["line"]) else float(picked["line"]),
        }]

        # Edge placeholder in V1: compare fair price vs taken price in probability space later (V2).
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

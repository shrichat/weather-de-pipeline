import os, math
import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
from dotenv import load_dotenv
from sqlalchemy import create_engine

# ---- env ----
load_dotenv()

def _engine():
    host = os.getenv("RDS_HOST", "localhost")
    port = os.getenv("RDS_PORT", "5432")
    db   = os.getenv("RDS_DB", "airflow")
    user = os.getenv("RDS_USER", "airflow")
    pwd  = os.getenv("RDS_PASSWORD", "airflow")
    ssl  = os.getenv("RDS_SSLMODE", "disable")
    uri = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}?sslmode={ssl}"
    return create_engine(uri)

# ---- data ----
QUERY = """
SELECT observed_at, city, temp_c, humidity, wind_speed, weather_desc
FROM weather_observations
WHERE observed_at > NOW() - INTERVAL '7 days'
ORDER BY observed_at
"""

def fetch_df() -> pd.DataFrame:
    try:
        with _engine().connect() as conn:
            df = pd.read_sql(QUERY, conn, parse_dates=["observed_at"])
        return df
    except Exception:
        return pd.DataFrame(columns=["observed_at","city","temp_c","humidity","wind_speed","weather_desc"])

# ---- helpers for nicer axes with tiny datasets ----
def padded_range(series: pd.Series, pad_min=1.0, pad_frac=0.15):
    if series is None or series.empty:
        return [0, 1]
    lo, hi = float(series.min()), float(series.max())
    span = max(hi - lo, 1e-9)
    pad = max(pad_min, span * pad_frac)
    return [math.floor(lo - pad), math.ceil(hi + pad)]

def time_range(ts: pd.Series, minutes_each_side=10):
    if ts is None or ts.empty:
        return None
    if len(ts) == 1:
        t = pd.to_datetime(ts.iloc[0])
        return [t - pd.Timedelta(minutes=minutes_each_side),
                t + pd.Timedelta(minutes=minutes_each_side)]
    return None  # let Plotly autoscale when we have multiple points

def build_figs(df: pd.DataFrame):
    # Temperature over time
    temp_fig = px.line(df, x="observed_at", y="temp_c", color="city", markers=True)
    xr = time_range(df["observed_at"])
    if xr: temp_fig.update_xaxes(range=xr)
    temp_fig.update_yaxes(range=padded_range(df["temp_c"]))
    temp_fig.update_layout(margin=dict(l=30,r=10,t=10,b=30), legend_title_text="city")

    # Humidity vs Wind
    scat_fig = px.scatter(df, x="humidity", y="wind_speed", color="city")
    scat_fig.update_xaxes(range=padded_range(df["humidity"], pad_min=2))
    scat_fig.update_yaxes(range=padded_range(df["wind_speed"], pad_min=1))
    scat_fig.update_layout(margin=dict(l=30,r=10,t=10,b=30), legend_title_text="city")
    return temp_fig, scat_fig

def kpi_cards(df: pd.DataFrame):
    if df.empty:
        return html.Div("No data yet — trigger the DAGs once.", style={"opacity":0.7})
    last = df.iloc[-1]
    fmt_time = pd.to_datetime(last["observed_at"]).strftime("%b %d, %Y %H:%M UTC")
    card = lambda title, value: html.Div(
        [html.Div(title, style={"fontSize":"12px","opacity":0.7}),
         html.Div(value, style={"fontSize":"22px","fontWeight":"600"})],
        style={"flex":"1","padding":"12px 16px","border":"1px solid #e5e7eb",
               "borderRadius":"12px","background":"#fff","boxShadow":"0 1px 3px rgba(0,0,0,0.06)"}
    )
    return html.Div([
        card("City", last["city"]),
        card("Temperature (°C)", f"{last['temp_c']:.1f}"),
        card("Humidity (%)", f"{last['humidity']}"),
        card("Wind (m/s)", f"{last['wind_speed']:.2f}"),
        card("Condition", str(last.get("weather_desc","-")).title()),
    ], style={"display":"flex","gap":"12px","flexWrap":"wrap"}), fmt_time

# ---- app ----
app = Dash(__name__)
app.title = "Weather Pipeline Dashboard"

df0 = fetch_df()
temp_fig0, scat_fig0 = build_figs(df0)
cards0, updated0 = kpi_cards(df0)

app.layout = html.Div(style={"fontFamily":"Inter, system-ui, sans-serif","padding":"24px","background":"#fafafa"}, children=[
    html.H2("Weather Pipeline Dashboard", style={"margin":"0 0 8px 0"}),
    html.Div(id="last-updated", children=f"Last updated: {updated0 if df0.size else '—'}",
             style={"opacity":0.7,"marginBottom":"16px"}),

    html.Div(id="kpi-cards", children=cards0, style={"marginBottom":"16px"}),

    html.Div([
        html.Div([html.H4("Last 7 Days Temperature", style={"margin":"0 0 8px 0"}),
                  dcc.Graph(id="temp-graph", figure=temp_fig0)], style={"flex":"1","minWidth":"420px"}),

        html.Div([html.H4("Humidity vs Wind", style={"margin":"0 0 8px 0"}),
                  dcc.Graph(id="scat-graph", figure=scat_fig0)], style={"flex":"1","minWidth":"420px"}),
    ], style={"display":"flex","gap":"24px","flexWrap":"wrap"}),

    dcc.Interval(id="tick", interval=60_000, n_intervals=0)  # refresh every 60s
])

# ---- callbacks ----
@app.callback(
    [Output("temp-graph","figure"),
     Output("scat-graph","figure"),
     Output("kpi-cards","children"),
     Output("last-updated","children")],
    Input("tick","n_intervals")
)
def refresh(_):
    df = fetch_df()
    temp_fig, scat_fig = build_figs(df)
    cards, updated = kpi_cards(df)
    return temp_fig, scat_fig, cards, f"Last updated: {updated if df.size else '—'}"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=True)

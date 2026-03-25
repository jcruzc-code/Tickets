from __future__ import annotations

import os
import sqlite3
import tempfile
import unicodedata
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


st.set_page_config(
    page_title="Dashboard RRHH Ticketera",
    page_icon="bar_chart",
    layout="wide",
    initial_sidebar_state="expanded",
)

ANALYSIS_VIEW = "vw_requests_rrhh_analisis"
RAW_VIEW = "vw_requests_detalles_web"
ACTIVE_STATES = {
    "Solicitados",
    "En Busqueda",
    "En Nominas - En Espera",
    "Resuelto",
}
REPO_DB_CANDIDATES = [
    Path(__file__).resolve().parent / "data" / "ticketera.sqlite",
    Path(__file__).resolve().parents[1] / "artifacts" / "ticketera.sqlite",
]


def safe_divide(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def format_number(value: float | int | None, decimals: int = 0) -> str:
    if value is None or pd.isna(value):
        return "-"
    if decimals == 0:
        return f"{value:,.0f}".replace(",", ".")
    return f"{value:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def format_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{value * 100:,.1f}%".replace(",", "X").replace(".", ",").replace("X", ".")


def streamlit_secret(name: str) -> str:
    try:
        value = st.secrets.get(name, "")
    except Exception:
        value = ""
    return str(value).strip()


def normalize_text(value: object) -> str:
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    return " ".join(text.strip().split())


def candidate_db_paths() -> list[Path]:
    raw_candidates: list[str] = []
    secret_path = streamlit_secret("ticketera_db_path")
    env_path = os.getenv("TICKETERA_DB_PATH", "").strip()

    if secret_path:
        raw_candidates.append(secret_path)
    if env_path:
        raw_candidates.append(env_path)

    paths: list[Path] = []
    seen: set[str] = set()

    for raw in raw_candidates:
        candidate = Path(raw)
        if not candidate.is_absolute():
            candidate = Path(__file__).resolve().parents[1] / candidate
        key = str(candidate.resolve()) if candidate.exists() else str(candidate)
        if key not in seen:
            seen.add(key)
            paths.append(candidate)

    for candidate in REPO_DB_CANDIDATES:
        key = str(candidate.resolve()) if candidate.exists() else str(candidate)
        if key not in seen:
            seen.add(key)
            paths.append(candidate)

    return paths


def persist_uploaded_sqlite(uploaded_file) -> Path:
    suffix = Path(uploaded_file.name).suffix or ".sqlite"
    temp_dir = Path(tempfile.gettempdir()) / "ticketera_streamlit"
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_path = temp_dir / f"uploaded_ticketera{suffix}"
    temp_path.write_bytes(uploaded_file.getbuffer())
    return temp_path


def resolve_data_source() -> tuple[Path | None, str]:
    st.sidebar.header("Fuente de datos")

    uploaded_file = st.sidebar.file_uploader(
        "Sube una base SQLite",
        type=["sqlite", "db"],
        help="Util para Streamlit Web cuando no quieres subir la base al repositorio.",
    )
    if uploaded_file is not None:
        db_path = persist_uploaded_sqlite(uploaded_file)
        return db_path, f"Archivo subido: {uploaded_file.name}"

    for candidate in candidate_db_paths():
        if candidate.exists():
            return candidate, f"Archivo local: {candidate}"

    return None, "No se encontro una base SQLite disponible."


@st.cache_data(show_spinner=False)
def load_rrhh_dataset(db_path: str) -> pd.DataFrame:
    connection = sqlite3.connect(db_path)
    try:
        dataframe = pd.read_sql_query(f"SELECT * FROM {ANALYSIS_VIEW}", connection)
    finally:
        connection.close()

    dataframe = dataframe.rename(columns={column: normalize_text(column) for column in dataframe.columns})

    date_columns = [
        "Fecha del ticket",
        "Fecha de procesamiento RRHH",
        "Fecha de reclutamiento",
        "Fecha de cobertura de la posicion",
        "Fecha de Solicitud de Ingreso",
        "Fecha de Fin del Primer Contrato",
        "Fecha de vencimiento",
        "Fecha de Ingreso del Trabajador a Reemplazar",
        "Fecha de Cese o Ultimo Dia de Trabajo del trabajador a reemplazar",
    ]
    for column in date_columns:
        if column in dataframe.columns:
            dataframe[column] = pd.to_datetime(dataframe[column], errors="coerce")

    numeric_columns = [
        "ID Ticket",
        "Cantidad de Colaboradores",
        "Dias desde ticket hasta procesamiento RRHH",
        "Dias desde ticket hasta reclutamiento",
        "Dias desde ticket hasta cobertura",
        "Dias desde solicitud de ingreso hasta cobertura",
        "Dias desde procesamiento RRHH hasta reclutamiento",
        "Dias desde procesamiento RRHH hasta cobertura",
    ]
    for column in numeric_columns:
        if column in dataframe.columns:
            dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")

    dataframe["Mes del ticket"] = dataframe["Fecha del ticket"].dt.to_period("M").astype("string")
    dataframe["Ano del ticket"] = dataframe["Fecha del ticket"].dt.year.astype("Int64")
    dataframe["Pendiente de cobertura"] = (
        dataframe["Posicion cubierta"].fillna("No").map(normalize_text).ne("Si")
    )
    dataframe["Alerta vencido"] = dataframe["Fecha de vencimiento"].notna() & (
        dataframe["Fecha de vencimiento"] < pd.Timestamp.now().normalize()
    ) & dataframe["Pendiente de cobertura"]
    dataframe["Alerta alta prioridad"] = (
        dataframe["Prioridad"].fillna("").str.upper().eq("ALTO") & dataframe["Pendiente de cobertura"]
    )
    dataframe["Alerta sin procesamiento RRHH"] = (
        dataframe["Fecha de procesamiento RRHH"].isna() & dataframe["Pendiente de cobertura"]
    )
    dataframe["Edad ticket abierto"] = (
        pd.Timestamp.now().normalize() - dataframe["Fecha del ticket"]
    ).dt.total_seconds() / 86400
    dataframe["Alerta ticket envejecido"] = dataframe["Pendiente de cobertura"] & (
        dataframe["Edad ticket abierto"] >= 7
    )
    dataframe["Estado simplificado"] = dataframe["Estado"].apply(map_stage)
    return dataframe


def map_stage(value: str | None) -> str:
    normalized = normalize_text(value).lower()
    if "cerrado" in normalized or "cubierto" in normalized:
        return "Cubierto"
    if "nominas" in normalized:
        return "En nominas"
    if "busqueda" in normalized:
        return "En busqueda"
    if "solicit" in normalized:
        return "Solicitado"
    if "resuelto" in normalized:
        return "Resuelto"
    return "Otro"


def apply_filters(dataframe: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Filtros")

    years = sorted(dataframe["Ano del ticket"].dropna().astype(int).unique().tolist())
    selected_years = st.sidebar.multiselect("Ano del ticket", years, default=years)

    locations = sorted(dataframe["Ubicacion de analisis"].dropna().replace("", pd.NA).dropna().unique().tolist())
    selected_locations = st.sidebar.multiselect(
        "Ubicacion de analisis",
        locations,
        default=locations,
    )

    motives = sorted(dataframe["Motivo del ticket"].dropna().replace("", pd.NA).dropna().unique().tolist())
    default_motives = motives[:12]
    selected_motives = st.sidebar.multiselect(
        "Motivo del ticket",
        motives,
        default=default_motives if default_motives else motives,
    )

    stages = sorted(dataframe["Estado simplificado"].dropna().unique().tolist())
    selected_stages = st.sidebar.multiselect("Etapa resumida", stages, default=stages)

    clients = sorted(dataframe["Cliente"].dropna().replace("", pd.NA).dropna().unique().tolist())
    selected_clients = st.sidebar.multiselect("Cliente", clients, default=[])

    filtered = dataframe.copy()
    if selected_years:
        filtered = filtered[filtered["Ano del ticket"].isin(selected_years)]
    if selected_locations:
        filtered = filtered[filtered["Ubicacion de analisis"].isin(selected_locations)]
    if selected_motives:
        filtered = filtered[filtered["Motivo del ticket"].isin(selected_motives)]
    if selected_stages:
        filtered = filtered[filtered["Estado simplificado"].isin(selected_stages)]
    if selected_clients:
        filtered = filtered[filtered["Cliente"].isin(selected_clients)]

    st.sidebar.caption(f"Tickets en el corte: {len(filtered):,}".replace(",", "."))
    return filtered


def build_kpis(dataframe: pd.DataFrame) -> dict[str, float]:
    total = len(dataframe)
    covered = int(dataframe["Posicion cubierta"].fillna("No").map(normalize_text).eq("Si").sum())
    pending = total - covered
    active = int(dataframe["Estado"].fillna("").map(normalize_text).isin(ACTIVE_STATES).sum())
    overdue = int(dataframe["Alerta vencido"].sum())
    high_priority = int(dataframe["Alerta alta prioridad"].sum())
    no_processing = int(dataframe["Alerta sin procesamiento RRHH"].sum())
    aged = int(dataframe["Alerta ticket envejecido"].sum())
    return {
        "total": total,
        "covered": covered,
        "pending": pending,
        "active": active,
        "coverage_rate": safe_divide(covered, total),
        "avg_days_processing": dataframe["Dias desde ticket hasta procesamiento RRHH"].mean(),
        "avg_days_recruitment": dataframe["Dias desde ticket hasta reclutamiento"].mean(),
        "avg_days_coverage": dataframe["Dias desde ticket hasta cobertura"].mean(),
        "overdue": overdue,
        "high_priority": high_priority,
        "no_processing": no_processing,
        "aged": aged,
    }


def render_header() -> None:
    st.title("Dashboard Gerencial RRHH")
    st.caption(
        "Fuente: vista `vw_requests_rrhh_analisis` generada desde la ticketera. "
        "El dashboard resume demanda, cobertura, tiempos y alertas del proceso de reemplazo de personal."
    )


def render_kpi_row(kpis: dict[str, float]) -> None:
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Tickets", format_number(kpis["total"]))
    col2.metric("Posiciones cubiertas", format_number(kpis["covered"]))
    col3.metric("Pendientes", format_number(kpis["pending"]))
    col4.metric("Cobertura", format_pct(kpis["coverage_rate"]))
    col5.metric("Dias ticket -> RRHH", format_number(kpis["avg_days_processing"], 1))
    col6.metric("Dias ticket -> cobertura", format_number(kpis["avg_days_coverage"], 1))


def render_warning_row(kpis: dict[str, float]) -> None:
    st.subheader("Warning ejecutivo")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Tickets vencidos sin cubrir", format_number(kpis["overdue"]))
    col2.metric("Alta prioridad sin cubrir", format_number(kpis["high_priority"]))
    col3.metric("Sin fecha de procesamiento RRHH", format_number(kpis["no_processing"]))
    col4.metric("Tickets abiertos con 7+ dias", format_number(kpis["aged"]))


def render_storytelling(dataframe: pd.DataFrame, kpis: dict[str, float]) -> None:
    if dataframe.empty:
        st.info("No hay datos para el filtro actual.")
        return

    top_location = dataframe.groupby("Ubicacion de analisis").size().sort_values(ascending=False).head(1)
    top_motive = dataframe.groupby("Motivo del ticket").size().sort_values(ascending=False).head(1)
    avg_by_location = (
        dataframe.groupby("Ubicacion de analisis")["Dias desde ticket hasta cobertura"]
        .mean()
        .sort_values(ascending=False)
    )

    location_name = top_location.index[0] if not top_location.empty else "-"
    location_share = safe_divide(float(top_location.iloc[0]), max(kpis["total"], 1))
    motive_name = top_motive.index[0] if not top_motive.empty else "-"
    motive_count = int(top_motive.iloc[0]) if not top_motive.empty else 0
    slowest_location = avg_by_location.index[0] if not avg_by_location.empty else "-"
    slowest_days = avg_by_location.iloc[0] if not avg_by_location.empty else None

    st.subheader("Story telling gerencial")
    st.markdown(
        f"""
**1. Donde esta la presion operativa.**  
La demanda se concentra en **{location_name}**, que representa **{format_pct(location_share)}** del volumen filtrado.  

**2. Que esta detonando los tickets.**  
El motivo con mas recurrencia es **{motive_name}**, con **{format_number(motive_count)}** tickets.  

**3. Que tan rapido responde RRHH.**  
En promedio pasan **{format_number(kpis['avg_days_processing'], 1)} dias** desde que nace el ticket hasta que RRHH empieza el procesamiento, y **{format_number(kpis['avg_days_coverage'], 1)} dias** hasta cubrir la posicion.  

**4. Donde esta el cuello de botella.**  
La ubicacion con mayor tiempo promedio hasta cobertura es **{slowest_location}** con **{format_number(slowest_days, 1)} dias**.
        """
    )


def render_charts(dataframe: pd.DataFrame) -> None:
    st.subheader("Metricas clave")

    timeline = (
        dataframe.groupby(["Mes del ticket", "Ubicacion de analisis"])
        .size()
        .reset_index(name="Tickets")
        .sort_values("Mes del ticket")
    )
    motives = (
        dataframe.groupby("Motivo del ticket")
        .size()
        .reset_index(name="Tickets")
        .sort_values("Tickets", ascending=False)
        .head(10)
    )
    states = (
        dataframe.groupby("Estado simplificado")
        .size()
        .reset_index(name="Tickets")
        .sort_values("Tickets", ascending=False)
    )
    aging = (
        dataframe.groupby("Ubicacion de analisis")["Dias desde ticket hasta cobertura"]
        .mean()
        .reset_index(name="Promedio dias")
        .dropna()
        .sort_values("Promedio dias", ascending=False)
    )

    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(
            timeline,
            x="Mes del ticket",
            y="Tickets",
            color="Ubicacion de analisis",
            barmode="group",
            title="Evolucion mensual de tickets",
            labels={"Mes del ticket": "Mes", "Tickets": "Tickets"},
        )
        fig.update_layout(height=380, margin=dict(l=20, r=20, t=60, b=20))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.bar(
            motives,
            x="Tickets",
            y="Motivo del ticket",
            orientation="h",
            title="Top 10 motivos del ticket",
            labels={"Motivo del ticket": "Motivo", "Tickets": "Tickets"},
        )
        fig.update_layout(height=380, margin=dict(l=20, r=20, t=60, b=20), yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)
    with col3:
        fig = px.pie(
            states,
            names="Estado simplificado",
            values="Tickets",
            hole=0.55,
            title="Distribucion por etapa",
        )
        fig.update_layout(height=360, margin=dict(l=20, r=20, t=60, b=20))
        st.plotly_chart(fig, use_container_width=True)

    with col4:
        fig = px.bar(
            aging,
            x="Promedio dias",
            y="Ubicacion de analisis",
            orientation="h",
            title="Tiempo promedio hasta cobertura por ubicacion",
            labels={"Promedio dias": "Dias promedio", "Ubicacion de analisis": "Ubicacion"},
        )
        fig.update_layout(height=360, margin=dict(l=20, r=20, t=60, b=20), yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)


def render_stage_funnel(dataframe: pd.DataFrame) -> None:
    stage_counts = pd.DataFrame(
        {
            "Etapa": [
                "Ticket creado",
                "Procesado por RRHH",
                "Reclutado",
                "Posicion cubierta",
            ],
            "Tickets": [
                len(dataframe),
                int(dataframe["Fecha de procesamiento RRHH"].notna().sum()),
                int(dataframe["Fecha de reclutamiento"].notna().sum()),
                int(dataframe["Posicion cubierta"].fillna("No").map(normalize_text).eq("Si").sum()),
            ],
        }
    )
    fig = px.funnel(stage_counts, x="Tickets", y="Etapa", title="Embudo del proceso")
    fig.update_layout(height=360, margin=dict(l=20, r=20, t=60, b=20))
    st.plotly_chart(fig, use_container_width=True)


def render_alert_tables(dataframe: pd.DataFrame) -> None:
    st.subheader("Detalle de alertas")

    overdue_columns = [
        "ID Ticket",
        "Ubicacion de analisis",
        "Cliente",
        "Motivo del ticket",
        "Estado",
        "Prioridad",
        "Fecha del ticket",
        "Fecha de vencimiento",
        "Dias desde ticket hasta cobertura",
    ]
    overdue = dataframe.loc[dataframe["Alerta vencido"], overdue_columns].sort_values(
        ["Prioridad", "Fecha de vencimiento", "Fecha del ticket"],
        ascending=[True, True, True],
    )

    process_columns = [
        "ID Ticket",
        "Ubicacion de analisis",
        "Cliente",
        "Motivo del ticket",
        "Estado",
        "Fecha del ticket",
        "Creado por",
    ]
    no_processing = dataframe.loc[dataframe["Alerta sin procesamiento RRHH"], process_columns].sort_values(
        "Fecha del ticket"
    )

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Tickets vencidos sin cobertura**")
        st.dataframe(overdue.head(20), use_container_width=True, hide_index=True)
    with col2:
        st.markdown("**Tickets abiertos sin procesamiento RRHH**")
        st.dataframe(no_processing.head(20), use_container_width=True, hide_index=True)


def render_support_tables(dataframe: pd.DataFrame) -> None:
    st.subheader("Apoyos para la presentacion")

    by_location = (
        dataframe.groupby("Ubicacion de analisis")
        .agg(
            Tickets=("ID Ticket", "count"),
            Cubiertos=("Posicion cubierta", lambda series: int(series.fillna("No").map(normalize_text).eq("Si").sum())),
            Pendientes=("Pendiente de cobertura", "sum"),
            Dias_Promedio_Cobertura=("Dias desde ticket hasta cobertura", "mean"),
        )
        .reset_index()
    )
    by_location["Cobertura"] = by_location["Cubiertos"] / by_location["Tickets"]

    by_motive = (
        dataframe.groupby("Motivo del ticket")
        .agg(
            Tickets=("ID Ticket", "count"),
            Pendientes=("Pendiente de cobertura", "sum"),
            Dias_Promedio_Cobertura=("Dias desde ticket hasta cobertura", "mean"),
        )
        .reset_index()
        .sort_values("Tickets", ascending=False)
        .head(15)
    )

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Resumen por ubicacion**")
        st.dataframe(by_location, use_container_width=True, hide_index=True)
    with col2:
        st.markdown("**Resumen por motivo**")
        st.dataframe(by_motive, use_container_width=True, hide_index=True)


def render_sidebar_footer(db_path: Path, source_label: str) -> None:
    st.sidebar.divider()
    st.sidebar.caption(source_label)
    st.sidebar.caption(f"Base SQLite: {db_path}")
    st.sidebar.caption(f"Vista de analisis: {ANALYSIS_VIEW}")
    st.sidebar.caption(f"Vista cruda: {RAW_VIEW}")
    st.sidebar.caption(
        "Definiciones: procesamiento RRHH = Fecha de Inicio de Busqueda; "
        "reclutamiento = Fecha de Contrato del Destacado; cobertura = Fecha de Inicio del Destacado."
    )
    if st.sidebar.button("Recargar datos"):
        st.cache_data.clear()
        st.rerun()


def main() -> None:
    db_path, source_label = resolve_data_source()
    if db_path is None:
        render_header()
        st.error(
            "No se encontro una base SQLite para el dashboard. "
            "Sube `ticketera.sqlite` en la barra lateral o agrega el archivo al repo."
        )
        st.info(
            "Opciones recomendadas para Streamlit Web: "
            "1) subir `ticketera.sqlite` manualmente, "
            "2) commitear `artifacts/ticketera.sqlite`, "
            "3) definir `ticketera_db_path` en Streamlit Secrets."
        )
        st.stop()

    render_header()
    dataframe = load_rrhh_dataset(str(db_path))
    filtered = apply_filters(dataframe)
    kpis = build_kpis(filtered)

    render_kpi_row(kpis)
    st.divider()
    render_warning_row(kpis)
    st.divider()
    render_storytelling(filtered, kpis)
    st.divider()
    render_charts(filtered)
    st.divider()
    render_stage_funnel(filtered)
    st.divider()
    render_alert_tables(filtered)
    st.divider()
    render_support_tables(filtered)
    render_sidebar_footer(db_path, source_label)


if __name__ == "__main__":
    main()

import streamlit as st
import pandas as pd
import numpy as np
import io
import hmac
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Reportería TF-21", layout="wide")

# ══════════════════════════════════════════════════════════════════════════════
# AUTH
# ══════════════════════════════════════════════════════════════════════════════
def check_password():
    if st.session_state.get("authenticated"):
        return True
    st.title("🔒 Reportería TF-21")
    st.markdown("Acceso restringido. Ingrese la contraseña para continuar.")
    pwd = st.text_input("Contraseña:", type="password", key="pwd_input")
    if st.button("Ingresar"):
        if hmac.compare_digest(pwd, st.secrets["APP_PASSWORD"]):
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Contraseña incorrecta.")
    return False

if not check_password():
    st.stop()

# ══════════════════════════════════════════════════════════════════════════════
# ETL — same logic as your processing notebook, now embedded in the app
# ══════════════════════════════════════════════════════════════════════════════
COLUMN_NAMES = [
    "Fecha", "TipoTransferencia", "Transferencia",
    "TipoPersona_Ordenante", "TipoID_Ordenante", "NoOrdenCedula_Ordenante",
    "NoID_Ordenante", "Municipio_Ordenante",
    "PrimerApellido_Ordenante", "SegundoApellido_Ordenante",
    "ApellidoCasada_Ordenante", "PrimerNombre_Ordenante", "SegundoNombre_Ordenante",
    "CuentaDebitar_Ordenante", "TipoPersona_Beneficiario", "TipoIDpersona_Beneficiario",
    "NoOrdenCedula_Beneficiario", "NoID_Beneficiario", "Municipio_Beneficiario",
    "PrimerApellido_Beneficiario", "SegundoApellido_Beneficiario",
    "ApellidoCasada_Beneficiario", "PrimerNombre_Beneficiario",
    "SegundoNombre_Beneficiario", "CuentaAbonar_Beneficiario",
    "CodigoInstitucion_Beneficiario", "NoTransaccion", "Pais",
    "CodigoDeptoOrigen", "CodigoDeptoDestino", "CodigoAgencia",
    "MontoMonedaOrgnal", "TipoMoneda", "MontoDolares",
]

RENAME_MAP = {
    'TipoTransferencia':              'Tipo de Transferencia',
    'TipoPersona_Ordenante':          'Tipo de Persona Ordenante',
    'TipoID_Ordenante':               'Tipo de Identificación Ordenante',
    'Municipio_Ordenante':            'Municipio del Ordenante',
    'TipoPersona_Beneficiario':       'Tipo de Persona Beneficiaria',
    'TipoIDpersona_Beneficiario':     'Tipo de Identificación Beneficiario',
    'Municipio_Beneficiario':         'Municipio del Beneficiario',
    'CodigoInstitucion_Beneficiario': 'Institución Beneficiaria',
    'Pais':                           'País',
    'CodigoDeptoOrigen':              'Departamento de Origen',
    'CodigoDeptoDestino':             'Departamento de Destino',
    'CodigoAgencia':                  'Agencia',
    'TipoMoneda':                     'Tipo de Moneda',
    'MontoMonedaOrgnal':              'Monto en Moneda Original',
}

def process_raw_file(uploaded_file) -> pd.DataFrame:
    """Parse one raw TF-21 monthly file (&&-delimited) into a clean DataFrame."""
    df = pd.read_csv(
        uploaded_file,
        sep='&&',
        engine='python',
        encoding='macintosh',
        header=None,
        names=COLUMN_NAMES,
        dtype=str,
        on_bad_lines='warn',
    )

    # Consolidate legal-entity (Jurídica) name fields into PrimerApellido
    for persona_col, apellido_col, rest_cols in [
        ('TipoPersona_Ordenante', 'PrimerApellido_Ordenante',
         ['SegundoApellido_Ordenante', 'ApellidoCasada_Ordenante',
          'PrimerNombre_Ordenante', 'SegundoNombre_Ordenante']),
        ('TipoPersona_Beneficiario', 'PrimerApellido_Beneficiario',
         ['SegundoApellido_Beneficiario', 'ApellidoCasada_Beneficiario',
          'PrimerNombre_Beneficiario', 'SegundoNombre_Beneficiario']),
    ]:
        mask = df[persona_col] == 'J'
        df.loc[mask, apellido_col] = (
            df.loc[mask, [apellido_col] + rest_cols]
            .fillna('').agg(' '.join, axis=1).str.strip()
        )
        df.loc[mask, rest_cols] = np.nan

    df['Fecha']         = pd.to_datetime(df['Fecha'], format='%Y%m%d', errors='coerce')
    df['Anio']          = df['Fecha'].dt.year
    df['Mes']           = df['Fecha'].dt.month
    df['Transferencia'] = df['Transferencia'].replace({"E": "Enviadas", "R": "Recibidas"})
    df['MontoMonedaOrgnal'] = pd.to_numeric(df['MontoMonedaOrgnal'], errors='coerce')

    return df.rename(columns=RENAME_MAP)


def finalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df["Año"]    = df["Anio"]
    df["Año_Mes"] = df["Año"].astype(str) + "-" + df["Mes"].astype(str).str.zfill(2)
    df["Agencia"] = df["Agencia"].astype(str)
    df["Departamento de Origen"] = df["Departamento de Origen"].astype(str)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# FILE UPLOAD
# ══════════════════════════════════════════════════════════════════════════════
st.title("Reportería TF-21")
st.subheader(
    "Información enviada a la IVE sobre remesas ≥ US$300.00 o su equivalente "
    "y transferencias individuales o múltiples ≥ USD 1,000.00"
)

with st.expander("📂 Cargar archivos de reporte", expanded="df" not in st.session_state):
    st.markdown(
        "Suba uno o varios archivos mensuales TF-21 (formato `&&` delimitado). "
        "Puede agregar nuevos meses en cualquier momento — **se acumulan en la sesión**."
    )
    uploaded_files = st.file_uploader(
        "Seleccione archivos:",
        accept_multiple_files=True,
        key="file_uploader",
    )

    if uploaded_files:
        already_loaded = st.session_state.get("loaded_filenames", set())
        new_files = [f for f in uploaded_files if f.name not in already_loaded]

        if new_files:
            with st.spinner(f"Procesando {len(new_files)} archivo(s)..."):
                new_frames, errors = [], []
                for f in new_files:
                    try:
                        new_frames.append(process_raw_file(f))
                        already_loaded.add(f.name)
                    except Exception as e:
                        errors.append(f"❌ `{f.name}`: {e}")

                if new_frames:
                    existing = st.session_state.get("df", pd.DataFrame())
                    combined = pd.concat([existing] + new_frames, ignore_index=True)
                    st.session_state["df"] = finalize_df(combined)
                    st.session_state["loaded_filenames"] = already_loaded

                for err in errors:
                    st.warning(err)

            st.success(
                f"✅ {len(new_files)} archivo(s) cargado(s). "
                f"Total registros en sesión: **{len(st.session_state['df']):,}**"
            )

    if "df" in st.session_state:
        loaded = st.session_state.get("loaded_filenames", set())
        st.caption(f"Archivos en sesión: {', '.join(sorted(loaded))}")
        if st.button("🗑️ Limpiar todos los datos de la sesión"):
            st.session_state.pop("df", None)
            st.session_state.pop("loaded_filenames", None)
            st.rerun()

# Gate — nothing renders below until data is loaded
if "df" not in st.session_state:
    st.info("👆 Cargue al menos un archivo para ver el dashboard.")
    st.stop()

df = st.session_state["df"]

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR FILTERS
# ══════════════════════════════════════════════════════════════════════════════
st.sidebar.header("Filtros")

transferencia_options = sorted(df["Transferencia"].dropna().unique().tolist())
selected_transferencia = st.sidebar.radio(
    "Transferencia:",
    options=["Todas"] + transferencia_options,
    index=0,
    help="Filtra si desea ver transferencias Enviadas, Recibidas o ambas.",
)
df_base = (df[df["Transferencia"] == selected_transferencia].copy()
           if selected_transferencia != "Todas" else df.copy())

st.sidebar.markdown("---")

CATEGORY_COLUMNS = [
    "Tipo de Transferencia",
    "Tipo de Persona Ordenante",
    "Tipo de Identificación Ordenante",
    "Municipio del Ordenante",
    "Tipo de Persona Beneficiaria",
    "Tipo de Identificación Beneficiario",
    "Municipio del Beneficiario",
    "Institución Beneficiaria",
    "País",
    "Departamento de Origen",
    "Departamento de Destino",
    "Agencia",
    "Tipo de Moneda",
]

category = st.sidebar.selectbox("Categoría principal:", CATEGORY_COLUMNS)
secondary_col = st.sidebar.selectbox(
    "Filtro secundario (opcional):",
    ["Ninguno"] + [c for c in CATEGORY_COLUMNS if c != category],
)

df_filtered_secondary = df_base.copy()
if secondary_col != "Ninguno":
    secondary_values = sorted(df_base[secondary_col].dropna().unique())
    selected_secondary = st.sidebar.multiselect(
        f"Valores de {secondary_col}:",
        options=secondary_values,
        default=secondary_values[:3] if len(secondary_values) >= 3 else secondary_values,
    )
    if selected_secondary:
        df_filtered_secondary = df_base[df_base[secondary_col].isin(selected_secondary)]
    else:
        st.sidebar.warning("Seleccione al menos un valor para el filtro secundario.")

if selected_transferencia != "Todas":
    st.info(f"🔍 Mostrando únicamente transferencias: **{selected_transferencia}**")

# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def format_amount(value: float) -> str:
    if abs(value) >= 1_000_000:
        return f"Q {value / 1_000_000:,.2f}M"
    if abs(value) >= 1_000:
        return f"Q {value / 1_000:,.1f}K"
    return f"Q {value:,.2f}"

def render_kpis(current_df, prev_df, label):
    cur_amount  = current_df["Monto en Moneda Original"].sum()
    cur_count   = len(current_df)
    prev_amount = prev_df["Monto en Moneda Original"].sum() if not prev_df.empty else None
    prev_count  = len(prev_df) if not prev_df.empty else None
    mom_amount  = ((cur_amount - prev_amount) / prev_amount * 100) if prev_amount else None
    mom_count   = ((cur_count  - prev_count)  / prev_count  * 100) if prev_count  else None
    avg_tx      = cur_amount / cur_count if cur_count else 0
    col1, col2, col3 = st.columns(3)
    col1.metric("Monto Total", format_amount(cur_amount),
                delta=f"{mom_amount:+.1f}% vs mes anterior" if mom_amount is not None else "Sin dato anterior")
    col2.metric("Nº Transacciones", f"{cur_count:,}",
                delta=f"{mom_count:+.1f}% vs mes anterior" if mom_count is not None else "Sin dato anterior")
    col3.metric("Monto Promedio / Tx", format_amount(avg_tx))
    st.caption(f"Período seleccionado: **{label}**")

def compute_anomalies(series_df, z_thresh, iqr_mult):
    df_out = series_df.copy().sort_values("Año_Mes")
    mu, sig = df_out["Monto_Total"].mean(), df_out["Monto_Total"].std()
    df_out["Z_score"]  = ((df_out["Monto_Total"] - mu) / sig) if sig > 0 else 0.0
    df_out["Flag_Z"]   = df_out["Z_score"].abs() > z_thresh
    Q1, Q3 = df_out["Monto_Total"].quantile(0.25), df_out["Monto_Total"].quantile(0.75)
    IQR    = Q3 - Q1
    df_out["Flag_IQR"] = ((df_out["Monto_Total"] < Q1 - iqr_mult * IQR) |
                          (df_out["Monto_Total"] > Q3 + iqr_mult * IQR))
    df_out["Flag_Any"] = df_out["Flag_Z"] | df_out["Flag_IQR"]
    return df_out

# ══════════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════════
tab1, tab2 = st.tabs(["RESUMEN", "CORRELACIONES"])

# ── TAB 1: RESUMEN ────────────────────────────────────────────────────────────
with tab1:
    mode = st.radio("Modo de visualización:", ["Mes individual", "Serie de tiempo"], horizontal=True)

    if mode == "Mes individual":
        col1, col2 = st.columns(2)
        with col1:
            anio  = st.selectbox("Año:", sorted(df_filtered_secondary["Año"].unique()))
        with col2:
            month = st.selectbox("Mes:", sorted(
                df_filtered_secondary[df_filtered_secondary["Año"] == anio]["Mes"].unique()
            ))

        filtered = df_filtered_secondary[
            (df_filtered_secondary["Mes"] == month) & (df_filtered_secondary["Año"] == anio)
        ]
        prev_month    = month - 1 if month > 1 else 12
        prev_year     = anio if month > 1 else anio - 1
        prev_filtered = df_filtered_secondary[
            (df_filtered_secondary["Mes"] == prev_month) & (df_filtered_secondary["Año"] == prev_year)
        ]

        st.markdown("---")
        render_kpis(filtered, prev_filtered, f"{month:02d}/{anio}")
        st.markdown("---")

        aggregated = (
            filtered.groupby(category)["Monto en Moneda Original"]
            .sum().reset_index()
            .sort_values("Monto en Moneda Original", ascending=False)
        )

        chart_type = st.radio("Tipo de gráfico:", ["Barra Horizontal", "Treemap"], horizontal=True)
        if chart_type == "Barra Horizontal":
            top_n_single = st.slider("Mostrar top N categorías:", 3, 30, 10, key="top_n_single")
            plot_data = aggregated.head(top_n_single).sort_values("Monto en Moneda Original", ascending=True)
            fig = px.bar(plot_data, x="Monto en Moneda Original", y=category, orientation="h",
                         title=f"Monto por {category} — {month:02d}/{anio}",
                         color="Monto en Moneda Original", color_continuous_scale="Blues",
                         text="Monto en Moneda Original")
            fig.update_traces(texttemplate="%{x:,.0f}", textposition="outside")
            fig.update_layout(coloraxis_showscale=False, yaxis_title="",
                              xaxis_title="Monto Total", height=max(400, top_n_single * 40))
        else:
            fig = px.treemap(aggregated, path=[category], values="Monto en Moneda Original",
                             title=f"Treemap por {category} — {month:02d}/{anio}",
                             color="Monto en Moneda Original", color_continuous_scale="Blues")

        st.plotly_chart(fig, use_container_width=True)

        if not aggregated.empty:
            top3_share = (aggregated.head(3)["Monto en Moneda Original"].sum()
                          / aggregated["Monto en Moneda Original"].sum() * 100)
            st.info(f"Las 3 principales categorías concentran el **{top3_share:.1f}%** del monto total.")

        buffer = io.BytesIO()
        aggregated.to_excel(buffer, index=False)
        buffer.seek(0)
        st.download_button("Descargar Excel", buffer, f"reporte_{anio}_{month:02d}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    else:  # Serie de tiempo
        st.markdown("#### Rango de meses")
        available_periods = sorted(df_filtered_secondary["Año_Mes"].unique())
        if len(available_periods) < 2:
            st.warning("No hay suficientes períodos para mostrar una serie de tiempo.")
            st.stop()

        idx_start, idx_end = st.select_slider(
            "Seleccione el rango de períodos:", options=available_periods,
            value=(available_periods[0], available_periods[-1]),
        )
        selected_periods = available_periods[
            available_periods.index(idx_start): available_periods.index(idx_end) + 1
        ]
        filtered_ts  = df_filtered_secondary[df_filtered_secondary["Año_Mes"].isin(selected_periods)]
        last_period  = selected_periods[-1]
        prev_period  = selected_periods[-2] if len(selected_periods) > 1 else None
        cur_kpi      = filtered_ts[filtered_ts["Año_Mes"] == last_period]
        prev_kpi     = filtered_ts[filtered_ts["Año_Mes"] == prev_period] if prev_period else pd.DataFrame()

        st.markdown("---")
        render_kpis(cur_kpi, prev_kpi, f"{idx_start} → {idx_end}")
        st.markdown("---")

        aggregated_ts = (
            filtered_ts.groupby(["Año_Mes", category])["Monto en Moneda Original"]
            .sum().reset_index().sort_values("Año_Mes")
        )
        top_n = st.slider("Mostrar top N categorías:", 3, 20, 10)
        top_cats = (aggregated_ts.groupby(category)["Monto en Moneda Original"]
                    .sum().nlargest(top_n).index.tolist())
        aggregated_ts = aggregated_ts[aggregated_ts[category].isin(top_cats)]

        a_col1, a_col2 = st.columns(2)
        with a_col1:
            z_thresh_ts = st.slider("Umbral Z-score:", 1.5, 3.5, 2.0, step=0.25, key="z_ts")
        with a_col2:
            iqr_mult_ts = st.slider("Multiplicador IQR:", 1.0, 3.0, 1.5, step=0.25, key="iqr_ts")

        total_per_period = (
            filtered_ts.groupby("Año_Mes")["Monto en Moneda Original"]
            .sum().reset_index().rename(columns={"Monto en Moneda Original": "Monto_Total"})
        )
        anomaly_df = compute_anomalies(total_per_period, z_thresh_ts, iqr_mult_ts)
        flagged_df = anomaly_df[anomaly_df["Flag_Any"]]

        chart_ts_type = st.radio("Tipo de gráfico:",
                                  ["Línea", "Barra apilada", "Barra horizontal (último período)"],
                                  horizontal=True)
        if chart_ts_type == "Línea":
            fig = px.line(aggregated_ts, x="Año_Mes", y="Monto en Moneda Original", color=category,
                          markers=True, title=f"Evolución mensual por {category}",
                          labels={"Monto en Moneda Original": "Monto Total", "Año_Mes": "Período"})
            fig.update_layout(hovermode="x unified")
        elif chart_ts_type == "Barra apilada":
            fig = px.bar(aggregated_ts, x="Año_Mes", y="Monto en Moneda Original", color=category,
                         barmode="stack", title=f"Composición mensual por {category}",
                         labels={"Monto en Moneda Original": "Monto Total", "Año_Mes": "Período"})
            fig.update_layout(hovermode="x unified")
        else:
            last_data = (aggregated_ts[aggregated_ts["Año_Mes"] == last_period]
                         .sort_values("Monto en Moneda Original", ascending=True))
            fig = px.bar(last_data, x="Monto en Moneda Original", y=category, orientation="h",
                         title=f"Monto por {category} — {last_period}",
                         color="Monto en Moneda Original", color_continuous_scale="Blues",
                         text="Monto en Moneda Original")
            fig.update_traces(texttemplate="%{x:,.0f}", textposition="outside")
            fig.update_layout(coloraxis_showscale=False, yaxis_title="", height=max(400, top_n * 40))

        st.plotly_chart(fig, use_container_width=True)

        if flagged_df.empty:
            st.success("✅ No se detectaron períodos anómalos con los umbrales actuales.")
        else:
            details = [f"**{r['Año_Mes']}** ({'Z' if r['Flag_Z'] else ''}{'/IQR' if r['Flag_IQR'] else ''})"
                       for _, r in flagged_df.iterrows()]
            st.error(f"⚠️ **{len(flagged_df)} período(s) anómalo(s):** " + " · ".join(details))

        if not aggregated_ts.empty:
            top1 = aggregated_ts.groupby(category)["Monto en Moneda Original"].sum().idxmax()
            st.info(f"La categoría con mayor volumen acumulado en el período es **{top1}**.")

        pivot = aggregated_ts.pivot(index="Año_Mes", columns=category,
                                     values="Monto en Moneda Original").fillna(0)
        st.dataframe(pivot.style.format("{:,.2f}"), use_container_width=True)

        buffer = io.BytesIO()
        pivot.to_excel(buffer)
        buffer.seek(0)
        st.download_button("Descargar Excel", buffer, "serie_tiempo.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ── TAB 2: CORRELACIONES ──────────────────────────────────────────────────────
with tab2:
    st.markdown("### Análisis de Correlaciones en Serie de Tiempo")
    st.caption("Explora cómo se relacionan el monto, el volumen de transacciones y las categorías.")

    available_periods_c = sorted(df_filtered_secondary["Año_Mes"].unique())
    if len(available_periods_c) < 3:
        st.warning("Se necesitan al menos 3 períodos para calcular correlaciones.")
        st.stop()

    c_start, c_end = st.select_slider(
        "Rango de períodos:", options=available_periods_c,
        value=(available_periods_c[0], available_periods_c[-1]), key="corr_range",
    )
    corr_periods = available_periods_c[
        available_periods_c.index(c_start): available_periods_c.index(c_end) + 1
    ]
    df_corr = df_filtered_secondary[df_filtered_secondary["Año_Mes"].isin(corr_periods)]

    base_ts = (
        df_corr.groupby("Año_Mes")
        .agg(Monto=("Monto en Moneda Original", "sum"),
             Volumen=("Monto en Moneda Original", "count"),
             Promedio=("Monto en Moneda Original", "mean"))
        .reset_index().sort_values("Año_Mes")
    )

    st.markdown("---")
    st.markdown("#### 1 · Monto vs Volumen de Transacciones")
    st.caption("Ambas series indexadas a 100 en el primer período. Divergencia sostenida puede indicar estructuración.")
    base_idx = base_ts.iloc[0]
    fig_idx  = go.Figure()
    fig_idx.add_trace(go.Scatter(x=base_ts["Año_Mes"],
                                  y=(base_ts["Monto"] / base_idx["Monto"]) * 100,
                                  name="Monto (índice)", mode="lines+markers",
                                  line=dict(color="#1f77b4", width=2.5)))
    fig_idx.add_trace(go.Scatter(x=base_ts["Año_Mes"],
                                  y=(base_ts["Volumen"] / base_idx["Volumen"]) * 100,
                                  name="N° Transacciones (índice)", mode="lines+markers",
                                  line=dict(color="#ff7f0e", width=2.5, dash="dot")))
    fig_idx.add_hline(y=100, line_dash="dash", line_color="gray", annotation_text="Base = 100")
    fig_idx.update_layout(title="Evolución proporcional: Monto vs Volumen (Base 100)",
                           yaxis_title="Índice", hovermode="x unified", height=380)
    st.plotly_chart(fig_idx, use_container_width=True)

    st.markdown("#### 1b · Ratio: Volumen / Monto")
    base_ts["Ratio"]    = (base_ts["Volumen"] / base_ts["Monto"]) * 10_000
    base_ts["Ratio_MA"] = base_ts["Ratio"].rolling(window=3, min_periods=1).mean()
    fig_ratio = go.Figure()
    fig_ratio.add_trace(go.Bar(x=base_ts["Año_Mes"], y=base_ts["Ratio"],
                                name="Ratio mensual", marker_color="#ff7f0e", opacity=0.6))
    fig_ratio.add_trace(go.Scatter(x=base_ts["Año_Mes"], y=base_ts["Ratio_MA"],
                                    name="Media móvil 3m", mode="lines",
                                    line=dict(color="#d62728", width=2.5)))
    fig_ratio.add_hline(y=base_ts["Ratio"].mean(), line_dash="dash", line_color="gray",
                         annotation_text=f"Media histórica: {base_ts['Ratio'].mean():.2f}")
    fig_ratio.update_layout(title="Ratio: N° Transacciones por cada Q10,000 movidos",
                             yaxis_title="Transacciones / Q10,000", hovermode="x unified",
                             height=360, bargap=0.3)
    st.plotly_chart(fig_ratio, use_container_width=True)

    st.markdown("#### 2 · Correlación Móvil: Monto vs Volumen")
    roll_window = st.slider("Ventana móvil (períodos):", min_value=2,
                             max_value=max(3, len(base_ts) - 1),
                             value=min(3, len(base_ts) - 1), key="roll_window")
    base_ts["Rolling_Corr"] = base_ts["Monto"].rolling(roll_window).corr(base_ts["Volumen"])
    fig_roll = go.Figure()
    fig_roll.add_trace(go.Scatter(x=base_ts["Año_Mes"], y=base_ts["Rolling_Corr"],
                                   mode="lines+markers", name=f"Corr móvil ({roll_window}p)",
                                   line=dict(color="#2ca02c", width=2.5),
                                   fill="tozeroy", fillcolor="rgba(44,160,44,0.1)"))
    for y, dash, color, label in [
        (0, "dash", "gray", "r = 0"), (0.7, "dot", "#1f77b4", "r = 0.7"), (-0.7, "dot", "#d62728", "r = −0.7")
    ]:
        fig_roll.add_hline(y=y, line_dash=dash, line_color=color, annotation_text=label)
    fig_roll.update_layout(title=f"Correlación móvil Monto ↔ Volumen (ventana = {roll_window}p)",
                            yaxis=dict(title="Coeficiente de correlación", range=[-1.1, 1.1]),
                            hovermode="x unified", height=350)
    st.plotly_chart(fig_roll, use_container_width=True)
    overall_corr  = base_ts["Monto"].corr(base_ts["Volumen"])
    color_emoji   = "🟢" if abs(overall_corr) >= 0.7 else ("🟡" if abs(overall_corr) >= 0.4 else "🔴")
    st.info(f"{color_emoji} Correlación global Monto ↔ Volumen: **r = {overall_corr:.3f}**")

    st.markdown("---")
    st.markdown("#### 3 · Heatmap de Correlación entre Categorías")
    top_n_heatmap = st.slider("N° de categorías para el heatmap:", 4, 20, 8, key="heatmap_n")
    top_cats_hm   = (df_corr.groupby(category)["Monto en Moneda Original"]
                     .sum().nlargest(top_n_heatmap).index.tolist())
    pivot_hm = (df_corr[df_corr[category].isin(top_cats_hm)]
                .groupby(["Año_Mes", category])["Monto en Moneda Original"]
                .sum().unstack(fill_value=0))
    if pivot_hm.shape[0] >= 3 and pivot_hm.shape[1] >= 2:
        corr_matrix = pivot_hm.corr()
        fig_hm = px.imshow(corr_matrix, text_auto=".2f", color_continuous_scale="RdBu_r",
                            zmin=-1, zmax=1, title=f"Correlación de Monto por {category}",
                            aspect="auto", height=500)
        st.plotly_chart(fig_hm, use_container_width=True)
        mask = np.abs(corr_matrix.values) >= 0.85
        np.fill_diagonal(mask, False)
        pairs = [(corr_matrix.index[i], corr_matrix.columns[j], corr_matrix.values[i, j])
                  for i, j in zip(*np.where(mask)) if i < j]
        if pairs:
            with st.expander(f"⚠️ {len(pairs)} par(es) con correlación fuerte (|r| ≥ 0.85)"):
                for a, b, r in sorted(pairs, key=lambda x: abs(x[2]), reverse=True):
                    st.write(f"• **{a}** ↔ **{b}** → r = {r:.3f}")
        else:
            st.success("No se detectaron correlaciones inusualmente fuertes entre categorías.")
    else:
        st.warning("Datos insuficientes para construir la matriz de correlación.")

    st.markdown("---")
    st.markdown("#### 4 · Dispersión: Monto vs Volumen por Categoría y Período")
    top_n_scatter = st.slider("N° de categorías:", 3, 15, 8, key="scatter_n")
    top_cats_sc   = (df_corr.groupby(category)["Monto en Moneda Original"]
                     .sum().nlargest(top_n_scatter).index.tolist())
    scatter_df = (df_corr[df_corr[category].isin(top_cats_sc)]
                  .groupby(["Año_Mes", category])
                  .agg(Monto=("Monto en Moneda Original", "sum"),
                       Volumen=("Monto en Moneda Original", "count"),
                       Promedio=("Monto en Moneda Original", "mean"))
                  .reset_index())
    fig_sc = px.scatter(scatter_df, x="Volumen", y="Monto", color=category, size="Promedio",
                         hover_data=["Año_Mes", "Promedio"],
                         title=f"Monto vs Volumen por {category} (burbuja = monto promedio)",
                         labels={"Volumen": "N° Transacciones", "Monto": "Monto Total"},
                         trendline="ols", trendline_scope="overall", height=480)
    st.plotly_chart(fig_sc, use_container_width=True)

    st.markdown("#### 5 · Detección de Anomalías por Z-Score")
    z_threshold = st.slider("Umbral de Z-score:", 1.5, 3.5, 2.0, step=0.25, key="z_thresh")
    base_ts["Z_Monto"] = (base_ts["Monto"] - base_ts["Monto"].mean()) / base_ts["Monto"].std()
    base_ts["Anomaly"] = base_ts["Z_Monto"].abs() > z_threshold
    mu, sig  = base_ts["Monto"].mean(), base_ts["Monto"].std()
    fig_z    = go.Figure()
    fig_z.add_trace(go.Scatter(x=base_ts["Año_Mes"], y=base_ts["Monto"],
                                mode="lines+markers", name="Monto Total",
                                line=dict(color="#1f77b4", width=2)))
    anomalies = base_ts[base_ts["Anomaly"]]
    if not anomalies.empty:
        fig_z.add_trace(go.Scatter(x=anomalies["Año_Mes"], y=anomalies["Monto"],
                                    mode="markers", name=f"Anomalía (|Z| > {z_threshold})",
                                    marker=dict(color="#d62728", size=14, symbol="x")))
    fig_z.add_hline(y=mu + z_threshold * sig, line_dash="dot", line_color="#d62728",
                     annotation_text=f"+{z_threshold}σ")
    fig_z.add_hline(y=mu - z_threshold * sig, line_dash="dot", line_color="#d62728",
                     annotation_text=f"−{z_threshold}σ")
    fig_z.add_hline(y=mu, line_dash="dash", line_color="gray", annotation_text="Media")
    fig_z.update_layout(title=f"Anomalías en Monto Total (Z-score ±{z_threshold})",
                         xaxis_title="Período", yaxis_title="Monto Total (Q)",
                         hovermode="x unified", height=380)
    st.plotly_chart(fig_z, use_container_width=True)
    if base_ts["Anomaly"].sum() > 0:
        st.warning(f"Se detectaron **{base_ts['Anomaly'].sum()}** período(s) anómalo(s): "
                   + ", ".join(anomalies["Año_Mes"].tolist()))
    else:
        st.success(f"No se detectaron anomalías con umbral Z = ±{z_threshold}.")

    st.markdown("---")
    buffer2 = io.BytesIO()
    base_ts.to_excel(buffer2, index=False)
    buffer2.seek(0)
    st.download_button("Descargar datos de correlación (Excel)", buffer2, "correlaciones.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
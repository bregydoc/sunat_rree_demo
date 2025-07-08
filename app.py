#!/usr/bin/env python
"""Dashboard Web - Observatorio de Comercio Perú 🇵🇪"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
from datetime import datetime, timedelta
import numpy as np

# Configuración de página
st.set_page_config(
    page_title="Observatorio Comercio Perú",
    page_icon="🇵🇪",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Cache para datos
@st.cache_data(ttl=900)  # Cache por 15 minutos
def load_data():
    """Cargar datos desde DuckDB"""
    try:
        con = duckdb.connect("trade.duckdb")
        
        # Intentar cargar KPI si existe
        try:
            kpi_df = con.sql("SELECT * FROM kpi_monthly WHERE month != 'Total'").df()
            has_kpi = True
        except:
            has_kpi = False
            kpi_df = None
        
        # Datos base siempre disponibles
        base_df = con.sql("""
            SELECT 
                year, month,
                SUM(CASE WHEN flow='export' THEN usd END) as export,
                SUM(CASE WHEN flow='import' THEN usd END) as import,
                SUM(CASE WHEN flow='export' THEN usd END) - 
                SUM(CASE WHEN flow='import' THEN usd END) as balance
            FROM trade 
            WHERE month != 'Total'
            GROUP BY year, month
            ORDER BY year, month
        """).df()
        
        con.close()
        return base_df, kpi_df, has_kpi
        
    except Exception as e:
        st.error(f"Error cargando datos: {e}")
        return None, None, False

def format_currency(value, suffix=""):
    """Formatear moneda en millones o billones"""
    if pd.isna(value):
        return "N/A"
    if abs(value) >= 1e9:
        return f"${value/1e9:.1f}B{suffix}"
    elif abs(value) >= 1e6:
        return f"${value/1e6:.0f}M{suffix}"
    else:
        return f"${value:,.0f}{suffix}"

def main():
    """Dashboard principal"""
    
    # Header
    st.title("🇵🇪 Observatorio de Comercio Exterior del Perú")
    st.markdown("*Análisis en tiempo real de importaciones y exportaciones*")
    
    # Cargar datos
    base_df, kpi_df, has_kpi = load_data()
    
    if base_df is None:
        st.error("❌ No se pudieron cargar los datos. Ejecuta primero `uv run python observatorio/etl.py`")
        return
    
    # Usar KPI si está disponible, sino base
    df = kpi_df if has_kpi else base_df
    
    # Preparar datos temporales
    month_order = [
        "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
        "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
    ]
    
    df['month_num'] = df['month'].map({m: i+1 for i, m in enumerate(month_order)})
    df = df.sort_values(['year', 'month_num']).reset_index(drop=True)
    df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month_num'].astype(str), format='%Y-%m')
    
    # ==============================================
    # SIDEBAR - CONTROLES
    # ==============================================
    
    st.sidebar.header("⚙️ Configuración")
    
    # Filtro de años
    min_year, max_year = int(df['year'].min()), int(df['year'].max())
    year_range = st.sidebar.slider(
        "Rango de años",
        min_value=min_year,
        max_value=max_year,
        value=(max(min_year, max_year-10), max_year),
        step=1
    )
    
    # Filtro por flujo
    show_exports = st.sidebar.checkbox("Mostrar Exportaciones", value=True)
    show_imports = st.sidebar.checkbox("Mostrar Importaciones", value=True)
    show_balance = st.sidebar.checkbox("Mostrar Balance", value=True)
    
    # Tipo de vista
    view_type = st.sidebar.selectbox(
        "Tipo de visualización",
        ["Valores Absolutos", "Índices (2005=100)", "Variaciones %"]
    )
    
    # Filtrar datos
    mask = df['year'].between(*year_range)
    filtered_df = df[mask].copy()
    
    # ==============================================
    # MÉTRICAS PRINCIPALES
    # ==============================================
    
    st.header("📊 Métricas Clave")
    
    # Calcular métricas YTD para el último año disponible
    current_year = filtered_df['year'].max()
    ytd_data = filtered_df[filtered_df['year'] == current_year]
    
    if len(ytd_data) > 0:
        export_ytd = ytd_data['export'].sum() if 'export' in ytd_data.columns else 0
        import_ytd = ytd_data['import'].sum() if 'import' in ytd_data.columns else 0
        balance_ytd = export_ytd - import_ytd
        
        # Comparar con año anterior
        prev_year_data = filtered_df[filtered_df['year'] == current_year - 1]
        if len(prev_year_data) > 0:
            export_prev = prev_year_data['export'].sum()
            import_prev = prev_year_data['import'].sum()
            export_change = (export_ytd / export_prev - 1) * 100 if export_prev > 0 else 0
            import_change = (import_ytd / import_prev - 1) * 100 if import_prev > 0 else 0
        else:
            export_change = import_change = 0
    else:
        export_ytd = import_ytd = balance_ytd = 0
        export_change = import_change = 0
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "🟢 Exportaciones YTD",
            format_currency(export_ytd),
            f"{export_change:+.1f}%" if export_change != 0 else None
        )
    
    with col2:
        st.metric(
            "🔴 Importaciones YTD", 
            format_currency(import_ytd),
            f"{import_change:+.1f}%" if import_change != 0 else None
        )
    
    with col3:
        balance_color = "🟢" if balance_ytd >= 0 else "🔴"
        st.metric(
            f"{balance_color} Balance YTD",
            format_currency(balance_ytd),
            None
        )
    
    with col4:
        coverage = (export_ytd / import_ytd * 100) if import_ytd > 0 else 0
        st.metric(
            "📊 Cobertura",
            f"{coverage:.1f}%",
            "Export/Import ratio"
        )
    
    # ==============================================
    # GRÁFICO PRINCIPAL
    # ==============================================
    
    st.header("📈 Serie Temporal")
    
    fig = go.Figure()
    
    if show_exports and 'export' in filtered_df.columns:
        y_values = filtered_df['export'] / 1e9
        fig.add_trace(go.Scatter(
            x=filtered_df['date'],
            y=y_values,
            name='Exportaciones',
            line=dict(color='#2E8B57', width=2),
            hovertemplate='<b>Exportaciones</b><br>%{x}<br>$%{y:.1f}B USD<extra></extra>'
        ))
    
    if show_imports and 'import' in filtered_df.columns:
        y_values = filtered_df['import'] / 1e9
        fig.add_trace(go.Scatter(
            x=filtered_df['date'],
            y=y_values,
            name='Importaciones',
            line=dict(color='#DC143C', width=2),
            hovertemplate='<b>Importaciones</b><br>%{x}<br>$%{y:.1f}B USD<extra></extra>'
        ))
    
    if show_balance and 'balance' in filtered_df.columns:
        y_values = filtered_df['balance'] / 1e9
        fig.add_trace(go.Scatter(
            x=filtered_df['date'],
            y=y_values,
            name='Balance',
            line=dict(color='#4169E1', width=2),
            hovertemplate='<b>Balance</b><br>%{x}<br>$%{y:.1f}B USD<extra></extra>'
        ))
        
        # Línea de referencia en cero
        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    
    fig.update_layout(
        title=f"Comercio Exterior del Perú ({year_range[0]}-{year_range[1]})",
        xaxis_title="Año",
        yaxis_title="Miles de Millones USD",
        template="plotly_white",
        hovermode="x unified",
        height=500
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # ==============================================
    # ANÁLISIS ADICIONALES
    # ==============================================
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📅 Estacionalidad")
        
        # Heatmap por año y mes
        if len(filtered_df) > 12:
            pivot_data = filtered_df.pivot_table(
                index='month',
                columns='year', 
                values='export',
                aggfunc='mean'
            )
            
            fig_heat = px.imshow(
                pivot_data.values,
                x=pivot_data.columns,
                y=pivot_data.index,
                aspect='auto',
                color_continuous_scale='RdYlGn',
                title="Exportaciones por Año y Mes"
            )
            
            fig_heat.update_layout(height=400)
            st.plotly_chart(fig_heat, use_container_width=True)
    
    with col2:
        st.subheader("📊 Distribución Mensual")
        
        # Box plot por mes
        month_stats = filtered_df.groupby('month')['export'].agg(['mean', 'std']).reset_index()
        month_stats['month_num'] = month_stats['month'].map({m: i+1 for i, m in enumerate(month_order)})
        month_stats = month_stats.sort_values('month_num')
        
        fig_box = go.Figure()
        fig_box.add_trace(go.Bar(
            x=month_stats['month'],
            y=month_stats['mean']/1e9,
            error_y=dict(type='data', array=month_stats['std']/1e9),
            name='Promedio ± Std',
            marker_color='lightblue'
        ))
        
        fig_box.update_layout(
            title="Exportaciones Promedio por Mes",
            xaxis_title="Mes",
            yaxis_title="Miles de Millones USD",
            height=400
        )
        fig_box.update_xaxes(tickangle=45)
        
        st.plotly_chart(fig_box, use_container_width=True)
    
    # ==============================================
    # TABLA DETALLADA
    # ==============================================
    
    st.header("📋 Datos Detallados")
    
    # Preparar tabla
    display_df = filtered_df[['year', 'month', 'export', 'import', 'balance']].copy()
    display_df['export'] = display_df['export'].apply(lambda x: format_currency(x))
    display_df['import'] = display_df['import'].apply(lambda x: format_currency(x))
    display_df['balance'] = display_df['balance'].apply(lambda x: format_currency(x))
    
    # Renombrar columnas
    display_df.columns = ['Año', 'Mes', 'Exportaciones', 'Importaciones', 'Balance']
    
    # Mostrar tabla con paginación
    st.dataframe(
        display_df.tail(24),  # Últimos 2 años
        use_container_width=True,
        height=400
    )
    
    # ==============================================
    # FOOTER
    # ==============================================
    
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.caption(f"📅 Última actualización: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    
    with col2:
        st.caption(f"📊 {len(filtered_df)} registros mostrados")
    
    with col3:
        if has_kpi:
            st.caption("✅ Con métricas KPI")
        else:
            st.caption("⚠️ Datos base (ejecutar metrics.py para KPIs)")

if __name__ == "__main__":
    main() 
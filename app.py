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

@st.cache_data(ttl=900)
def load_products_data():
    """Cargar datos de productos por categoría"""
    try:
        con = duckdb.connect("trade.duckdb")
        
        # Intentar cargar KPI de productos
        try:
            kpi_prod_df = con.sql("SELECT * FROM kpi_prod_monthly WHERE month != 'Total'").df()
            has_prod_kpi = True
        except:
            has_prod_kpi = False
            kpi_prod_df = None
        
        # Datos base de productos
        try:
            prod_df = con.sql("""
                SELECT 
                    year, month, category,
                    SUM(CASE WHEN flow='export' THEN usd END) as export,
                    SUM(CASE WHEN flow='import' THEN usd END) as import,
                    SUM(CASE WHEN flow='export' THEN usd END) - 
                    SUM(CASE WHEN flow='import' THEN usd END) as balance
                FROM trade_prod 
                WHERE month != 'Total'
                GROUP BY year, month, category
                ORDER BY year, month, category
            """).df()
            has_prod = True
        except:
            has_prod = False
            prod_df = None
        
        con.close()
        return prod_df, kpi_prod_df, has_prod, has_prod_kpi
        
    except Exception as e:
        return None, None, False, False

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
    
    # Cargar datos generales
    base_df, kpi_df, has_kpi = load_data()
    
    # Cargar datos de productos
    prod_df, kpi_prod_df, has_prod, has_prod_kpi = load_products_data()
    
    if base_df is None:
        st.error("❌ No se pudieron cargar los datos. Ejecuta primero `uv run python observatorio/etl.py`")
        return
    
    # Pestañas para diferentes análisis
    tab1, tab2 = st.tabs(["🇵🇪 Análisis por País", "🏷️ Análisis por Categorías"])
    
    with tab1:
        render_country_analysis(base_df, kpi_df, has_kpi)
    
    with tab2:
        if has_prod:
            render_category_analysis(prod_df, kpi_prod_df, has_prod_kpi)
        else:
            st.warning("❌ No hay datos de productos disponibles.")
            st.info("💡 Ejecuta: `uv run python observatorio/etl_products.py` para generar datos por categorías")

def render_country_analysis(base_df, kpi_df, has_kpi):
    """Renderizar análisis por país (datos agregados nacionales)"""
    
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
    # SIDEBAR - CONTROLES POR PAÍS
    # ==============================================
    
    st.sidebar.header("🇵🇪 Configuración País")
    
    # Filtro de años
    min_year, max_year = int(df['year'].min()), int(df['year'].max())
    year_range = st.sidebar.slider(
        "Rango de años",
        min_value=min_year,
        max_value=max_year,
        value=(max(min_year, max_year-10), max_year),
        step=1,
        key="country_years"
    )
    
    # Filtro por flujo
    show_exports = st.sidebar.checkbox("Mostrar Exportaciones", value=True, key="country_exports")
    show_imports = st.sidebar.checkbox("Mostrar Importaciones", value=True, key="country_imports")
    show_balance = st.sidebar.checkbox("Mostrar Balance", value=True, key="country_balance")
    
    # Tipo de vista
    view_type = st.sidebar.selectbox(
        "Tipo de visualización",
        ["Valores Absolutos", "Índices (2005=100)", "Variaciones %"],
        key="country_view_type"
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

def render_category_analysis(prod_df, kpi_prod_df, has_prod_kpi):
    """Renderizar análisis por categorías de productos"""
    
    # Usar KPI de productos si está disponible, sino base
    df = kpi_prod_df if has_prod_kpi else prod_df
    
    # Preparar datos temporales
    month_order = [
        "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
        "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
    ]
    
    df['month_num'] = df['month'].map({m: i+1 for i, m in enumerate(month_order)})
    df = df.sort_values(['year', 'month_num', 'category']).reset_index(drop=True)
    df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month_num'].astype(str), format='%Y-%m')
    
    # ==============================================
    # SIDEBAR - CONTROLES DE CATEGORÍAS
    # ==============================================
    
    st.sidebar.header("🏷️ Filtros de Categorías")
    
    # Filtro de años para categorías
    min_year, max_year = int(df['year'].min()), int(df['year'].max())
    year_range_cat = st.sidebar.slider(
        "Rango de años (categorías)",
        min_value=min_year,
        max_value=max_year,
        value=(max(min_year, max_year-5), max_year),
        step=1,
        key="category_years"
    )
    
    # Filtro de categorías
    all_categories = sorted(df['category'].unique())
    
    # Selector de top categorías
    n_top = st.sidebar.number_input(
        "Mostrar top N categorías",
        min_value=5,
        max_value=min(50, len(all_categories)),
        value=10,
        step=5,
        key="n_top_categories"
    )
    
    # Obtener top categorías por exportación del último año
    last_year_data = df[df['year'] == df['year'].max()]
    if 'exp' in df.columns:
        top_categories = (last_year_data.groupby('category')['exp']
                         .sum()
                         .sort_values(ascending=False)
                         .head(n_top)
                         .index.tolist())
    else:
        top_categories = (last_year_data.groupby('category')['export']
                         .sum()
                         .sort_values(ascending=False)
                         .head(n_top)
                         .index.tolist())
    
    # Selector manual de categorías
    manual_mode = st.sidebar.checkbox("Selección manual de categorías", key="manual_categories")
    
    if manual_mode:
        selected_categories = st.sidebar.multiselect(
            "Seleccionar categorías",
            options=all_categories,
            default=top_categories[:5],
            key="selected_categories"
        )
    else:
        selected_categories = top_categories
        st.sidebar.info(f"Mostrando top {len(selected_categories)} categorías por exportación")
    
    # Tipo de análisis
    analysis_type = st.sidebar.selectbox(
        "Tipo de análisis",
        ["Exportaciones", "Importaciones", "Balance", "Cobertura (Exp/Imp)"],
        key="category_analysis_type"
    )
    
    # Filtrar datos
    mask = (df['year'].between(*year_range_cat)) & (df['category'].isin(selected_categories))
    filtered_df = df[mask].copy()
    
    if filtered_df.empty:
        st.warning("❌ No hay datos para los filtros seleccionados")
        return
    
    # ==============================================
    # MÉTRICAS PRINCIPALES POR CATEGORÍAS
    # ==============================================
    
    st.header("📊 Métricas por Categorías")
    
    # Calcular métricas YTD por categoría
    current_year = filtered_df['year'].max()
    ytd_data = filtered_df[filtered_df['year'] == current_year]
    
    if len(ytd_data) > 0:
        if 'exp' in filtered_df.columns and 'imp' in filtered_df.columns:
            exp_col, imp_col = 'exp', 'imp'
        else:
            exp_col, imp_col = 'export', 'import'
        
        cat_metrics = ytd_data.groupby('category').agg({
            exp_col: 'sum',
            imp_col: 'sum'
        }).round(0)
        cat_metrics['balance'] = cat_metrics[exp_col] - cat_metrics[imp_col]
        cat_metrics['coverage'] = (cat_metrics[exp_col] / cat_metrics[imp_col] * 100).round(1)
        
        # Mostrar métricas totales
        total_exp = cat_metrics[exp_col].sum()
        total_imp = cat_metrics[imp_col].sum()
        total_bal = total_exp - total_imp
        total_cov = (total_exp / total_imp * 100) if total_imp > 0 else 0
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("🟢 Exportaciones Categorías", format_currency(total_exp))
        with col2:
            st.metric("🔴 Importaciones Categorías", format_currency(total_imp))
        with col3:
            balance_color = "🟢" if total_bal >= 0 else "🔴"
            st.metric(f"{balance_color} Balance Categorías", format_currency(total_bal))
        with col4:
            st.metric("📊 Cobertura Promedio", f"{total_cov:.1f}%")
    
    # ==============================================
    # GRÁFICO PRINCIPAL - STACKED AREA
    # ==============================================
    
    st.header("📈 Análisis Temporal por Categorías")
    
    # Preparar datos para visualización
    if analysis_type == "Exportaciones":
        value_col = exp_col if 'exp' in filtered_df.columns else 'export'
        title = "Exportaciones por Categoría"
        color_scale = "Greens"
    elif analysis_type == "Importaciones":
        value_col = imp_col if 'imp' in filtered_df.columns else 'import'
        title = "Importaciones por Categoría"
        color_scale = "Reds"
    elif analysis_type == "Balance":
        value_col = 'balance'
        title = "Balance Comercial por Categoría"
        color_scale = "RdBu"
    else:  # Cobertura
        if 'cov_ratio' in filtered_df.columns:
            value_col = 'cov_ratio'
            filtered_df[value_col] = filtered_df[value_col] * 100  # Convertir a porcentaje
        else:
            filtered_df['coverage'] = (filtered_df[exp_col] / filtered_df[imp_col] * 100).replace([float('inf'), -float('inf')], None)
            value_col = 'coverage'
        title = "Ratio de Cobertura por Categoría (%)"
        color_scale = "Viridis"
    
    # Gráfico stacked area
    if analysis_type != "Cobertura":
        fig = px.area(
            filtered_df, 
            x='date', 
            y=value_col,
            color='category',
            title=title,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        
        fig.update_layout(
            xaxis_title="Fecha",
            yaxis_title="Miles de Millones USD" if analysis_type != "Cobertura" else "Ratio de Cobertura (%)",
            template="plotly_white",
            hovermode="x unified",
            height=500
        )
    else:
        # Para cobertura, usar líneas en lugar de área
        fig = px.line(
            filtered_df,
            x='date',
            y=value_col,
            color='category',
            title=title,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        
        fig.update_layout(
            xaxis_title="Fecha",
            yaxis_title="Ratio de Cobertura (%)",
            template="plotly_white",
            hovermode="x unified",
            height=500
        )
        
        # Línea de referencia en 100%
        fig.add_hline(y=100, line_dash="dash", line_color="gray", opacity=0.5)
    
    st.plotly_chart(fig, use_container_width=True)
    
    # ==============================================
    # TABLA RANKING DE CATEGORÍAS
    # ==============================================
    
    st.header("🏆 Ranking de Categorías")
    
    # Calcular ranking para el período seleccionado
    ranking_data = filtered_df.groupby('category').agg({
        exp_col: 'sum',
        imp_col: 'sum'
    }).round(0)
    ranking_data['balance'] = ranking_data[exp_col] - ranking_data[imp_col]
    ranking_data['coverage'] = (ranking_data[exp_col] / ranking_data[imp_col] * 100).round(1)
    
    # Ordenar por la métrica seleccionada
    if analysis_type == "Exportaciones":
        ranking_data = ranking_data.sort_values(exp_col, ascending=False)
    elif analysis_type == "Importaciones":
        ranking_data = ranking_data.sort_values(imp_col, ascending=False)
    elif analysis_type == "Balance":
        ranking_data = ranking_data.sort_values('balance', ascending=False)
    else:
        ranking_data = ranking_data.sort_values('coverage', ascending=False)
    
    # Formatear para mostrar
    display_ranking = ranking_data.copy()
    display_ranking[exp_col] = display_ranking[exp_col].apply(lambda x: format_currency(x))
    display_ranking[imp_col] = display_ranking[imp_col].apply(lambda x: format_currency(x))
    display_ranking['balance'] = display_ranking['balance'].apply(lambda x: format_currency(x))
    display_ranking['coverage'] = display_ranking['coverage'].apply(lambda x: f"{x:.1f}%")
    
    # Renombrar columnas
    column_names = {
        exp_col: 'Exportaciones',
        imp_col: 'Importaciones',
        'balance': 'Balance',
        'coverage': 'Cobertura %'
    }
    display_ranking = display_ranking.rename(columns=column_names)
    
    st.dataframe(
        display_ranking,
        use_container_width=True,
        height=400
    )
    
    # ==============================================
    # FOOTER CATEGORÍAS
    # ==============================================
    
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.caption(f"📅 Período: {year_range_cat[0]}-{year_range_cat[1]}")
    
    with col2:
        st.caption(f"🏷️ {len(selected_categories)} categorías seleccionadas")
    
    with col3:
        if has_prod_kpi:
            st.caption("✅ Con métricas KPI de productos")
        else:
            st.caption("⚠️ Datos base (ejecutar metrics_products.py para KPIs)")

if __name__ == "__main__":
    main() 
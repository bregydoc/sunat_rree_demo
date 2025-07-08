#!/usr/bin/env python
"""Análisis Exploratorio de Datos (EDA) - Observatorio de Comercio Perú"""

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
from pathlib import Path
from datetime import datetime

def run_eda():
    """Ejecuta análisis exploratorio completo"""
    
    # Configurar directorios
    reports_dir = Path("reports/eda")
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    print("🔍 Iniciando Análisis Exploratorio de Datos...")
    
    # Conectar a DuckDB
    con = duckdb.connect("trade.duckdb")
    
    # Cargar datos KPI (si existen, sino usar datos base)
    try:
        df = con.sql("SELECT * FROM kpi_monthly WHERE balance IS NOT NULL").df()
        print(f"   → Usando kpi_monthly: {len(df)} registros")
    except:
        print("   → kpi_monthly no existe, generando desde trade...")
        df = con.sql("""
            SELECT 
                year, month,
                SUM(CASE WHEN flow='export' THEN usd END) as export,
                SUM(CASE WHEN flow='import' THEN usd END) as import,
                SUM(CASE WHEN flow='export' THEN usd END) - 
                SUM(CASE WHEN flow='import' THEN usd END) as balance
            FROM trade 
            WHERE month != 'Total'
            GROUP BY year, month
        """).df()
    
    # Preparar datos temporales
    month_order = [
        "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
        "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
    ]
    
    df['month_num'] = df['month'].map({m: i+1 for i, m in enumerate(month_order)})
    df = df.sort_values(['year', 'month_num'])
    
    # Crear columna de fecha
    df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month_num'].astype(str), format='%Y-%m')
    
    # =====================================================
    # 1. ANÁLISIS DE SERIES TEMPORALES
    # =====================================================
    
    print("📈 1. Generando gráficos de series temporales...")
    
    # Serie temporal principal
    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(
        x=df['date'], y=df['export']/1e9,
        name='Exportaciones', line=dict(color='green', width=2)
    ))
    fig1.add_trace(go.Scatter(
        x=df['date'], y=df['import']/1e9,
        name='Importaciones', line=dict(color='red', width=2)
    ))
    fig1.add_trace(go.Scatter(
        x=df['date'], y=df['balance']/1e9,
        name='Balance', line=dict(color='blue', width=2)
    ))
    
    fig1.update_layout(
        title='📊 Comercio Exterior del Perú (2005-2025)',
        xaxis_title='Año',
        yaxis_title='Miles de Millones USD',
        template='plotly_white',
        hovermode='x unified'
    )
    
    fig1.write_html(reports_dir / "series_temporal.html")
    
    # =====================================================
    # 2. ANÁLISIS DE ESTACIONALIDAD
    # =====================================================
    
    print("🗓️  2. Analizando patrones estacionales...")
    
    # Heatmap año × mes para exportaciones
    heat_data = df.pivot(index='month_num', columns='year', values='export')
    
    fig2 = px.imshow(
        heat_data.values,
        x=heat_data.columns,
        y=[month_order[i-1] for i in heat_data.index],
        aspect='auto',
        color_continuous_scale='RdYlGn',
        title='🌡️ Estacionalidad de Exportaciones (Heatmap)'
    )
    
    fig2.update_layout(
        xaxis_title='Año',
        yaxis_title='Mes'
    )
    
    fig2.write_html(reports_dir / "estacionalidad_heatmap.html")
    
    # Box plot por mes
    fig3 = px.box(
        df, x='month', y='export',
        title='📦 Distribución de Exportaciones por Mes'
    )
    fig3.update_xaxes(tickangle=45)
    fig3.write_html(reports_dir / "distribucion_mensual.html")
    
    # =====================================================
    # 3. ANÁLISIS DE TENDENCIAS
    # =====================================================
    
    print("📊 3. Identificando tendencias y ciclos...")
    
    # Calcular medias móviles
    df['export_ma12'] = df['export'].rolling(12, min_periods=1).mean()
    df['export_ma24'] = df['export'].rolling(24, min_periods=1).mean()
    
    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(
        x=df['date'], y=df['export']/1e9,
        name='Exportaciones', line=dict(color='lightblue', width=1)
    ))
    fig4.add_trace(go.Scatter(
        x=df['date'], y=df['export_ma12']/1e9,
        name='Media Móvil 12m', line=dict(color='blue', width=2)
    ))
    fig4.add_trace(go.Scatter(
        x=df['date'], y=df['export_ma24']/1e9,
        name='Media Móvil 24m', line=dict(color='darkblue', width=2)
    ))
    
    fig4.update_layout(
        title='📈 Tendencias de Exportaciones con Medias Móviles',
        xaxis_title='Año',
        yaxis_title='Miles de Millones USD',
        template='plotly_white'
    )
    
    fig4.write_html(reports_dir / "tendencias.html")
    
    # =====================================================
    # 4. ANÁLISIS DE OUTLIERS Y EVENTOS
    # =====================================================
    
    print("🔍 4. Detectando outliers y eventos atípicos...")
    
    # Variaciones mensuales
    df['export_pct_change'] = df['export'].pct_change() * 100
    df['import_pct_change'] = df['import'].pct_change() * 100
    
    # Detectar outliers (>2 desviaciones estándar)
    export_std = df['export_pct_change'].std()
    export_mean = df['export_pct_change'].mean()
    
    outliers = df[abs(df['export_pct_change'] - export_mean) > 2 * export_std]
    
    fig5 = go.Figure()
    fig5.add_trace(go.Scatter(
        x=df['date'], y=df['export_pct_change'],
        mode='lines+markers',
        name='Variación % Export',
        line=dict(color='green')
    ))
    
    # Marcar outliers
    fig5.add_trace(go.Scatter(
        x=outliers['date'], y=outliers['export_pct_change'],
        mode='markers',
        name='Outliers',
        marker=dict(color='red', size=10, symbol='x')
    ))
    
    fig5.update_layout(
        title='📊 Variaciones Mensuales y Outliers en Exportaciones',
        xaxis_title='Año',
        yaxis_title='Variación % Mensual',
        template='plotly_white'
    )
    
    fig5.write_html(reports_dir / "outliers.html")
    
    # =====================================================
    # 5. DASHBOARD RESUMEN
    # =====================================================
    
    print("📋 5. Creando dashboard resumen...")
    
    # Subplot con múltiples métricas
    fig6 = make_subplots(
        rows=2, cols=2,
        subplot_titles=['Comercio Exterior', 'Balance Comercial', 'Crecimiento YoY', 'Estacionalidad'],
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    # Comercio exterior
    fig6.add_trace(
        go.Scatter(x=df['date'], y=df['export']/1e9, name='Export', line=dict(color='green')),
        row=1, col=1
    )
    fig6.add_trace(
        go.Scatter(x=df['date'], y=df['import']/1e9, name='Import', line=dict(color='red')),
        row=1, col=1
    )
    
    # Balance
    fig6.add_trace(
        go.Scatter(x=df['date'], y=df['balance']/1e9, name='Balance', line=dict(color='blue')),
        row=1, col=2
    )
    
    # Crecimiento YoY
    df['export_yoy'] = df['export'].pct_change(12) * 100
    fig6.add_trace(
        go.Scatter(x=df['date'], y=df['export_yoy'], name='Export YoY%', line=dict(color='orange')),
        row=2, col=1
    )
    
    # Promedio por mes
    monthly_avg = df.groupby('month_num')['export'].mean()/1e9
    fig6.add_trace(
        go.Bar(x=list(range(1,13)), y=monthly_avg.values, name='Promedio Mensual'),
        row=2, col=2
    )
    
    fig6.update_layout(
        title_text='📊 Dashboard EDA - Observatorio Comercio Perú',
        template='plotly_white',
        height=600
    )
    
    fig6.write_html(reports_dir / "dashboard_eda.html")
    
    # =====================================================
    # 6. GENERAR REPORTE RESUMEN
    # =====================================================
    
    print("📝 6. Generando reporte de hallazgos...")
    
    # Estadísticas descriptivas
    stats = {
        'export_mean': df['export'].mean()/1e9,
        'export_std': df['export'].std()/1e9,
        'export_max': df['export'].max()/1e9,
        'export_max_date': df.loc[df['export'].idxmax(), 'date'].strftime('%Y-%m'),
        'balance_positive_months': (df['balance'] > 0).sum(),
        'total_months': len(df),
        'peak_month': df.groupby('month')['export'].mean().idxmax(),
        'low_month': df.groupby('month')['export'].mean().idxmin(),
        'outliers_count': len(outliers)
    }
    
    summary_report = f"""# Reporte EDA - Observatorio de Comercio Perú

## 📊 Estadísticas Generales
- **Exportaciones promedio**: ${stats['export_mean']:.1f}B USD
- **Desviación estándar**: ${stats['export_std']:.1f}B USD
- **Máximo histórico**: ${stats['export_max']:.1f}B USD ({stats['export_max_date']})
- **Meses con superávit**: {stats['balance_positive_months']}/{stats['total_months']} ({stats['balance_positive_months']/stats['total_months']*100:.1f}%)

## 🗓️ Patrones Estacionales
- **Mes pico**: {stats['peak_month']} (mayor promedio de exportaciones)
- **Mes valle**: {stats['low_month']} (menor promedio de exportaciones)

## 🔍 Eventos Atípicos
- **Outliers detectados**: {stats['outliers_count']} eventos
- **Criterio**: Variaciones >2σ respecto a la media

## 📈 Hallazgos Clave
1. **Tendencia general**: Crecimiento sostenido con volatilidad cíclica
2. **Estacionalidad**: Patrones regulares con picos en {stats['peak_month']}
3. **Volatilidad**: Mayor inestabilidad en períodos 2008-2009, 2020-2021
4. **Balance comercial**: Alternancia entre superávit y déficit según coyuntura

## 📋 Archivos Generados
- `series_temporal.html`: Serie temporal principal
- `estacionalidad_heatmap.html`: Mapa de calor estacional
- `distribucion_mensual.html`: Distribución por meses
- `tendencias.html`: Análisis de tendencias
- `outliers.html`: Detección de eventos atípicos
- `dashboard_eda.html`: Dashboard resumen

*Generado el {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
    
    with open(reports_dir / "eda_summary.md", "w", encoding="utf-8") as f:
        f.write(summary_report)
    
    con.close()
    
    print(f"\n✅ EDA completado")
    print(f"   📁 Reportes guardados en: {reports_dir}")
    print(f"   📊 {len(list(reports_dir.glob('*.html')))} gráficos interactivos")
    print(f"   📝 Resumen: {reports_dir}/eda_summary.md")
    
    return df, stats

if __name__ == "__main__":
    run_eda() 
#!/usr/bin/env python
"""Generador de métricas KPI para el Observatorio de Comercio Perú"""

import duckdb
import pandas as pd
from pathlib import Path

def generate_metrics():
    """Genera tabla kpi_monthly con métricas calculadas"""
    
    # Conectar a la base de datos
    con = duckdb.connect("trade.duckdb")
    
    print("📊 Generando métricas KPI...")
    
    # 1) Extraer datos base
    df = con.execute("""
        SELECT
            year,
            month,
            flow,
            usd,
            CASE WHEN flow='export' THEN usd END AS export_usd,
            CASE WHEN flow='import' THEN usd END AS import_usd
        FROM trade
        WHERE month != 'Total'
        ORDER BY year, month, flow
    """).df()
    
    print(f"   → {len(df)} registros extraídos")
    
    # 2) Pivot a columnas export/import
    wide = (
        df.pivot_table(index=['year','month'], columns='flow', values='usd', aggfunc='first')
          .reset_index()
    )
    
    # Renombrar columnas para claridad
    wide.columns.name = None
    if 'export' not in wide.columns:
        wide['export'] = None
    if 'import' not in wide.columns:
        wide['import'] = None
        
    # 3) Calcular balance
    wide['balance'] = wide['export'] - wide['import']
    
    # 4) Crear mapeo de meses para ordenamiento correcto
    month_order = [
        "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
        "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
    ]
    wide['month_num'] = wide['month'].map({m: i+1 for i, m in enumerate(month_order)})
    
    # 5) Ordenar cronológicamente
    wide = wide.sort_values(['year', 'month_num']).reset_index(drop=True)
    
    # 6) Índice base 2005 = 100 (usando enero 2005 como base)
    base_export = wide.query("year == 2005 & month == 'Enero'")['export'].iloc[0]
    base_import = wide.query("year == 2005 & month == 'Enero'")['import'].iloc[0]
    
    wide['idx2005_export'] = (wide['export'] / base_export * 100).round(2)
    wide['idx2005_import'] = (wide['import'] / base_import * 100).round(2)
    
    # 7) Variaciones mensuales y anuales
    wide['export_lag_1'] = wide['export'].shift(1)
    wide['export_lag_12'] = wide['export'].shift(12)
    wide['import_lag_1'] = wide['import'].shift(1)
    wide['import_lag_12'] = wide['import'].shift(12)
    
    # Variaciones porcentuales
    wide['export_mom'] = ((wide['export'] / wide['export_lag_1'] - 1) * 100).round(2)
    wide['export_yoy'] = ((wide['export'] / wide['export_lag_12'] - 1) * 100).round(2)
    wide['import_mom'] = ((wide['import'] / wide['import_lag_1'] - 1) * 100).round(2)
    wide['import_yoy'] = ((wide['import'] / wide['import_lag_12'] - 1) * 100).round(2)
    
    # 8) Promedios móviles 3 meses
    wide['export_ma3'] = wide['export'].rolling(3, min_periods=1).mean().round(0)
    wide['import_ma3'] = wide['import'].rolling(3, min_periods=1).mean().round(0)
    wide['balance_ma3'] = wide['balance'].rolling(3, min_periods=1).mean().round(0)
    
    # 9) Limpiar columnas auxiliares
    final_cols = [
        'year', 'month', 'month_num', 'export', 'import', 'balance',
        'export_mom', 'export_yoy', 'import_mom', 'import_yoy',
        'export_ma3', 'import_ma3', 'balance_ma3',
        'idx2005_export', 'idx2005_import'
    ]
    
    kpi_monthly = wide[final_cols].copy()
    
    print(f"   → {len(kpi_monthly)} registros con métricas calculadas")
    
    # 10) Persistir en DuckDB
    con.execute("CREATE OR REPLACE TABLE kpi_monthly AS SELECT * FROM kpi_monthly")
    
    # 11) Exportar a Parquet
    kpi_monthly.to_parquet("kpi_monthly.parquet", index=False)
    
    # 12) Mostrar resumen
    print("\n📈 Resumen de métricas:")
    latest = kpi_monthly.tail(3)
    for _, row in latest.iterrows():
        print(f"   {row['year']}-{row['month']}: Export=${row['export']:,.0f}M, Balance=${row['balance']:,.0f}M")
    
    con.close()
    print("\n✓ kpi_monthly listo → kpi_monthly.parquet")
    
    return kpi_monthly

if __name__ == "__main__":
    generate_metrics() 
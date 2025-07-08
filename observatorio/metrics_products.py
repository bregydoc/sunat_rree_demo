#!/usr/bin/env python
"""
Construye tabla kpi_prod_monthly con:
  export, import, balance, %_YoY, %_MoM, cov_ratio
"""

import duckdb
import pandas as pd
from pathlib import Path
from rich import print

def generate_product_metrics():
    """Genera métricas KPI por categoría de productos"""
    
    print("[bold cyan]📊 GENERANDO MÉTRICAS DE PRODUCTOS[/]")
    print("=" * 50)
    
    # Conectar a DuckDB
    con = duckdb.connect("trade.duckdb")
    
    # Verificar que existe la tabla trade_prod
    try:
        count = con.execute("SELECT COUNT(*) FROM trade_prod").fetchone()[0]
        print(f"✅ trade_prod encontrada: {count:,} registros")
    except:
        print("[red]❌ Error: No se encontró tabla trade_prod[/]")
        print("[yellow]   → Ejecuta primero: uv run python observatorio/etl_products.py[/]")
        return False
    
    # Extraer datos base
    print("\n📥 Extrayendo datos base...")
    df = con.execute("""
        SELECT year, month, flow, category, usd
        FROM trade_prod 
        WHERE month != 'Total'
        ORDER BY category, year, month
    """).df()
    
    print(f"   → {len(df):,} registros extraídos")
    print(f"   → {df['category'].nunique()} categorías únicas")
    print(f"   → Período: {df['year'].min()}-{df['year'].max()}")
    
    # Mapeo de meses para ordenamiento
    month_order = [
        "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
        "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
    ]
    df['month_num'] = df['month'].map({m: i+1 for i, m in enumerate(month_order)})
    
    # Pivot export/import por categoría
    print("\n🔄 Pivoteando datos por flujo...")
    wide = (df.pivot_table(
        index=["year", "month", "month_num", "category"],
        columns="flow", 
        values="usd",
        aggfunc='sum'
    ).reset_index())
    
    # Limpiar nombres de columnas
    wide.columns.name = None
    if 'export' not in wide.columns:
        wide['export'] = 0
    if 'import' not in wide.columns:
        wide['import'] = 0
    
    # Renombrar para consistencia
    wide = wide.rename(columns={"export": "exp", "import": "imp"})
    
    # Calcular métricas básicas
    print("\n🧮 Calculando métricas básicas...")
    wide["balance"] = wide["exp"] - wide["imp"]
    wide["cov_ratio"] = (wide["exp"] / wide["imp"]).replace([float('inf'), -float('inf')], None).round(4)
    
    # Ordenar por categoría, año y mes
    wide = wide.sort_values(["category", "year", "month_num"]).reset_index(drop=True)
    
    # Calcular lags para variaciones YoY y MoM
    print("\n📈 Calculando variaciones temporales...")
    
    # Lags por categoría
    wide["exp_lag1"] = wide.groupby("category")["exp"].shift(1)    # MoM
    wide["exp_lag12"] = wide.groupby("category")["exp"].shift(12)  # YoY
    wide["imp_lag1"] = wide.groupby("category")["imp"].shift(1)    # MoM
    wide["imp_lag12"] = wide.groupby("category")["imp"].shift(12)  # YoY
    
    # Variaciones porcentuales
    wide["exp_mom"] = ((wide["exp"] / wide["exp_lag1"] - 1) * 100).round(2)
    wide["exp_yoy"] = ((wide["exp"] / wide["exp_lag12"] - 1) * 100).round(2)
    wide["imp_mom"] = ((wide["imp"] / wide["imp_lag1"] - 1) * 100).round(2)
    wide["imp_yoy"] = ((wide["imp"] / wide["imp_lag12"] - 1) * 100).round(2)
    
    # Promedios móviles por categoría
    print("\n📊 Calculando promedios móviles...")
    wide["exp_ma3"] = wide.groupby("category")["exp"].rolling(3, min_periods=1).mean().reset_index(0, drop=True).round(0)
    wide["imp_ma3"] = wide.groupby("category")["imp"].rolling(3, min_periods=1).mean().reset_index(0, drop=True).round(0)
    wide["balance_ma3"] = wide.groupby("category")["balance"].rolling(3, min_periods=1).mean().reset_index(0, drop=True).round(0)
    
    # Índices base por categoría (primer año disponible = 100)
    print("\n📈 Calculando índices base...")
    def calculate_base_index(group):
        first_exp = group['exp'].iloc[0] if len(group) > 0 and group['exp'].iloc[0] > 0 else 1
        first_imp = group['imp'].iloc[0] if len(group) > 0 and group['imp'].iloc[0] > 0 else 1
        group['idx_exp'] = (group['exp'] / first_exp * 100).round(2)
        group['idx_imp'] = (group['imp'] / first_imp * 100).round(2)
        return group
    
    wide = wide.groupby("category").apply(calculate_base_index).reset_index(drop=True)
    
    # Seleccionar columnas finales
    final_cols = [
        'year', 'month', 'month_num', 'category', 
        'exp', 'imp', 'balance', 'cov_ratio',
        'exp_mom', 'exp_yoy', 'imp_mom', 'imp_yoy',
        'exp_ma3', 'imp_ma3', 'balance_ma3',
        'idx_exp', 'idx_imp'
    ]
    
    kpi_prod = wide[final_cols].copy()
    
    # Persistir en DuckDB
    print("\n💾 Guardando en DuckDB...")
    con.execute("CREATE OR REPLACE TABLE kpi_prod_monthly AS SELECT * FROM kpi_prod")
    
    # Exportar a Parquet
    print("💾 Guardando en Parquet...")
    kpi_prod.to_parquet("kpi_prod_monthly.parquet", index=False)
    
    # Estadísticas finales
    print(f"\n📊 [bold]Métricas generadas:[/]")
    print(f"   → {len(kpi_prod):,} registros con KPIs")
    print(f"   → {kpi_prod['category'].nunique()} categorías procesadas")
    print(f"   → Período: {kpi_prod['year'].min()}-{kpi_prod['year'].max()}")
    
    # Top categorías por exportación (último año)
    print(f"\n🏆 [bold]Top 5 categorías exportadoras ({kpi_prod['year'].max()}):[/]")
    last_year = kpi_prod['year'].max()
    top_cats = (kpi_prod[kpi_prod['year'] == last_year]
                .groupby('category')['exp']
                .sum()
                .sort_values(ascending=False)
                .head(5))
    
    for i, (cat, value) in enumerate(top_cats.items(), 1):
        cat_short = cat[:40] + "..." if len(cat) > 40 else cat
        print(f"   {i}. {cat_short}: ${value/1e6:,.0f}M")
    
    con.close()
    print("\n[bold green]✓ kpi_prod_monthly generado exitosamente[/]")
    return True

if __name__ == "__main__":
    generate_product_metrics() 
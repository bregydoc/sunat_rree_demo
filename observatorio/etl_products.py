#!/usr/bin/env python
"""
ETL de Importaciones / Exportaciones por categoría (2005-2025)
Genera tabla trade_prod en trade.duckdb y archivo trade_prod.parquet
"""

import duckdb
import pandas as pd
import re
from pathlib import Path
from rich import print, box
from rich.table import Table
from rich.console import Console

MONTHS = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
          "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]

SRC = {
    "import": Path("data/cdro_F1.xlsx"),   # libro F1: importaciones
    "export": Path("data/cdro_G1.xlsx"),   # libro G1: exportaciones
}

def parse_book(path: Path, flow: str) -> pd.DataFrame:
    """Convierte cada hoja-año en formato largo con columna category"""
    records = []
    xls = pd.ExcelFile(path)
    
    print(f"📖 Procesando {flow}: {path.name}")
    
    for sheet in xls.sheet_names:
        if not re.fullmatch(r"\d{4}", sheet):
            continue
            
        year = int(sheet)
        print(f"   → Año {year}")
        df = xls.parse(sheet, header=None)

        # 1. Localizar fila de encabezados (donde está 'Enero')
        try:
            head_idx = next(i for i, row in df.iterrows() 
                          if any(str(cell).strip() == "Enero" for cell in row))
        except StopIteration:
            print(f"   ⚠️  No se encontró encabezado 'Enero' en {year}")
            continue
            
        # Mapear columnas de meses
        col_map = {}
        for c in range(df.shape[1]):
            cell_val = str(df.iat[head_idx, c]).strip()
            if cell_val in MONTHS + ["Total"]:
                col_map[c] = cell_val

        if not col_map:
            print(f"   ⚠️  No se encontraron columnas de meses en {year}")
            continue

        # 2. Iterar filas-categoría a partir de head_idx+3
        categories_found = 0
        for r in range(head_idx + 3, df.shape[0]):
            if r >= len(df):
                break
                
            # Obtener categoría de la columna 2 (generalmente donde está el nombre)
            cat_cell = df.iat[r, 2] if df.shape[1] > 2 else df.iat[r, 0]
            cat = str(cat_cell).strip()
            
            # Filtrar filas vacías o metadata
            if (not cat or 
                cat.lower() in ['nan', 'none', ''] or
                cat.lower().startswith("incluye") or
                cat.lower().startswith("total") or
                len(cat) < 3):
                continue
                
            categories_found += 1
            
            # Extraer valores por mes
            for col, mes in col_map.items():
                try:
                    val = df.iat[r, col]
                    if pd.isna(val) or val == '' or val == 0:
                        continue
                        
                    records.append({
                        "year": year,
                        "month": mes,
                        "flow": flow,
                        "category": cat,
                        "usd": float(val),
                    })
                except (ValueError, IndexError):
                    continue
        
        print(f"      {categories_found} categorías encontradas")
    
    df_result = pd.DataFrame(records)
    print(f"   📊 Total registros: {len(df_result)}")
    return df_result

def qa_totals(df_long: pd.DataFrame) -> None:
    """Compara suma de los 12 meses vs. Total anual por (year,flow,category)"""
    print("\n🔍 Ejecutando QA de totales...")
    
    # Suma mensual por (year, flow, category)
    df_monthly = (df_long[df_long["month"] != "Total"]
                 .groupby(["year", "flow", "category"])["usd"]
                 .sum()
                 .reset_index()
                 .rename(columns={"usd": "sum_months"}))
    
    # Totales anuales
    df_total = (df_long[df_long["month"] == "Total"]
               .groupby(["year", "flow", "category"])["usd"]
               .sum()
               .reset_index()
               .rename(columns={"usd": "usd_total"}))
    
    # Comparar
    if df_total.empty:
        print("[yellow]⚠️  No se encontraron filas 'Total' - saltando QA[/]")
        return
        
    joined = df_monthly.merge(df_total, on=["year", "flow", "category"], how="left")
    joined["Δ"] = joined["usd_total"] - joined["sum_months"]
    
    # Filtrar diferencias significativas
    bad = joined[joined["Δ"].abs() > 1e-3]
    
    if bad.empty:
        print("[green]✓ QA: Totales anuales coinciden con suma de meses[/]")
    else:
        print(f"[yellow]⚠️  {len(bad)} discrepancias encontradas (diferencias > $1K)[/]")
        
        # Mostrar solo las 5 peores
        worst = bad.nlargest(5, "Δ")
        tbl = Table(title="❌ QA: diferencias detectadas", box=box.SIMPLE)
        for col in ["year", "flow", "category", "usd_total", "sum_months", "Δ"]:
            tbl.add_column(col)
            
        for _, row in worst.iterrows():
            tbl.add_row(
                str(int(row["year"])),
                row["flow"],
                row["category"][:30] + "..." if len(row["category"]) > 30 else row["category"],
                f"{row['usd_total']:,.0f}" if pd.notna(row['usd_total']) else "N/A",
                f"{row['sum_months']:,.0f}",
                f"{row['Δ']:,.0f}"
            )
        
        Console().print(tbl)
        print(f"[yellow]Continuando con {len(bad)} discrepancias menores...[/]")

def main():
    print("[bold cyan]🇵🇪 ETL DE PRODUCTOS - OBSERVATORIO COMERCIO PERÚ[/]")
    print("=" * 60)
    
    # Verificar archivos
    for flow, path in SRC.items():
        if not path.exists():
            print(f"[red]❌ Archivo no encontrado: {path}[/]")
            return
        print(f"[green]✅ {flow.title()}: {path}[/]")
    
    print()
    
    # Procesar archivos
    frames = []
    for flow, path in SRC.items():
        try:
            df = parse_book(path, flow)
            if not df.empty:
                frames.append(df)
            else:
                print(f"[yellow]⚠️  No se extrajeron datos de {flow}[/]")
        except Exception as e:
            print(f"[red]❌ Error procesando {flow}: {e}[/]")
            continue
    
    if not frames:
        print("[red]❌ No se pudo procesar ningún archivo[/]")
        return
    
    # Consolidar datos
    df = pd.concat(frames, ignore_index=True)
    print(f"\n📊 [bold]Total registros consolidados: {len(df)}[/]")
    print(f"📅 Años: {df['year'].min()}-{df['year'].max()}")
    print(f"🏷️  Categorías únicas: {df['category'].nunique()}")
    print(f"💰 Valor total: ${df['usd'].sum()/1e9:.1f}B USD")
    
    # QA
    qa_totals(df)
    
    # Persistencia
    print("\n💾 Guardando datos...")
    try:
        con = duckdb.connect("trade.duckdb")
        con.execute("CREATE OR REPLACE TABLE trade_prod AS SELECT * FROM df")
        con.close()
        print("[green]✅ DuckDB: trade_prod creada[/]")
        
        df.to_parquet("trade_prod.parquet", index=False)
        print("[green]✅ Parquet: trade_prod.parquet generado[/]")
        
        print("\n[bold green]✓ trade_prod listo → parquet y duckdb[/]")
        
    except Exception as e:
        print(f"[red]❌ Error guardando: {e}[/]")

if __name__ == "__main__":
    main() 
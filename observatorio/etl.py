#!/usr/bin/env python
"""ETL para Observatorio de Comercio Perú (2005-2025)"""

import re
import duckdb
import pandas as pd
from pathlib import Path
from rich.console import Console
from rich.table import Table

MONTHS = [
    "Enero","Febrero","Marzo","Abril","Mayo","Junio",
    "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"
]
SRC = {
    "import": Path("data/cdro_F8.xlsx"),
    "export": Path("data/cdro_G6.xlsx"),
}

def parse_book(path: Path, flow: str) -> pd.DataFrame:
    """Extrae la fila 'Total general' de cada hoja-año y la convierte a formato largo"""
    tidy = []
    xls = pd.ExcelFile(path)
    for sheet in xls.sheet_names:
        if not re.fullmatch(r"\d{4}", sheet):
            continue                       # solo hojas con nombre tipo 2014
        year = int(sheet)
        df = xls.parse(sheet, header=None)

        # — localizar la fila de encabezados (donde aparece 'Enero') —
        hdr_idx = next(
            idx for idx, row in df.iterrows()
            if any(str(v).strip() == "Enero" for v in row)
        )
        header = df.loc[hdr_idx]
        col_meses = [i for i,v in header.items() if str(v).strip() in MONTHS]
        col_total = header[header=="Total"].index[0]

        # — localizar la fila 'Total general' —
        tot_idx = next(
            idx for idx, row in df.iterrows()
            if any(isinstance(v,str) and "Total general" in v for v in row)
        )

        # — registrar valores —
        for col in col_meses:
            tidy.append({
                "year": year,
                "month": header[col].strip(),
                "flow" : flow,
                "usd"  : float(df.iat[tot_idx, col]),
            })

        tidy.append({                        # fila anual para QA
            "year": year, "month":"Total", "flow":flow,
            "usd" : float(df.iat[tot_idx, col_total]),
            "sum_months": float(df.iloc[tot_idx, col_meses].sum())
        })
    return pd.DataFrame(tidy)

def qa_report(df_tot: pd.DataFrame):
    """Imprime diferencias entre suma mensual y total anual"""
    console = Console()
    table = Table(title="QA: Total anual vs. suma de meses")
    for col in ["Año","Flujo","Total libro","Suma meses","Δ"]:
        table.add_column(col, justify="right")
    for _,row in df_tot.iterrows():
        diff = row["usd"]-row["sum_months"]
        table.add_row(
            str(int(row["year"])), row["flow"],
            f"{row['usd']:,.0f}", f"{row['sum_months']:,.0f}",
            f"{diff:,.2f}"
        )
    console.print(table)

def main():
    # 1) Ingesta
    frames = [parse_book(path, flow) for flow, path in SRC.items()]
    df = pd.concat(frames, ignore_index=True)

    # 2) QA
    df_tot = df[df["month"]=="Total"].copy()
    qa_report(df_tot)

    # 3) Persistencia (duckdb + parquet opcional)
    con = duckdb.connect("trade.duckdb")
    con.execute("CREATE OR REPLACE TABLE trade AS SELECT * FROM df")
    con.close()
    df.to_parquet("trade.parquet", index=False)
    print("\n✓ ETL completado → trade.duckdb & trade.parquet")

if __name__ == "__main__":
    main() 
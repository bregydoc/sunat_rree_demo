# Observatorio de Comercio Perú (2005-2025)

ETL para procesar datos de importaciones y exportaciones del Perú desde hojas de Excel hacia un formato estructurado.

## Estructura del proyecto

```
observatorio/
├─ data/
│  ├─ cdro_F8.xlsx          # importaciones 2005-2025
│  └─ cdro_G6.xlsx          # exportaciones 2005-2025
├─ etl.py                   # script que unifica y valida
├─ trade.duckdb             # se crea al correr el ETL
└─ README.md
```

## Requisitos

- Python >= 3.10
- Dependencias: `pandas`, `duckdb`, `openpyxl`, `rich`

## Instalación

### Opción 1: Con virtual environment (recomendado)
```bash
cd observatorio
python3 -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate
pip install pandas duckdb openpyxl rich
```

### Opción 2: Instalación global
```bash
pip install pandas duckdb openpyxl rich
```

## Uso

1. Coloca los archivos Excel en la carpeta `data/`:
   - `cdro_F8.xlsx` (importaciones)
   - `cdro_G6.xlsx` (exportaciones)

2. Ejecuta el ETL:
   ```bash
   python etl.py
   ```

3. El script generará:
   - `trade.duckdb` - Base de datos para consultas SQL ultrarápidas
   - `trade.parquet` - Archivo comprimido y portable
   - Reporte QA en consola

## Funcionalidad del ETL

- Lee las 21 hojas-año de cada libro Excel
- Detecta automáticamente la fila de encabezados y "Total general"
- Produce un DataFrame en formato largo con ~504 registros (year, month, flow, usd)
- Genera reporte QA comparando suma mensual vs total anual
- Guarda resultados en DuckDB y Parquet

## Consultas de ejemplo

```sql
-- Saldo comercial anual
SELECT
  year,
  SUM(CASE WHEN flow='export' THEN usd END) AS export_usd,
  SUM(CASE WHEN flow='import' THEN usd END) AS import_usd,
  SUM(CASE WHEN flow='export' THEN usd END) -
  SUM(CASE WHEN flow='import' THEN usd END) AS balance_usd
FROM trade
WHERE month != 'Total'
GROUP BY 1
ORDER BY 1;
``` 
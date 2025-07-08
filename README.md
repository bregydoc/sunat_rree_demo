# 🇵🇪 Observatorio de Comercio Exterior del Perú

Pipeline completo de análisis de datos de comercio exterior peruano (2005-2025) con ETL, métricas KPI, análisis exploratorio y dashboard web interactivo.

## 🏗️ Estructura del Proyecto

```
observatorio/
├── 📁 data/
│   ├── cdro_F8.xlsx          # Importaciones 2005-2025
│   └── cdro_G6.xlsx          # Exportaciones 2005-2025
├── 📄 etl.py                 # Pipeline ETL principal
├── 📄 metrics.py             # Generador de métricas KPI
├── 📄 eda.py                 # Análisis exploratorio
├── 📁 models/
│   └── metrics.sql           # Definiciones SQL reutilizables
└── 📄 README.md              # Documentación del observatorio

📁 reports/
└── 📁 eda/                   # Reportes de análisis exploratorio
    ├── series_temporal.html
    ├── estacionalidad_heatmap.html
    ├── tendencias.html
    ├── outliers.html
    ├── dashboard_eda.html
    └── eda_summary.md

📄 app.py                     # Dashboard web Streamlit
📄 trade.duckdb              # Base de datos principal
📄 kpi_monthly.parquet        # Métricas KPI calculadas
📄 pyproject.toml             # Configuración del proyecto
```

## 🚀 Instalación y Configuración

### Requisitos
- Python >= 3.10
- [uv](https://docs.astral.sh/uv/) (recomendado) o pip

### 1. Instalación con uv (recomendado)
```bash
# Clonar e instalar dependencias
git clone <repo>
cd sunat_rree_demo
uv sync
```

### 2. Instalación con pip
```bash
pip install pandas duckdb openpyxl rich pyarrow plotly streamlit numpy statsmodels
```

## 📊 Pipeline de Datos

### Paso 1: ETL (Extracción, Transformación, Carga)

```bash
# Ejecutar pipeline ETL
uv run python observatorio/etl.py
```

**¿Qué hace?**
- ✅ Lee 21 hojas-año de importaciones (`cdro_F8.xlsx`)
- ✅ Lee 21 hojas-año de exportaciones (`cdro_G6.xlsx`)
- ✅ Detecta automáticamente filas "Total general" y encabezados
- ✅ Convierte a formato largo con ~519 registros
- ✅ Genera reporte QA comparando sumas mensuales vs totales anuales
- ✅ Exporta a `trade.duckdb` y `trade.parquet`

### Paso 2: Métricas KPI

```bash
# Generar métricas avanzadas
uv run python observatorio/metrics.py
```

**Métricas calculadas:**
- 📈 **Variaciones**: MoM (mensual) y YoY (anual)
- 📊 **Índices**: Base 2005 = 100
- 📉 **Promedios móviles**: 3 meses
- 💰 **Balance comercial**: Export - Import
- 🎯 **KPIs**: Exportados a `kpi_monthly.parquet`

### Paso 3: Análisis Exploratorio (EDA)

```bash
# Ejecutar análisis exploratorio
uv run python observatorio/eda.py
```

**Análisis generados:**
- 📈 **Series temporales**: Tendencias y ciclos
- 🗓️ **Estacionalidad**: Heatmaps año × mes
- 🔍 **Outliers**: Detección de eventos atípicos
- 📊 **Dashboard EDA**: Resumen interactivo
- 📝 **Reporte**: Hallazgos clave en Markdown

*Reportes guardados en `reports/eda/`*

### Paso 4: Dashboard Web

```bash
# Lanzar dashboard interactivo
uv run streamlit run app.py
```

🌐 **Accede a:** `http://localhost:8501`

**Características del dashboard:**
- 📊 **Métricas YTD**: Exportaciones, importaciones, balance
- 📈 **Gráficos interactivos**: Series temporales con Plotly
- ⚙️ **Filtros**: Rango de años, tipo de flujo
- 📅 **Estacionalidad**: Heatmaps y distribuciones
- 📋 **Tablas**: Datos detallados con formato

## 🗃️ Base de Datos

### Consultas SQL de Ejemplo

```sql
-- Conectar a DuckDB
duckdb trade.duckdb

-- Balance comercial anual
SELECT
    year,
    SUM(CASE WHEN flow='export' THEN usd END) / 1e9 AS export_billions,
    SUM(CASE WHEN flow='import' THEN usd END) / 1e9 AS import_billions,
    (SUM(CASE WHEN flow='export' THEN usd END) - 
     SUM(CASE WHEN flow='import' THEN usd END)) / 1e9 AS balance_billions
FROM trade
WHERE month != 'Total'
GROUP BY year
ORDER BY year;

-- Top 5 meses con mayor exportación
SELECT year, month, usd/1e9 as export_billions
FROM trade 
WHERE flow = 'export' AND month != 'Total'
ORDER BY usd DESC 
LIMIT 5;

-- Métricas KPI (si se ejecutó metrics.py)
SELECT * FROM kpi_monthly 
WHERE year >= 2020 
ORDER BY year, month_num;
```

### Vistas SQL Disponibles

Si ejecutaste `observatorio/models/metrics.sql`:
- `base_monthly`: Datos pivoteados por flujo
- `metrics_windowed`: Métricas con ventanas deslizantes
- `quarterly_summary`: Resumen trimestral
- `annual_performance`: Rendimiento anual

## 📈 Resultados Clave

### Estadísticas Principales
- **📊 Registros procesados**: 519 total
- **📅 Período cubierto**: 2005-2025
- **💰 Pico exportaciones**: $73.6B USD (2024)
- **📈 Tendencia**: Crecimiento sostenido con volatilidad cíclica

### Hallazgos del Análisis
1. **🗓️ Estacionalidad**: Patrones regulares con picos en diciembre
2. **📊 Balance comercial**: Alternancia superávit/déficit según coyuntura
3. **🔍 Outliers**: Mayor volatilidad en 2008-2009, 2020-2021
4. **📈 Crecimiento**: Tendencia alcista con medias móviles

## 🔧 Comandos Útiles

```bash
# Pipeline completo automatizado
uv run python observatorio/etl.py && \
uv run python observatorio/metrics.py && \
uv run python observatorio/eda.py && \
uv run streamlit run app.py

# Verificar base de datos
duckdb trade.duckdb "DESCRIBE trade; SELECT COUNT(*) FROM trade;"

# Verificar métricas KPI
duckdb trade.duckdb "DESCRIBE kpi_monthly; SELECT * FROM kpi_monthly LIMIT 5;"

# Ver reportes EDA
open reports/eda/dashboard_eda.html
```

## 🚀 Despliegue

### Streamlit Cloud
```bash
# 1. Subir a GitHub
git add . && git commit -m "Observatorio completo" && git push

# 2. Conectar en https://share.streamlit.io/
# 3. Seleccionar archivo: app.py
```

### Render.com / Railway
```bash
# Agregar al proyecto:
echo "web: streamlit run app.py --server.port=$PORT --server.address=0.0.0.0" > Procfile
```

## 🤝 Contribuir

1. **Fork** el repositorio
2. **Crear** rama: `git checkout -b feature/nueva-metrica`
3. **Commit**: `git commit -am 'Agregar nueva métrica'`
4. **Push**: `git push origin feature/nueva-metrica`
5. **Pull Request**

## 📄 Licencia

MIT License - Ver `LICENSE` para detalles.

## 📞 Contacto

- **Autor**: Bregy Doc
- **Proyecto**: Observatorio de Comercio Exterior del Perú
- **Tecnologías**: Python, DuckDB, Streamlit, Plotly, Pandas

---

*🇵🇪 Desarrollado con ❤️ para el análisis del comercio exterior peruano*

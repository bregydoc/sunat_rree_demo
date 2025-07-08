-- =====================================================
-- Definiciones SQL reutilizables para métricas KPI
-- =====================================================

-- Vista base: datos pivoteados por flujo
CREATE OR REPLACE VIEW base_monthly AS
SELECT 
    year,
    month,
    CASE 
        WHEN month = 'Enero' THEN 1
        WHEN month = 'Febrero' THEN 2
        WHEN month = 'Marzo' THEN 3
        WHEN month = 'Abril' THEN 4
        WHEN month = 'Mayo' THEN 5
        WHEN month = 'Junio' THEN 6
        WHEN month = 'Julio' THEN 7
        WHEN month = 'Agosto' THEN 8
        WHEN month = 'Septiembre' THEN 9
        WHEN month = 'Octubre' THEN 10
        WHEN month = 'Noviembre' THEN 11
        WHEN month = 'Diciembre' THEN 12
    END as month_num,
    SUM(CASE WHEN flow = 'export' THEN usd END) as export,
    SUM(CASE WHEN flow = 'import' THEN usd END) as import
FROM trade 
WHERE month != 'Total'
GROUP BY year, month
ORDER BY year, month_num;

-- Métricas con ventanas deslizantes
CREATE OR REPLACE VIEW metrics_windowed AS
SELECT *,
    -- Balance comercial
    export - import as balance,
    
    -- Variaciones mensuales (MoM)
    ROUND((export / LAG(export, 1) OVER (ORDER BY year, month_num) - 1) * 100, 2) as export_mom,
    ROUND((import / LAG(import, 1) OVER (ORDER BY year, month_num) - 1) * 100, 2) as import_mom,
    
    -- Variaciones anuales (YoY)
    ROUND((export / LAG(export, 12) OVER (ORDER BY year, month_num) - 1) * 100, 2) as export_yoy,
    ROUND((import / LAG(import, 12) OVER (ORDER BY year, month_num) - 1) * 100, 2) as import_yoy,
    
    -- Promedios móviles 3 meses
    ROUND(AVG(export) OVER (ORDER BY year, month_num ROWS 2 PRECEDING), 0) as export_ma3,
    ROUND(AVG(import) OVER (ORDER BY year, month_num ROWS 2 PRECEDING), 0) as import_ma3,
    
    -- Índices base 2005=100
    ROUND(export / FIRST_VALUE(export) OVER (ORDER BY year, month_num) * 100, 2) as idx2005_export,
    ROUND(import / FIRST_VALUE(import) OVER (ORDER BY year, month_num) * 100, 2) as idx2005_import

FROM base_monthly;

-- Query para exportaciones por trimestre
CREATE OR REPLACE VIEW quarterly_summary AS
SELECT 
    year,
    CASE 
        WHEN month_num BETWEEN 1 AND 3 THEN 'Q1'
        WHEN month_num BETWEEN 4 AND 6 THEN 'Q2'
        WHEN month_num BETWEEN 7 AND 9 THEN 'Q3'
        ELSE 'Q4'
    END as quarter,
    ROUND(SUM(export)/1000000, 1) as export_usd_millions,
    ROUND(SUM(import)/1000000, 1) as import_usd_millions,
    ROUND(SUM(export - import)/1000000, 1) as balance_usd_millions
FROM base_monthly
GROUP BY year, quarter
ORDER BY year, quarter;

-- Top/bottom performers por año
CREATE OR REPLACE VIEW annual_performance AS
SELECT 
    year,
    ROUND(SUM(export)/1000000000, 2) as export_usd_billions,
    ROUND(SUM(import)/1000000000, 2) as import_usd_billions,
    ROUND(SUM(export - import)/1000000000, 2) as balance_usd_billions,
    ROUND((SUM(export) / LAG(SUM(export)) OVER (ORDER BY year) - 1) * 100, 1) as export_yoy_growth
FROM base_monthly
GROUP BY year
ORDER BY year; 
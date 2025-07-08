#!/usr/bin/env python
"""
Tests unitarios para el motor de insights
"""

import pandas as pd
import pytest
from insights_engine import build_insights, build_summary_insights, get_quick_stats, _format_currency, _get_trend_emoji

def test_build_insights_non_empty():
    """Test que build_insights devuelve resultados con datos válidos"""
    # DataFrame mínimo dummy
    df = pd.DataFrame({
        'year': [2025, 2025, 2024, 2024],
        'month': ['Enero', 'Febrero', 'Enero', 'Febrero'],
        'category': ['A', 'B', 'A', 'B'],
        'exp_yoy': [10.5, -5.2, 8.0, -3.1],
        'balance': [1e6, -2e6, 0.8e6, -1.5e6]
    })
    
    insights = build_insights(df, top_n=2)
    
    assert len(insights) > 0, "build_insights no devolvió resultados"
    assert isinstance(insights, list), "build_insights debe devolver una lista"
    assert len(insights) <= 2, "build_insights devolvió más resultados de los solicitados"
    
    # Verificar que contiene texto de insights
    for insight in insights:
        assert isinstance(insight, str), "Cada insight debe ser string"
        assert "Hallazgo" in insight, "Insight debe contener sección Hallazgo"
        assert "Impacto" in insight, "Insight debe contener sección Impacto"
        assert "Acción" in insight, "Insight debe contener sección Acción"

def test_build_insights_empty_data():
    """Test que build_insights maneja datos vacíos correctamente"""
    df_empty = pd.DataFrame()
    
    insights = build_insights(df_empty)
    
    assert len(insights) > 0, "Debe devolver al menos un mensaje de error"
    assert "Sin datos" in insights[0], "Debe indicar que no hay datos"

def test_build_insights_missing_columns():
    """Test que build_insights maneja columnas faltantes"""
    df = pd.DataFrame({
        'year': [2025],
        'month': ['Enero'],
        'category': ['Test'],
        # Falta exp_yoy y balance
    })
    
    insights = build_insights(df)
    
    assert len(insights) > 0, "Debe manejar columnas faltantes"

def test_build_summary_insights():
    """Test para el resumen ejecutivo"""
    df_country = pd.DataFrame({
        'year': [2025],
        'export': [50e9],
        'import': [40e9]
    })
    
    df_products = pd.DataFrame({
        'year': [2025],
        'category': ['Minería'],
        'exp': [30e9]
    })
    
    summary = build_summary_insights(df_country, df_products)
    
    assert len(summary) > 0, "Debe generar resumen ejecutivo"
    assert "Resumen Ejecutivo" in summary[0], "Debe contener título de resumen"
    assert "50.0B" in summary[0], "Debe mostrar exportaciones formateadas"

def test_get_quick_stats():
    """Test para estadísticas rápidas"""
    df = pd.DataFrame({
        'year': [2025, 2025],
        'category': ['A', 'B'],
        'month': ['Enero', 'Febrero'],
        'exp': [100, 200],
        'exp_yoy': [10, -5]
    })
    
    stats = get_quick_stats(df)
    
    assert isinstance(stats, dict), "Debe devolver diccionario"
    assert "año_actual" in stats, "Debe incluir año actual"
    assert stats["año_actual"] == 2025, "Año actual correcto"
    assert "categorías_activas" in stats, "Debe incluir número de categorías"

def test_get_quick_stats_empty():
    """Test que get_quick_stats maneja datos vacíos"""
    df_empty = pd.DataFrame()
    
    stats = get_quick_stats(df_empty)
    
    assert "error" in stats, "Debe indicar error con datos vacíos"

def test_format_currency():
    """Test para formateo de moneda"""
    assert _format_currency(1e9) == "1.0B"
    assert _format_currency(500e6) == "500.0M"
    assert _format_currency(1000) == "1.0K"
    assert _format_currency(-2e9) == "-2.0B"

def test_get_trend_emoji():
    """Test para emojis de tendencia"""
    assert _get_trend_emoji(15) == "🚀"  # Alto crecimiento
    assert _get_trend_emoji(5) == "📈"   # Crecimiento moderado
    assert _get_trend_emoji(-3) == "📉"  # Decrecimiento moderado
    assert _get_trend_emoji(-15) == "⚠️" # Alto decrecimiento

def test_insights_data_quality():
    """Test de calidad de datos en insights"""
    df = pd.DataFrame({
        'year': [2025, 2025],
        'month': ['Enero', 'Febrero'],
        'category': ['Productos Tradicionales', 'Productos No Tradicionales'],
        'exp_yoy': [12.5, -8.3],
        'balance': [5e9, -2e9]
    })
    
    insights = build_insights(df, top_n=2)
    
    # Verificar que los insights contienen datos relevantes
    assert len(insights) == 2, "Debe generar exactamente 2 insights"
    
    # Verificar que el primer insight tiene el mayor cambio absoluto YoY
    first_insight = insights[0]
    assert "12.5%" in first_insight, "Primer insight debe mostrar el mayor cambio YoY"

if __name__ == "__main__":
    """Ejecutar tests directamente"""
    print("🧪 EJECUTANDO TESTS DE INSIGHTS ENGINE")
    print("=" * 50)
    
    try:
        test_build_insights_non_empty()
        print("✅ test_build_insights_non_empty")
        
        test_build_insights_empty_data()
        print("✅ test_build_insights_empty_data")
        
        test_build_insights_missing_columns()
        print("✅ test_build_insights_missing_columns")
        
        test_build_summary_insights()
        print("✅ test_build_summary_insights")
        
        test_get_quick_stats()
        print("✅ test_get_quick_stats")
        
        test_get_quick_stats_empty()
        print("✅ test_get_quick_stats_empty")
        
        test_format_currency()
        print("✅ test_format_currency")
        
        test_get_trend_emoji()
        print("✅ test_get_trend_emoji")
        
        test_insights_data_quality()
        print("✅ test_insights_data_quality")
        
        print("\n🎉 Todos los tests pasaron exitosamente!")
        
    except Exception as e:
        print(f"\n❌ Error en test: {e}")
        exit(1) 
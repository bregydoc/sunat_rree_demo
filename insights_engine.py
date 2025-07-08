import pandas as pd
import calendar
import datetime as dt
import textwrap
from typing import List, Dict, Any

def _month_name(mes_str: str) -> str:
    """Convierte 'Enero' → 'Jan' para narrativa corta"""
    months_es = [
        "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
        "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
    ]
    try:
        idx = months_es.index(mes_str) + 1
        return dt.date(1900, idx, 1).strftime('%b')
    except ValueError:
        return mes_str[:3]

def _format_currency(value: float) -> str:
    """Formatea valores monetarios en M o B"""
    if abs(value) >= 1e9:
        return f"{value/1e9:.1f}B"
    elif abs(value) >= 1e6:
        return f"{value/1e6:.1f}M"
    else:
        return f"{value/1e3:.1f}K"

def _get_trend_emoji(yoy_change: float) -> str:
    """Devuelve emoji según el cambio YoY"""
    if yoy_change > 10:
        return "🚀"
    elif yoy_change > 0:
        return "📈"
    elif yoy_change > -10:
        return "📉"
    else:
        return "⚠️"

def build_insights(df_view: pd.DataFrame, top_n: int = 3) -> List[str]:
    """
    Devuelve una lista de strings Markdown con los n hallazgos
    más 'interesantes' (ordenados por |%YoY_exp| desc).
    """
    if df_view.empty:
        return ["📊 **Sin datos para el período seleccionado**\n\nAjusta los filtros para ver insights."]
    
    latest_year = df_view['year'].max()
    sub = df_view[df_view['year'] == latest_year].copy()
    
    if sub.empty:
        return ["📊 **Sin datos para el año más reciente**\n\nSelecciona un rango de años más amplio."]
    
    # Verificar si tenemos las columnas necesarias
    required_cols = ['category', 'exp_yoy', 'balance']
    if 'exp_yoy' not in sub.columns:
        # Usar columnas alternativas si existen
        if '%YoY_exp' in sub.columns:
            sub['exp_yoy'] = sub['%YoY_exp']
        else:
            sub['exp_yoy'] = 0
    
    # Filtrar solo categorías con datos válidos (solo las columnas que existen)
    cols_to_check = [col for col in ['exp_yoy', 'balance'] if col in sub.columns]
    if cols_to_check:
        sub = sub.dropna(subset=cols_to_check)
    
    if sub.empty:
        return ["📊 **Datos insuficientes para generar insights**\n\nVerifica que los datos KPI estén disponibles."]
    
    # Añadir balance si no existe
    if 'balance' not in sub.columns:
        sub['balance'] = 0
    
    # Ordenar por variación export YoY (absoluto desc)
    ordered = (sub
               .sort_values('exp_yoy', key=lambda s: s.abs(), ascending=False)
               .head(top_n)
               .to_dict('records'))

    insights = []
    
    for i, record in enumerate(ordered, 1):
        category = record.get('category', 'N/A')
        yoy = record.get('exp_yoy', 0)
        balance = record.get('balance', 0)
        month = record.get('month', 'Dic')
        year = record.get('year', latest_year)
        
        # Determinar tipo de insight
        emoji = _get_trend_emoji(yoy)
        trend_text = "crecieron" if yoy > 0 else "decrecieron"
        
        # Generar recomendaciones específicas
        if yoy > 15:
            action = f"Intensificar promoción comercial y expandir capacidad productiva. Meta: +{yoy*.1:.0f}% adicional en Q4."
            responsible = "DGCE + MINCETUR"
        elif yoy > 5:
            action = f"Consolidar tendencia positiva con misiones comerciales focalizadas."
            responsible = "Oficinas Comerciales"
        elif yoy > -5:
            action = f"Monitorear de cerca y preparar estrategias de diversificación de mercados."
            responsible = "DGIP"
        else:
            action = f"Revisar política sectorial y considerar incentivos específicos."
            responsible = "DGCE + Gremios"
        
        # Contexto adicional
        balance_txt = "superávit" if balance > 0 else "déficit"
        impact_detail = f"Contribuye con US$ {_format_currency(abs(balance))} al {balance_txt} comercial"
        
        insight_md = f"""
### {emoji} **Insight #{i}: {category}**

**📊 Hallazgo:** Las exportaciones de **{category}** {trend_text} **{yoy:+.1f}% YoY** en {_month_name(month)} {year}.

**💰 Impacto:** {impact_detail}. Tendencia {'favorable' if yoy > 0 else 'adversa'} para la balanza sectorial.

**🎯 Acción:** {action}
- **Responsable:** {responsible}  
- **Plazo:** Q4 {year}
- **Seguimiento:** Reunión mensual DGCE

---
"""
        insights.append(insight_md.strip())
    
    return insights

def build_summary_insights(df_country: pd.DataFrame, df_products: pd.DataFrame) -> List[str]:
    """
    Genera insights de alto nivel comparando país vs productos
    """
    if df_country.empty or df_products.empty:
        return ["📊 **Datos insuficientes para generar resumen ejecutivo**"]
    
    # Métricas país (último año)
    latest_year = df_country['year'].max()
    country_latest = df_country[df_country['year'] == latest_year]
    total_exp = country_latest['export'].sum() if 'export' in country_latest.columns else 0
    total_imp = country_latest['import'].sum() if 'import' in country_latest.columns else 0
    balance_country = total_exp - total_imp
    
    # Top categoría (último año)
    products_latest = df_products[df_products['year'] == latest_year]
    if not products_latest.empty:
        # Determinar columna de exportaciones
        exp_col = None
        if 'exp' in products_latest.columns:
            exp_col = 'exp'
        elif 'export' in products_latest.columns:
            exp_col = 'export'
        
        if exp_col and 'category' in products_latest.columns:
            try:
                category_sums = products_latest.groupby('category')[exp_col].sum().sort_values(ascending=False)
                if not category_sums.empty:
                    top_category = category_sums.index[0]
                    top_value = category_sums.iloc[0]
                else:
                    top_category = "N/A"
                    top_value = 0
            except (KeyError, IndexError):
                top_category = "N/A"
                top_value = 0
        else:
            top_category = "N/A"
            top_value = 0
    else:
        top_category = "N/A"
        top_value = 0
    
    # Calcular porcentaje de manera segura
    percentage = (top_value/total_exp*100) if total_exp > 0 else 0
    
    executive_summary = f"""
## 📈 **Resumen Ejecutivo - {latest_year}**

### 🇵🇪 **Posición Nacional**
- **Exportaciones totales:** US$ {_format_currency(total_exp)}
- **Balance comercial:** US$ {_format_currency(balance_country)} ({'superávit' if balance_country > 0 else 'déficit'})

### 🏆 **Sector Líder**
- **Top categoría:** {top_category}
- **Valor:** US$ {_format_currency(top_value)} ({percentage:.1f}% del total)

### 🎯 **Recomendación Estratégica**
Enfocar recursos en diversificación dentro de {top_category} y desarrollo de mercados emergentes para reducir dependencia.

---
"""
    
    return [executive_summary.strip()]

def get_quick_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """Genera estadísticas rápidas para la pestaña de insights"""
    if df.empty:
        return {"error": "Sin datos"}
    
    latest_year = df['year'].max()
    latest_data = df[df['year'] == latest_year]
    
    # Determinar columna de exportaciones (puede ser 'exp' o 'export')
    exp_col = None
    if 'exp' in latest_data.columns:
        exp_col = 'exp'
    elif 'export' in latest_data.columns:
        exp_col = 'export'
    
    # Determinar mejor mes de manera segura
    mejor_mes = "N/A"
    if exp_col and not latest_data.empty and 'month' in latest_data.columns:
        try:
            # Filtrar solo valores no nulos y positivos
            valid_data = latest_data[latest_data[exp_col].notna() & (latest_data[exp_col] > 0)]
            if not valid_data.empty:
                mejor_mes = valid_data.loc[valid_data[exp_col].idxmax(), 'month']
        except (KeyError, ValueError, IndexError):
            mejor_mes = "N/A"
    
    # Determinar columna YoY
    volatilidad = 0
    if 'exp_yoy' in df.columns:
        yoy_data = df['exp_yoy'].dropna()
        volatilidad = yoy_data.std() if not yoy_data.empty else 0
    elif '%YoY_exp' in df.columns:
        yoy_data = df['%YoY_exp'].dropna()
        volatilidad = yoy_data.std() if not yoy_data.empty else 0
    
    stats = {
        "año_actual": latest_year,
        "categorías_activas": df['category'].nunique() if 'category' in df.columns else 0,
        "mejor_mes": mejor_mes,
        "volatilidad": volatilidad
    }
    
    return stats 
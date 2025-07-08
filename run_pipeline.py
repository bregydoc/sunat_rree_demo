#!/usr/bin/env python
"""Pipeline automatizado completo - Observatorio de Comercio Perú 🇵🇪"""

import subprocess
import sys
import time
from pathlib import Path

def run_command(cmd, description):
    """Ejecutar comando con logging"""
    print(f"\n🔄 {description}")
    print(f"   Comando: {cmd}")
    print("   " + "="*50)
    
    start_time = time.time()
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    duration = time.time() - start_time
    
    if result.returncode == 0:
        print(f"✅ Completado en {duration:.1f}s")
        if result.stdout:
            # Mostrar últimas líneas del output
            lines = result.stdout.strip().split('\n')
            for line in lines[-3:]:
                if line.strip():
                    print(f"   {line}")
    else:
        print(f"❌ Error en {duration:.1f}s")
        print(f"   Error: {result.stderr}")
        return False
    
    return True

def check_files():
    """Verificar archivos necesarios"""
    required_files = [
        "observatorio/data/cdro_F8.xlsx",
        "observatorio/data/cdro_G6.xlsx"
    ]
    
    missing = []
    for file_path in required_files:
        if not Path(file_path).exists():
            missing.append(file_path)
    
    if missing:
        print("❌ Archivos faltantes:")
        for file in missing:
            print(f"   - {file}")
        return False
    
    print("✅ Todos los archivos requeridos están presentes")
    return True

def main():
    """Pipeline principal"""
    print("🇵🇪 OBSERVATORIO DE COMERCIO EXTERIOR DEL PERÚ")
    print("=" * 60)
    print("Pipeline automatizado completo")
    print()
    
    # Verificar archivos
    if not check_files():
        print("\n❌ No se puede continuar sin los archivos de datos")
        sys.exit(1)
    
    # Paso 1: ETL
    if not run_command("uv run python observatorio/etl.py", "Paso 1: ETL - Procesando datos base"):
        sys.exit(1)
    
    # Paso 2: Métricas KPI
    if not run_command("uv run python observatorio/metrics.py", "Paso 2: Generando métricas KPI"):
        sys.exit(1)
    
    # Paso 3: Análisis exploratorio
    if not run_command("uv run python observatorio/eda.py", "Paso 3: Análisis exploratorio (EDA)"):
        sys.exit(1)
    
    # Verificar outputs generados
    print("\n📋 VERIFICANDO ARCHIVOS GENERADOS:")
    outputs = [
        ("trade.duckdb", "Base de datos principal"),
        ("kpi_monthly.parquet", "Métricas KPI"),
        ("reports/eda/", "Reportes EDA"),
        ("reports/eda/eda_summary.md", "Resumen de hallazgos")
    ]
    
    for file_path, description in outputs:
        if Path(file_path).exists():
            if Path(file_path).is_file():
                size = Path(file_path).stat().st_size
                print(f"   ✅ {description}: {file_path} ({size/1024:.1f} KB)")
            else:
                files_count = len(list(Path(file_path).glob("*")))
                print(f"   ✅ {description}: {file_path} ({files_count} archivos)")
        else:
            print(f"   ❌ {description}: {file_path} (no encontrado)")
    
    # Resumen final
    print("\n🎉 PIPELINE COMPLETADO EXITOSAMENTE!")
    print("="*60)
    print("📊 Archivos generados:")
    print("   • trade.duckdb         → Base de datos principal")
    print("   • kpi_monthly.parquet  → Métricas KPI calculadas")
    print("   • reports/eda/         → 6 reportes interactivos")
    print()
    print("🚀 Próximos pasos:")
    print("   1. Dashboard:  uv run streamlit run app.py")
    print("   2. SQL CLI:    duckdb trade.duckdb")
    print("   3. Ver EDA:    open reports/eda/dashboard_eda.html")
    print()
    print("🌐 Documentación completa en README.md")

if __name__ == "__main__":
    main() 
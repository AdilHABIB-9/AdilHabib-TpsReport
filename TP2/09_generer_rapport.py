# ================================================================
# Script : Génération de rapport final
# ================================================================

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, col, min, max
from delta import configure_spark_with_delta_pip
from datetime import datetime

# ================================================================
# FIX WINDOWS
# ================================================================
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

GOLD_PATH = 'C:/lakehouse/gold'

# ================================================================
# CRÉATION SESSION SPARK (CORRIGÉE)
# ================================================================
builder = SparkSession.builder \
    .appName("Rapport Final") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.defaultFS", "file:///")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*70)
print("RAPPORT DATA WAREHOUSE - " + datetime.now().strftime("%Y-%m-%d %H:%M"))
print("="*70 + "\n")

print(f"✓ Session Spark créée - Version: {spark.version}")

# ================================================================
# FONCTION PRINCIPALE
# ================================================================
try:
    # ================================================================
    # 1. LIRE DONNÉES GOLD
    # ================================================================
    print("Lecture des données Gold...")
    df_ventes_quot = spark.read.format("delta").load(f"{GOLD_PATH}/ventes_quotidiennes")
    
    total_jours = df_ventes_quot.count()
    print(f"✓ Données chargées: {total_jours} jours analysés")
    
    # ================================================================
    # 2. STATISTIQUES GLOBALES
    # ================================================================
    stats = df_ventes_quot.agg(
        sum('nb_ventes').alias('total_ventes'),
        sum('ca_total').alias('ca_global'),
        avg('panier_moyen').alias('panier_moyen_global')
    ).collect()[0]
    
    print("\n" + "="*70)
    print("STATISTIQUES GLOBALES")
    print("="*70)
    print(f"\n📊 Total des ventes : {stats['total_ventes']:,}")
    print(f"💰 Chiffre d'affaires : {stats['ca_global']:,.2f} €")
    print(f"🛒 Panier moyen : {stats['panier_moyen_global']:,.2f} €")
    
    # ================================================================
    # 3. TOP 5 MEILLEURS JOURS
    # ================================================================
    print("\n" + "="*70)
    print("TOP 5 MEILLEURS JOURS (par chiffre d'affaires)")
    print("="*70)
    
    df_ventes_quot.select(
        col("date"),
        col("nb_ventes"),
        col("ca_total"),
        col("panier_moyen")
    ).orderBy(col('ca_total').desc()).show(5, truncate=False)
    
    # ================================================================
    # 4. ANALYSE SUPPLÉMENTAIRE
    # ================================================================
    print("\n" + "="*70)
    print("ANALYSE DÉTAILLÉE")
    print("="*70)
    
    # Jour avec le plus de ventes
    jour_max_ventes = df_ventes_quot.orderBy(col('nb_ventes').desc()).first()
    if jour_max_ventes:
        print(f"\n📈 JOUR RECORD (nombre de ventes):")
        print(f"   Date: {jour_max_ventes['date']}")
        print(f"   Nombre de ventes: {jour_max_ventes['nb_ventes']}")
        print(f"   CA: {jour_max_ventes['ca_total']:,.2f} €")
    
    # Période analysée (FIXED LINE)
    dates = df_ventes_quot.agg(
        min(col('date')).alias('premier_jour'),
        max(col('date')).alias('dernier_jour')
    ).collect()[0]
    
    print(f"\n📅 PÉRIODE ANALYSÉE:")
    print(f"   Du: {dates['premier_jour']}")
    print(f"   Au: {dates['dernier_jour']}")
    print(f"   Nombre de jours: {total_jours}")
    
    # ================================================================
    # 5. RÉSUMÉ DU DATA WAREHOUSE
    # ================================================================
    print("\n" + "="*70)
    print("RÉSUMÉ DU DATA WAREHOUSE")
    print("="*70)
    
    print(f"\n🏗️  ARCHITECTURE:")
    print(f"   • Source: PostgreSQL (retailpro_dwh)")
    print(f"   • Bronze: Données brutes + métadonnées")
    print(f"   • Silver: Données nettoyées + transformations")
    print(f"   • Gold: Métriques business + agrégations")
    
    print(f"\n📊 DONNÉES DISPONIBLES:")
    print(f"   • Table Gold: ventes_quotidiennes")
    print(f"   • {total_jours} jours d'analyse")
    print(f"   • {stats['total_ventes']} ventes totales")
    
    print("\n" + "="*70)
    print("✓ RAPPORT GÉNÉRÉ AVEC SUCCÈS")
    print("="*70)
    
except Exception as e:
    print(f"\n❌ ERREUR: {e}")
    print("\nConseils de dépannage:")
    print("1. Vérifiez que la couche Gold existe: python 08_gold_aggregation.py")
    print("2. Vérifiez le chemin: C:/lakehouse/gold/ventes_quotidiennes")
    print("3. Assurez-vous que Spark est correctement configuré")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
    print("\n✓ Session Spark arrêtée")
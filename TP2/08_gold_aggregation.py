# ================================================================
# Script : Création de la couche Gold
# ================================================================

import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import *

# ================================================================
# FIX WINDOWS
# ================================================================
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

SILVER_PATH = 'C:/lakehouse/silver'
GOLD_PATH = 'C:/lakehouse/gold'

# ================================================================
# CRÉATION SESSION SPARK (FIXED!)
# ================================================================
# ✅ CORRECTION: Ajouter les configurations Delta
builder = SparkSession.builder \
    .appName("Gold Aggregation") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.defaultFS", "file:///")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("="*70)
print("CRÉATION COUCHE GOLD")
print("="*70)

print(f"✓ Session Spark créée - Version: {spark.version}")

# Créer dossier Gold
import os
os.makedirs(GOLD_PATH, exist_ok=True)
print(f"✓ Dossier Gold créé: {GOLD_PATH}")

# ================================================================
# LECTURE SILVER
# ================================================================
try:
    df_ventes = spark.read.format("delta").load(f"{SILVER_PATH}/ventes")
    print(f"✓ Ventes Silver chargées: {df_ventes.count()} lignes")
    
    # Afficher un aperçu
    print("\nAperçu des ventes Silver:")
    df_ventes.select("vente_id", "client_id", "produit_id", "date_vente", "montant_total").show(5)
    
    # ================================================================
    # AGRÉGATION : VENTES PAR JOUR
    # ================================================================
    print("\n" + "="*70)
    print("CALCUL DES VENTES QUOTIDIENNES")
    print("="*70)
    
    ventes_quotidiennes = df_ventes \
        .withColumn('date', to_date(col('date_vente'))) \
        .groupBy('date') \
        .agg(
            count('*').alias('nb_ventes'),
            sum('montant_total').alias('ca_total'),
            avg('montant_total').alias('panier_moyen'),
            sum('quantite').alias('quantite_totale')
        ) \
        .orderBy('date')
    
    # EXPLICATIONS :
    # - to_date(col('date_vente')) : convertit timestamp en date (sans heure)
    # - groupBy('date') : regroupe par jour
    # - agg(...) : applique des agrégations
    #   * count('*') : compte les ventes
    #   * sum('montant_total') : somme des montants
    #   * avg('montant_total') : moyenne des montants
    # - orderBy('date') : trie par date
    
    print("\nVentes quotidiennes:")
    ventes_quotidiennes.show()
    
    # ================================================================
    # ÉCRITURE GOLD
    # ================================================================
    print("\n" + "="*70)
    print("ÉCRITURE COUCHE GOLD")
    print("="*70)
    
    ventes_quotidiennes.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{GOLD_PATH}/ventes_quotidiennes")
    
    # Vérifier l'écriture
    df_check = spark.read.format("delta").load(f"{GOLD_PATH}/ventes_quotidiennes")
    count = df_check.count()
    
    print(f"✓ Gold ventes_quotidiennes créé: {count} jours")
    print("\nAperçu de la table Gold:")
    df_check.show()
    
    # ================================================================
    # STATISTIQUES
    # ================================================================
    print("\n" + "="*70)
    print("STATISTIQUES GLOBALES")
    print("="*70)
    
    stats = df_check.agg(
        sum('nb_ventes').alias('total_ventes'),
        sum('ca_total').alias('ca_total'),
        avg('panier_moyen').alias('panier_moyen')
    ).collect()[0]
    
    print(f"📊 Nombre total de ventes: {stats['total_ventes']}")
    print(f"💰 Chiffre d'affaires total: {stats['ca_total']:,.2f} €")
    print(f"🛒 Panier moyen: {stats['panier_moyen']:,.2f} €")
    
    # Meilleur jour
    meilleur_jour = df_check.orderBy(col('ca_total').desc()).first()
    if meilleur_jour:
        print(f"\n🏆 MEILLEUR JOUR:")
        print(f"   Date: {meilleur_jour['date']}")
        print(f"   CA: {meilleur_jour['ca_total']:,.2f} €")
        print(f"   Nombre de ventes: {meilleur_jour['nb_ventes']}")
    
    print("\n" + "="*70)
    print("✓ COUCHE GOLD CRÉÉE AVEC SUCCÈS!")
    print("="*70)
    
except Exception as e:
    print(f"\n❌ ERREUR: {e}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
    print("\n✓ Session Spark arrêtée")
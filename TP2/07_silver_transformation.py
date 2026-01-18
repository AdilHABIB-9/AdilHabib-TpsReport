# ================================================================
# Script : Transformation Bronze → Silver (Version complète corrigée)
# ================================================================

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import configure_spark_with_delta_pip

# ================================================================
# CONFIGURATION WINDOWS
# ================================================================
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

# ================================================================
# CHEMINS
# ================================================================
BRONZE_PATH = 'C:/lakehouse/bronze'
SILVER_PATH = 'C:/lakehouse/silver'

# ================================================================
# CRÉATION SESSION SPARK (CORRIGÉE)
# ================================================================
print("="*70)
print("TRANSFORMATION SILVER")
print("="*70)

# ✅ CORRECTION: Ajouter les configurations Delta OBLIGATOIRES!
builder = SparkSession.builder \
    .appName("Silver Transformation") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.defaultFS", "file:///")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"✓ Session Spark créée - Version: {spark.version}")

# ================================================================
# CRÉER DOSSIER SILVER
# ================================================================
os.makedirs(SILVER_PATH, exist_ok=True)
print(f"✓ Dossier Silver créé: {SILVER_PATH}")

# ================================================================
# 1. TRANSFORMATION CLIENTS
# ================================================================
print("\n" + "="*70)
print("TRANSFORMATION CLIENTS")
print("="*70)

# Lire Bronze
df_clients = spark.read.format("delta").load(f"{BRONZE_PATH}/clients")
print(f"Clients Bronze : {df_clients.count()} lignes")

# Nettoyage :
df_clients_clean = df_clients \
    .dropDuplicates(['client_id']) \
    .withColumn('nom', upper(trim(col('nom')))) \
    .withColumn('prenom', initcap(trim(col('prenom')))) \
    .withColumn('ville', upper(trim(col('ville')))) \
    .withColumn('segment', upper(trim(col('segment')))) \
    .withColumn('email', lower(trim(col('email')))) \
    .withColumn('email_valide', col('email').rlike('^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$')) \
    .withColumn('silver_ingestion_timestamp', current_timestamp())

print(f"Clients Silver (après nettoyage) : {df_clients_clean.count()} lignes")

# Écrire en Silver
df_clients_clean.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/clients")
print("✓ Silver clients créé")

# ================================================================
# 2. TRANSFORMATION PRODUITS
# ================================================================
print("\n" + "="*70)
print("TRANSFORMATION PRODUITS")
print("="*70)

df_produits = spark.read.format("delta").load(f"{BRONZE_PATH}/produits")
print(f"Produits Bronze : {df_produits.count()} lignes")

# Correction de l'encodage (MÃ©canique → Mécanique) et nettoyage
df_produits_clean = df_produits \
    .dropDuplicates(['produit_id']) \
    .withColumn('nom_produit', 
               when(col('nom_produit').contains('MÃ©canique'),
                    regexp_replace(col('nom_produit'), 'MÃ©canique', 'Mécanique'))
               .otherwise(col('nom_produit'))) \
    .withColumn('nom_produit', upper(trim(col('nom_produit')))) \
    .withColumn('categorie', upper(trim(col('categorie')))) \
    .withColumn('marge', round(col('prix_unitaire') - col('cout_achat'), 2)) \
    .withColumn('taux_marge', 
               when(col('cout_achat') > 0, 
                    round(((col('prix_unitaire') - col('cout_achat')) / col('cout_achat')) * 100, 2))
               .otherwise(0)) \
    .withColumn('silver_ingestion_timestamp', current_timestamp())

print(f"Produits Silver (après nettoyage) : {df_produits_clean.count()} lignes")

# Écrire en Silver
df_produits_clean.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/produits")
print("✓ Silver produits créé")

# ================================================================
# 3. TRANSFORMATION VENTES
# ================================================================
print("\n" + "="*70)
print("TRANSFORMATION VENTES")
print("="*70)

df_ventes = spark.read.format("delta").load(f"{BRONZE_PATH}/ventes")
print(f"Ventes Bronze : {df_ventes.count()} lignes")

# Nettoyage et enrichissement
df_ventes_clean = df_ventes \
    .dropDuplicates(['vente_id']) \
    .withColumn('date_vente', to_date(col('date_vente'))) \
    .withColumn('annee', year(col('date_vente'))) \
    .withColumn('mois', month(col('date_vente'))) \
    .withColumn('jour', dayofmonth(col('date_vente'))) \
    .withColumn('jour_semaine', 
               when(dayofweek(col('date_vente')) == 1, 7)  # Dimanche = 7
               .otherwise(dayofweek(col('date_vente')) - 1)) \
    .withColumn('montant_valide', col('montant_total') > 0) \
    .withColumn('quantite_valide', col('quantite') > 0) \
    .withColumn('silver_ingestion_timestamp', current_timestamp())

print(f"Ventes Silver (après nettoyage) : {df_ventes_clean.count()} lignes")

# Écrire en Silver
df_ventes_clean.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/ventes")
print("✓ Silver ventes créé")

# ================================================================
# VÉRIFICATION FINALE
# ================================================================
print("\n" + "="*70)
print("VÉRIFICATION SILVER LAYER")
print("="*70)

tables = ['clients', 'produits', 'ventes']
for table in tables:
    df_check = spark.read.format("delta").load(f"{SILVER_PATH}/{table}")
    count = df_check.count()
    colonnes = len(df_check.columns)
    print(f"✓ {table}: {count} lignes, {colonnes} colonnes")

print("\n" + "="*70)
print("✓ TRANSFORMATION SILVER TERMINÉE AVEC SUCCÈS!")
print("="*70)

# ================================================================
# APERÇU DES DONNÉES
# ================================================================
print("\n" + "="*70)
print("APERÇU DES DONNÉES SILVER")
print("="*70)

print("\nClients (3 premières lignes):")
spark.read.format("delta").load(f"{SILVER_PATH}/clients").show(3, truncate=False)

print("\nProduits (3 premières lignes):")
spark.read.format("delta").load(f"{SILVER_PATH}/produits").show(3, truncate=False)

print("\nVentes (3 premières lignes):")
spark.read.format("delta").load(f"{SILVER_PATH}/ventes").show(3, truncate=False)

# ================================================================
# NETTOYAGE
# ================================================================
spark.stop()
print("\n✓ Session Spark arrêtée")
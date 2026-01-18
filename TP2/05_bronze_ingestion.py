# ================================================================
# Script : Ingestion Bronze depuis PostgreSQL vers Delta Lake
# Description : Lit PostgreSQL avec PySpark et écrit en Delta
# ================================================================

# ================================================================
# PARTIE 1 : IMPORTS
# ================================================================
from pyspark.sql import SparkSession
# SparkSession : point d'entrée principal de PySpark

from delta import configure_spark_with_delta_pip
# Fonction qui configure Spark pour utiliser Delta Lake

from pyspark.sql.functions import current_timestamp, lit
# current_timestamp : fonction Spark pour la date/heure actuelle
# lit : fonction Spark pour créer une colonne avec une valeur fixe

print("✓ Bibliothèques importées")

# ================================================================
# PARTIE 2 : CONFIGURATION POSTGRESQL
# ================================================================

# Informations de connexion à PostgreSQL
POSTGRES_CONFIG = {
    'url': 'jdbc:postgresql://localhost:5432/retailpro_dwh',
    # EXPLICATION :
    # - jdbc:postgresql:// = préfixe JDBC pour PostgreSQL
    # - localhost = serveur (votre machine)
    # - 5432 = port PostgreSQL
    # - retailpro_dwh = nom de la base de données
    
    'user': 'postgres',
    # Utilisateur PostgreSQL
    
    'password': 'postgres123',  # CHANGEZ ICI
    # Mot de passe PostgreSQL
    
    'driver': 'org.postgresql.Driver'
    # Classe Java du driver JDBC PostgreSQL
}

# Chemins des couches Lakehouse
# IMPORTANT : Adaptez selon votre système (Windows = C:/lakehouse/, Linux/Mac = ~/lakehouse/)
BRONZE_PATH = 'C:/lakehouse/bronze'  # Windows
# BRONZE_PATH = '/home/votre_user/lakehouse/bronze'  # Linux/Mac

print(f"✓ Configuration définie - Bronze path: {BRONZE_PATH}")

# ================================================================
# PARTIE 3 : CRÉATION DE LA SESSION SPARK
# ================================================================

def creer_session_spark():
    """
    Crée et configure une session Spark avec Delta Lake et JDBC
    Returns: SparkSession configurée
    """
    print("\nCréation de la session Spark...")
    
    # Étape 1 : Créer le builder
    builder = SparkSession.builder \
        .appName("Bronze Ingestion") \
        .master("local[*]")
    # EXPLICATION :
    # - builder = constructeur de session Spark
    # - .appName("...") = nom de l'application (pour identification)
    # - .master("local[*]") = mode local, utilise tous les cœurs CPU
    
    # Étape 2 : Ajouter le driver JDBC PostgreSQL
    builder = builder.config(
    "spark.jars",
    "file:///C:/Users/adilh/OneDrive/Desktop/TP_DataWarehouse/drivers/postgresql-42.7.8.jar"
)

    # EXPLICATION :
    # - spark.jars = liste des fichiers .jar à charger au démarrage
    # - PostgreSQL driver .jar permet à Spark de se connecter à PostgreSQL
    # - IMPORTANT : Utilisez le chemin ABSOLU vers votre fichier .jar
    
    # Étape 3 : Configurer Delta Lake
    builder = builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # EXPLICATION :
    # Ces 2 configs activent Delta Lake dans Spark
    # - extensions = ajoute les fonctions Delta (MERGE, TIME TRAVEL, etc.)
    # - catalog = utilise le catalogue Delta pour gérer les tables
    
    # Étape 4 : Créer la session avec Delta Lake
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    # EXPLICATION :
    # - configure_spark_with_delta_pip() = finalise la config Delta
    # - .getOrCreate() = crée une nouvelle session OU réutilise l'existante
    
    # Réduire les logs
    spark.sparkContext.setLogLevel("WARN")
    # EXPLICATION :
    # Par défaut, Spark affiche BEAUCOUP de logs
    # WARN = afficher seulement les warnings et erreurs
    
    print(f"✓ Session Spark créée - Version: {spark.version}")
    return spark

# ================================================================
# PARTIE 4 : LECTURE DEPUIS POSTGRESQL
# ================================================================

def lire_table_postgres(spark, nom_table):
    """
    Lit une table depuis PostgreSQL avec PySpark
    
    Args:
        spark: Session Spark
        nom_table: Nom de la table à lire (ex: 'clients_source')
    
    Returns:
        DataFrame PySpark contenant les données de la table
    """
    print(f"\nLecture de la table '{nom_table}' depuis PostgreSQL...")
    
    # Lire avec JDBC
    df = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_CONFIG['url']) \
        .option("dbtable", nom_table) \
        .option("user", POSTGRES_CONFIG['user']) \
        .option("password", POSTGRES_CONFIG['password']) \
        .option("driver", POSTGRES_CONFIG['driver']) \
        .load()
    
    # EXPLICATION DÉTAILLÉE :
    # 
    # spark.read = commence une opération de lecture
    # 
    # .format("jdbc") = dit à Spark "utilise JDBC pour lire"
    #   JDBC = Java Database Connectivity
    #   Permet à Spark (Java/Scala) de communiquer avec PostgreSQL
    # 
    # .option("url", ...) = adresse de la base de données
    #   Format : jdbc:postgresql://host:port/database
    # 
    # .option("dbtable", nom_table) = quelle table lire
    #   Spark va exécuter : SELECT * FROM nom_table
    # 
    # .option("user", ...) = nom d'utilisateur PostgreSQL
    # 
    # .option("password", ...) = mot de passe PostgreSQL
    # 
    # .option("driver", "org.postgresql.Driver") = classe Java du driver
    #   Cette classe est dans le fichier .jar qu'on a ajouté avec spark.jars
    # 
    # .load() = EXÉCUTE la lecture
    #   C'est ici que Spark se connecte réellement à PostgreSQL
    #   et charge les données en mémoire
    # 
    # RÉSULTAT :
    # df = DataFrame PySpark contenant toutes les lignes de la table
    
    nb_lignes = df.count()
    # .count() = compte le nombre de lignes
    # ATTENTION : C'est une ACTION, Spark va exécuter la requête
    
    print(f"✓ {nb_lignes} lignes lues depuis '{nom_table}'")
    
    # Afficher le schéma (structure des colonnes)
    print(f"  Schéma de {nom_table}:")
    df.printSchema()
    # EXPLICATION :
    # printSchema() affiche :
    # - Le nom de chaque colonne
    # - Le type de données (string, integer, decimal, etc.)
    # - Si la colonne peut être NULL
    
    return df

# ================================================================
# PARTIE 5 : AJOUT DE MÉTADONNÉES
# ================================================================

def ajouter_metadata(df, nom_table_source):
    """
    Ajoute des colonnes de métadonnées au DataFrame
    
    Args:
        df: DataFrame à enrichir
        nom_table_source: Nom de la table source
    
    Returns:
        DataFrame avec colonnes métadonnées ajoutées
    """
    # Ajouter 3 colonnes de métadonnées
    df_enrichi = df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("PostgreSQL")) \
        .withColumn("source_table", lit(nom_table_source))
    
    # EXPLICATION DÉTAILLÉE :
    # 
    # .withColumn(nom_colonne, valeur) = ajoute ou modifie une colonne
    # 
    # 1. ingestion_timestamp = current_timestamp()
    #    - Ajoute une colonne avec la date/heure actuelle
    #    - Permet de savoir QUAND les données ont été ingérées
    # 
    # 2. source_system = lit("PostgreSQL")
    #    - lit() crée une valeur constante
    #    - Toutes les lignes auront "PostgreSQL" dans cette colonne
    #    - Utile si vous avez plusieurs sources (PostgreSQL, MySQL, API, etc.)
    # 
    # 3. source_table = lit(nom_table_source)
    #    - Nom de la table d'origine
    #    - Ex: "clients_source", "produits_source"
    # 
    # RÉSULTAT :
    # Le DataFrame a maintenant 3 colonnes supplémentaires :
    # | ... colonnes originales ... | ingestion_timestamp | source_system | source_table |
    
    print(f"✓ Métadonnées ajoutées (3 colonnes)")
    return df_enrichi

# ================================================================
# PARTIE 6 : ÉCRITURE EN DELTA LAKE
# ================================================================

def ecrire_bronze_delta(df, nom_table):
    """
    Écrit un DataFrame en format Delta Lake dans Bronze
    
    Args:
        df: DataFrame à écrire
        nom_table: Nom pour le dossier Delta (ex: 'clients')
    """
    chemin_complet = f"{BRONZE_PATH}/{nom_table}"
    
    print(f"\nÉcriture en Delta Lake : {chemin_complet}")
    
    # Écrire en format Delta
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(chemin_complet)
    
    # EXPLICATION DÉTAILLÉE :
    # 
    # df.write = commence une opération d'écriture
    # 
    # .format("delta") = utilise le format Delta Lake
    #   Delta Lake est un format de stockage qui ajoute :
    #   - Transactions ACID (Atomicité, Cohérence, Isolation, Durabilité)
    #   - Versioning automatique (Time Travel)
    #   - Schema enforcement (validation du schéma)
    #   - Sous le capot, Delta utilise Parquet + un fichier de log
    # 
    # .mode("overwrite") = mode d'écriture
    #   Options :
    #   - "overwrite" : remplace toutes les données existantes
    #   - "append" : ajoute aux données existantes
    #   - "error" : erreur si les données existent déjà
    #   - "ignore" : ne fait rien si les données existent
    # 
    # .save(chemin_complet) = EXÉCUTE l'écriture
    #   Crée un dossier avec :
    #   - Fichiers .parquet (données)
    #   - Dossier _delta_log/ (historique des transactions)
    # 
    # RÉSULTAT SUR DISQUE :
    # C:/lakehouse/bronze/clients/
    #   ├── part-00000-xxx.snappy.parquet  (données)
    #   ├── part-00001-xxx.snappy.parquet
    #   └── _delta_log/
    #       └── 00000000000000000000.json  (log de transaction)
    
    nb_lignes = df.count()
    print(f"✓ {nb_lignes} lignes écrites en Delta Lake")
    print(f"  Emplacement : {chemin_complet}")

# ================================================================
# PARTIE 7 : FONCTION PRINCIPALE
# ================================================================

def main():
    """
    Fonction principale qui orchestre tout le processus
    """
    print("="*70)
    print("INGESTION BRONZE : PostgreSQL → Delta Lake")
    print("="*70)
    
    try:
        # Étape 1 : Créer la session Spark
        spark = creer_session_spark()
        
        # Étape 2 : Liste des tables à ingérer
        tables = ['clients_source', 'produits_source', 'ventes_source']
        
        # Étape 3 : Traiter chaque table
        for table_source in tables:
            print(f"\n{'='*70}")
            print(f"Traitement de : {table_source}")
            print(f"{'='*70}")
            
            # Lire depuis PostgreSQL
            df = lire_table_postgres(spark, table_source)
            
            # Ajouter les métadonnées
            df_enrichi = ajouter_metadata(df, table_source)
            
            # Écrire en Bronze (Delta Lake)
            nom_table_bronze = table_source.replace('_source', '')
            ecrire_bronze_delta(df_enrichi, nom_table_bronze)
        
        print(f"\n{'='*70}")
        print("✓ INGESTION BRONZE TERMINÉE AVEC SUCCÈS")
        print(f"{'='*70}")
        
    except Exception as e:
        print(f"\n✗ ERREUR : {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Toujours arrêter Spark proprement
        spark.stop()
        print("\n✓ Session Spark arrêtée")

# ================================================================
# POINT D'ENTRÉE
# ================================================================

if __name__ == "__main__":
    main()
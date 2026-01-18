# ================================================================
# Script : Test de connexion PySpark vers PostgreSQL
# Description : Vérifie que PySpark peut lire des données depuis PostgreSQL
# ================================================================

# ÉTAPE 1 : Importer les bibliothèques nécessaires
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

print("Bibliothèques importées avec succès")

# ================================================================
# ÉTAPE 2 : Configurer Spark avec le driver JDBC
# ================================================================

# Créer un builder Spark (constructeur de session)
builder = SparkSession.builder \
    .appName("Test Connexion PostgreSQL") \
    .master("local[*]")

# EXPLICATION DE .master("local[*]") :
# - "local" = Spark tourne sur votre machine (pas en cluster)
# - "[*]" = Utilise tous les cœurs CPU disponibles

# Ajouter le driver JDBC PostgreSQL au classpath
# IMPORTANT : Remplacez ce chemin par le vôtre
# VOICI LE CHEMIN À MODIFIER ↓↓↓
builder = builder.config(
    "spark.jars", 
    "file:///C:/Users/adilh/OneDrive/Desktop/TP_DataWarehouse/drivers/postgresql-42.7.8.jar"  # ← CHANGÉ À 42.7.8
)

# EXPLICATION :
# - spark.jars dit à Spark "charge ce fichier .jar au démarrage"
# - Spark va pouvoir utiliser le code Java dans ce .jar pour se connecter à PostgreSQL

# Configurer Delta Lake (optionnel pour ce test, mais nécessaire plus tard)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# EXPLICATION DE .getOrCreate() :
# - Si une session Spark existe déjà, la réutilise
# - Sinon, en crée une nouvelle

print("Session Spark créée avec succès")
print(f"Version de Spark : {spark.version}")

# ================================================================
# ÉTAPE 3 : Configurer les paramètres de connexion PostgreSQL
# ================================================================

# Dictionnaire contenant toutes les infos de connexion
postgres_config = {
    "url": "jdbc:postgresql://localhost:5432/postgres",
    # EXPLICATION de l'URL :
    # - jdbc:postgresql:// = préfixe obligatoire pour JDBC PostgreSQL
    # - localhost = serveur (votre machine)
    # - 5432 = port PostgreSQL (par défaut)
    # - postgres = nom de la base de données
    
    "dbtable": "pg_database",  
    # EXPLICATION :
    # - pg_database est une table système PostgreSQL (toujours présente)
    # - On l'utilise juste pour tester la connexion
    
    "user": "postgres",
    # EXPLICATION :
    # - Nom d'utilisateur PostgreSQL
    # - "postgres" est le superutilisateur créé à l'installation
    
    "password": "postgres123",  # ← CORRIGÉ : VOTRE MOT DE PASSE postgres123
    # IMPORTANT : C'est le mot de passe que vous avez confirmé
    
    "driver": "org.postgresql.Driver"
    # EXPLICATION :
    # - Nom de la classe Java du driver PostgreSQL
    # - Cette classe est dans le fichier .jar qu'on a téléchargé
}

print("Configuration PostgreSQL définie")

# ================================================================
# ÉTAPE 4 : Lire les données depuis PostgreSQL
# ================================================================

print("\nTentative de connexion à PostgreSQL...")

try:
    # Lire la table avec Spark
    df = spark.read \
        .format("jdbc") \
        .options(**postgres_config) \
        .load()
    
    # EXPLICATION DÉTAILLÉE :
    # 1. spark.read = commence une lecture de données
    # 2. .format("jdbc") = dit à Spark "utilise JDBC pour lire"
    # 3. .options(**postgres_config) = passe tous les paramètres de connexion
    #    Le ** "déplie" le dictionnaire : 
    #    {url: "...", user: "..."} devient url="...", user="..."
    # 4. .load() = exécute la lecture
    
    print("Connexion réussie !")
    print(f"Nombre de lignes lues : {df.count()}")
    
    # Afficher le schéma (structure) des données
    print("\nSchéma de la table :")
    df.printSchema()
    
    # Afficher quelques lignes
    print("\nAperçu des données :")
    df.show(5)
    
    print("\n" + "="*70)
    print("TEST RÉUSSI : PySpark peut lire les données depuis PostgreSQL !")
    print("="*70)
    
except Exception as e:
    print("\nERREUR lors de la connexion :")
    print(str(e))
    print("\nVérifiez :")
    print("1. PostgreSQL est démarré")
    print("2. Le mot de passe postgres123 est correct")  # ← CORRIGÉ
    print("3. Le chemin vers postgresql-42.7.8.jar est correct")
    print("4. Le port 5432 est ouvert")
    print("\nNote : Utilisez la commande 'dir drivers' pour voir votre fichier .jar")

# ================================================================
# ÉTAPE 5 : Arrêter la session Spark proprement
# ================================================================

spark.stop()
print("\nSession Spark arrêtée")
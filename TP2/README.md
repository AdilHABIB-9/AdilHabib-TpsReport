# 📊 TP DATA WAREHOUSE - Lakehouse avec Delta Lake

## 📋 Description du Projet
Pipeline ETL complet pour créer un data warehouse moderne avec architecture Lakehouse (Bronze-Silver-Gold) en utilisant:
- **PostgreSQL** comme source de données
- **Apache Spark** pour le traitement
- **Delta Lake** pour le stockage
- **Python/PySpark** pour l'orchestration

## 🏗️ Architecture

PostgreSQL → Bronze (raw) → Silver (cleaned) → Gold (aggregated) → Rapports

## 📁 Structure des Fichiers

TP_DataWarehouse/
├── drivers/ # Drivers JDBC
│ └── postgresql-42.7.1.jar # Driver PostgreSQL
├── venv/ # Environnement virtuel Python
├── 05_bronze_ingestion.py # Ingestion PostgreSQL → Bronze
├── 06_verify_bronze.py # Vérification couche Bronze
├── 07_silver_transformation.py # Transformation Bronze → Silver
├── 08_gold_aggregation.py # Aggrégation Silver → Gold
├── 09_generer_rapport.py # Génération rapport final
├── README.md # Ce fichier
└── requirements.txt # Dépendances Python

## ⚙️ Pré-requis

### 1. Installer Python 3.8+
```cmd
python --version
java -version
Important: Définir JAVA_HOME dans les variables d'environnement.
3. Installer PostgreSQL

Télécharger: https://www.postgresql.org/download/

Créer la base: retailpro_dwh

Exécuter les scripts SQL fournis

4. Configurer l'environnement Windows
# Variables d'environnement à ajouter
HADOOP_HOME = C:\Users\adilh\OneDrive\Desktop\hadoop-3.0.0
JAVA_HOME = C:\Program Files\Eclipse Adoptium\jdk-11.0.28.6-hotspot

# Ajouter au PATH
%HADOOP_HOME%\bin
%JAVA_HOME%\bin

🚀 Installation
1. Cloner/Initialiser le projet
cmd
cd C:\Users\adilh\OneDrive\Desktop\TP_DataWarehouse
2. Créer l'environnement virtuel
cmd
python -m venv venv
3. Activer l'environnement virtuel
# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
4. Installer les dépendances
pip install -r requirements.txt
Si requirements.txt n'existe pas, installer manuellement:
pip install pyspark==3.5.0
pip install delta-spark==3.0.0
pip install pandas openpyxl


5. Télécharger le driver PostgreSQL JDBC
# Créer le dossier drivers
mkdir drivers

# Télécharger depuis:
https://jdbc.postgresql.org/download/postgresql-42.7.1.jar

# Placer dans:
C:\Users\adilh\OneDrive\Desktop\TP_DataWarehouse\drivers\

🗄️ Configuration de la Base de Données
1. Démarrer PostgreSQL
# Via pgAdmin ou ligne de commande
psql -U postgres

2. Créer la base de données
CREATE DATABASE retailpro_dwh;
\c retailpro_dwh;

3. Créer les tables (exemple)
-- Table clients_source
CREATE TABLE clients_source (
    client_id SERIAL PRIMARY KEY,
    nom VARCHAR(50),
    prenom VARCHAR(50),
    email VARCHAR(100),
    ville VARCHAR(50),
    segment VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table produits_source
CREATE TABLE produits_source (
    produit_id SERIAL PRIMARY KEY,
    nom_produit VARCHAR(100),
    categorie VARCHAR(50),
    prix_unitaire DECIMAL(10,2),
    cout_achat DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table ventes_source
CREATE TABLE ventes_source (
    vente_id SERIAL PRIMARY KEY,
    client_id INT REFERENCES clients_source(client_id),
    produit_id INT REFERENCES produits_source(produit_id),
    date_vente TIMESTAMP,
    quantite INT,
    montant_total DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insérer des données de test
INSERT INTO clients_source (nom, prenom, email, ville, segment) VALUES
('Dupont', 'Jean', 'jean.dupont@email.com', 'Lyon', 'Gold'),
('Martin', 'Marie', 'marie.martin@email.com', 'Lyon', 'Platinum'),
('Bernard', 'Pierre', 'pierre.bernard@email.com', 'Marseille', 'Bronze');

INSERT INTO produits_source (nom_produit, categorie, prix_unitaire, cout_achat) VALUES
('Laptop HP', 'Informatique', 899.99, 650.00),
('Souris Logitech', 'Informatique', 29.99, 15.00),
('Clavier Mécanique', 'Informatique', 149.99, 80.00);

INSERT INTO ventes_source (client_id, produit_id, date_vente, quantite, montant_total) VALUES
(1, 1, '2026-01-01 10:00:00', 1, 899.99),
(2, 2, '2026-01-01 11:00:00', 2, 59.98),
(3, 3, '2026-01-01 12:00:00', 1, 149.99);

🏃‍♂️ Exécution du Pipeline
Étape 1: Ingestion Bronze
python 05_bronze_ingestion.py
Résultat: Crée C:\lakehouse\bronze\ avec 3 tables Delta.

Étape 2: Vérification Bronze
python 06_verify_bronze.py
Vérifie: Données, schémas et métadonnées de la couche Bronze.

Étape 3: Transformation Silver
python 07_silver_transformation.py
Actions: Nettoyage, standardisation, correction encodage.

Étape 4: Aggrégation Gold
python 08_gold_aggregation.py
Crée: Métriques business (ventes quotidiennes, etc.).

Étape 5: Génération Rapport
python 09_generer_rapport.py
Affiche: Statistiques globales et KPIs.

📊 Résultats Attendus
Structure des Données

C:\lakehouse\
├── bronze\          # Données brutes (raw)
│   ├── clients\     # clients_source → clients
│   ├── produits\    # produits_source → produits
│   └── ventes\      # ventes_source → ventes
├── silver\          # Données nettoyées
│   ├── clients\     # Standardisé + validation
│   ├── produits\    # Encodage fixé + marges
│   └── ventes\      # Découpage date + validation
└── gold\           # Métriques business
    └── ventes_quotidiennes\  # CA par jour

Métriques Calculées
✅ Chiffre d'affaires total

✅ Nombre de ventes

✅ Panier moyen

✅ Top jours par CA

✅ Période analysée

📚 Documentation Technique
Bibliothèques Utilisées
PySpark 3.5.0: Traitement distribué

Delta Lake 3.0.0: Stockage transactionnel

PostgreSQL JDBC: Connexion base de données

Concepts Clés
Lakehouse: Combine data lake + data warehouse

Delta Lake: Stockage ACID sur object storage

Medallion Architecture: Bronze → Silver → Gold

Time Travel: Historique des données Delta

👥 Contribution
Fork le projet

Créer une branche (git checkout -b feature/amélioration)

Commit les changements (git commit -m 'Ajout feature X')

Push vers la branche (git push origin feature/amélioration)

Ouvrir une Pull Request

📄 Licence
Projet éducatif - Libre d'utilisation pour l'apprentissage

✨ Auteurs
Adil H. - Développement du pipeline complet

Encadrant - Supervision et validation

📅 Dernière mise à jour: Janvier 2026
🏷️ Version: 1.0.0
✅ Statut: Pipeline fonctionnel et validé
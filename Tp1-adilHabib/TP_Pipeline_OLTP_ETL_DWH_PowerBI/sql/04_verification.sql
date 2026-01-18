-- Sélectionnez la base de données DWH
USE ventes_dwh;

-- 1. Vérification du nombre total de lignes dans la table des faits
-- Le TP mentionne 100 000 lignes dans le fichier source, donc 100 000 devraient être insérées ici.
SELECT 
    'FactVentes' AS table_name,
    COUNT(*) AS total_rows 
FROM FactVentes;

-- 2. Vérification des lignes dans la table de dimension Client
-- Le TP mentionne 10 000 clients dans le fichier source.
SELECT 
    'DimClient' AS table_name,
    COUNT(*) AS total_rows
FROM DimClient;

-- 3. Vérification des lignes dans la table de dimension Produit
-- Le TP mentionne 500 produits dans le fichier source.
SELECT 
    'DimProduit' AS table_name,
    COUNT(*) AS total_rows
FROM DimProduit;

-- 4. Vérification des lignes dans la table de dimension Date
-- Le TP mentionne la génération de 1096 dates (environ 3 ans complets).
SELECT 
    'DimDate' AS table_name,
    COUNT(*) AS total_rows
FROM DimDate;

-- 5. Vérification rapide de l'intervalle de temps chargé
-- Confirme que les dates de début et de fin correspondent à ce que l'on attend (ex: 2022-01-01 et 2024-12-31)
SELECT 
    'Min Date' AS metric, 
    MIN(date_complete) AS date_value 
FROM DimDate
UNION ALL
SELECT 
    'Max Date' AS metric, 
    MAX(date_complete) AS date_value 
FROM DimDate;

-- 6. (BONUS) Vérification des valeurs NULL dans la table des faits
-- S'il y a des NULLs dans les clés étrangères, cela indique un problème de Lookup.
-- Le résultat attendu pour chaque COUNT(*) est 0.
SELECT
    'NULL Foreign Keys' AS metric,
    COUNT(CASE WHEN id_client_dim IS NULL THEN 1 END) AS null_clients,
    COUNT(CASE WHEN id_produit_dim IS NULL THEN 1 END) AS null_produits,
    COUNT(CASE WHEN id_date_dim IS NULL THEN 1 END) AS null_dates
FROM FactVentes;
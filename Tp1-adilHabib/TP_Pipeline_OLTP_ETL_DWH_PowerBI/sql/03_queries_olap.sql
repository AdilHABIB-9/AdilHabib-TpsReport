USE ventes_dwh;

SELECT 
    c.ville,
    SUM(f.montant_total) AS chiffre_affaires,
    COUNT(DISTINCT f.id_client_dim) AS nombre_clients,
    COUNT(f.id_vente) AS nombre_ventes
FROM FactVentes f
INNER JOIN DimClient c ON f.id_client_dim = c.id_client_dim
GROUP BY c.ville
ORDER BY chiffre_affaires DESC;

SELECT 
    p.categorie,
    SUM(f.montant_total) AS chiffre_affaires,
    SUM(f.quantite) AS quantite_vendue,
    COUNT(DISTINCT f.id_produit_dim) AS nombre_produits_distincts,
    ROUND(AVG(f.prix_unitaire), 2) AS prix_moyen
FROM FactVentes f
INNER JOIN DimProduit p ON f.id_produit_dim = p.id_produit_dim
GROUP BY p.categorie
ORDER BY chiffre_affaires DESC;


SELECT 
    d.annee,
    d.mois,
    d.nom_mois,
    SUM(f.montant_total) AS chiffre_affaires,
    COUNT(f.id_vente) AS nombre_ventes,
    ROUND(AVG(f.montant_total), 2) AS panier_moyen
FROM FactVentes f
INNER JOIN DimDate d ON f.id_date_dim = d.id_date_dim
GROUP BY d.annee, d.mois, d.nom_mois
ORDER BY d.annee, d.mois;




SELECT 
    p.nom_produit,
    p.categorie,
    SUM(f.quantite) AS quantite_totale_vendue,
    SUM(f.montant_total) AS chiffre_affaires,
    ROUND(AVG(f.prix_unitaire), 2) AS prix_moyen
FROM FactVentes f
INNER JOIN DimProduit p ON f.id_produit_dim = p.id_produit_dim
GROUP BY p.id_produit_dim, p.nom_produit, p.categorie
ORDER BY quantite_totale_vendue DESC
LIMIT 10;




SELECT 
    d.annee,
    d.trimestre,
    p.categorie,
    SUM(f.montant_total) AS chiffre_affaires,
    SUM(f.quantite) AS quantite_vendue
FROM FactVentes f
INNER JOIN DimDate d ON f.id_date_dim = d.id_date_dim
INNER JOIN DimProduit p ON f.id_produit_dim = p.id_produit_dim
GROUP BY d.annee, d.trimestre, p.categorie
ORDER BY d.annee, d.trimestre, chiffre_affaires DESC;
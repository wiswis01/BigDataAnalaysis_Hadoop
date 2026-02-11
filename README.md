
# Jeu de données UA2 — Ventes multicanal

Fichiers:
- ventes_multicanal.csv (≈ 5000 lignes) — période du 2025-07-01 au 2025-10-15, schéma v1
- ventes_increment_2025-10.csv (≈ 300 lignes) — période du 2025-10-16 au 2025-10-25, schéma v2 (ajoute `coupon_value`, `shipping_cost`)
- catalogue_produits.csv — dictionnaire produits/catégories

Colonnes (v1):
transaction_id, ts, user_id, country, city, product_id, category, subcategory,
unit_price, qty, discount_code, payment_type, device, channel, referrer,
is_return, return_date, status, notes

Colonnes ajoutées (v2) :
coupon_value, shipping_cost

Particularités:
- Encodage UTF-8 avec accents/émojis (📦, ⚠️).
- Lignes de commentaires commençant par `#` à ignorer.
- Quelques lignes malformées (dates invalides, citations non fermées, nombre de colonnes incorrect).
- Duplicats intentionnels de `transaction_id`.
- Retours modélisés par `qty` négatif et `is_return` = 1.
- Valeurs aberrantes possibles sur `total` implicite (= unit_price * qty).
- Hétérogénéité de casse et de contenus libres dans `notes`.

Conseils:
- Normaliser les dates et gérer les fuseaux si nécessaire.
- Nettoyer/dédupliquer avant agrégation.
- Détecter/mettre de côté les lignes malformées.
- Gérer la dérive de schéma entre v1 et v2.

# Top Products MapReduce Job
# Calcule le top 10 des produits par chiffre d'affaires net
# Effectue une jointure avec le catalogue produits

from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import csv

class TopProductsJob(MRJob):
    """
    Job MapReduce pour identifier le top 10 des produits
    Effectue une jointure avec le catalogue produits
    """

    FILES = ['catalogue_produits.csv']

    # -- helpers simples et robustes --
    @staticmethod
    def _to_float(x):
        try:
            return float(str(x).replace(',', '').strip())
        except Exception:
            return 0.0

    @staticmethod
    def _to_int(x):
        try:
            return int(str(x).strip())
        except Exception:
            return 0

    def mapper_init(self):
        """Charge le catalogue produits (side-join) avec csv pour gérer les virgules/quotes"""
        self.product_catalog = {}
        try:
            with open('catalogue_produits.csv', 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=',', quotechar='"', escapechar='\\')
                header = next(reader, None)  # ignore l'en-tête
                for row in reader:
                    if not row or row[0].startswith('#'):
                        continue
                    # on tolère les lignes courtes
                    product_id = (row[0] or '').strip() if len(row) > 0 else ''
                    product_name = (row[1] or '').strip() if len(row) > 1 else ''
                    category = (row[2] or '').strip() if len(row) > 2 else ''
                    subcategory = (row[3] or '').strip() if len(row) > 3 else ''
                    if product_id:
                        self.product_catalog[product_id] = {
                            'name': product_name or f'Unknown Product {product_id}',
                            'category': category or 'Unknown',
                            'subcategory': subcategory or 'Unknown'
                        }
            self.increment_counter('catalog', 'products_loaded', len(self.product_catalog))
        except Exception:
            self.increment_counter('errors', 'catalog_load_error', 1)

    def mapper_aggregate_by_product(self, _, line):
        """
        Mapper 1: Agrège les ventes par produit
        Clé: product_id
        Valeur: montants/quantités/transactions + infos produit
        """
        try:
            # lignes de type: "<KEY>\t<JSON>"
            parts = line.split('\t', 1)
            if len(parts) < 2:
                return

            key_type = parts[0].strip('"')
            # on ne traite que CLEAN (les rejets ne doivent pas entrer dans le top)
            if key_type != 'CLEAN':
                return

            record = json.loads(parts[1])

            product_id = (record.get('product_id') or '').strip()
            if not product_id:
                return

            unit_price = self._to_float(record.get('unit_price', 0))
            qty = self._to_int(record.get('qty', 0))

            amount = unit_price * qty  # net: retours négatifs déduisent le CA

            # enrichissement depuis le catalogue (fallback Unknown)
            info = self.product_catalog.get(product_id, {
                'name': f'Unknown Product {product_id}',
                'category': 'Unknown',
                'subcategory': 'Unknown'
            })

            self.increment_counter('processing', 'products_processed', 1)

            yield (product_id, {
                'amount': amount,
                'qty': qty,
                'transactions': 1,
                'product_name': info['name'],
                'category': info['category'],
                'subcategory': info['subcategory']
            })

        except Exception:
            self.increment_counter('errors', 'parse_errors', 1)

    def reducer_sum_by_product(self, product_id, values):
        """
        Reducer 1: Somme les ventes par produit
        Émet sur une clé constante 'TOP' pour pouvoir calculer un top global ensuite.
        """
        total_amount = 0.0
        total_qty = 0
        total_transactions = 0
        product_name = ''
        category = ''
        subcategory = ''

        for v in values:
            total_amount += v.get('amount', 0.0)
            total_qty += v.get('qty', 0)
            total_transactions += v.get('transactions', 0)
            if not product_name:
                product_name = v.get('product_name', '')
                category = v.get('category', '')
                subcategory = v.get('subcategory', '')

        yield ('TOP', {
            'product_id': product_id,
            'product_name': product_name,
            'category': category,
            'subcategory': subcategory,
            'net_revenue': round(total_amount, 2),
            'quantity_sold': total_qty,
            'transactions': total_transactions
        })

    def reducer_top_10(self, _, values):
        """
        Reducer 2: Calcule le top 10 global par CA net (une seule réduction)
        """
        # On collecte tout puis on trie (dataset compact après agrégation)
        products = list(values)
        products.sort(key=lambda x: x.get('net_revenue', 0.0), reverse=True)
        for i, p in enumerate(products[:10], 1):
            yield (f'RANK_{i:02d}', {
                'rank': i,
                'product_id': p['product_id'],
                'product_name': p['product_name'],
                'category': p['category'],
                'subcategory': p['subcategory'],
                'net_revenue': p['net_revenue'],
                'quantity_sold': p['quantity_sold'],
                'transactions': p['transactions']
            })

    def steps(self):
        """Définit les étapes MapReduce"""
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper_aggregate_by_product,
                   reducer=self.reducer_sum_by_product),
            # Une seule réduction pour garantir un top 10 global
            MRStep(reducer=self.reducer_top_10,
                   jobconf={'mapreduce.job.reduces': '1'})
        ]


if __name__ == '__main__':
    TopProductsJob.run()

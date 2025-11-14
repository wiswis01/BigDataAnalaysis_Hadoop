import csv
from mrjob.job import MRJob
from mrjob.step import MRStep
"""
Data Cleaning MapReduce Job
Nettoie les données de ventes, supprime les doublons et les lignes invalides
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import re
from datetime import datetime

class DataCleaningJob(MRJob):
    """
    Job MapReduce pour nettoyer les données de ventes
    - Supprime les commentaires et lignes malformées
    - Détecte les doublons par transaction_id
    - Valide les dates et types de données
    """
    
    def configure_args(self):
        super(DataCleaningJob, self).configure_args()
        self.add_passthru_arg('--schema-version', default='v1',
                            help='Version du schéma (v1 ou v2)')
    
    def mapper_init(self):
        
        # Schéma v1 (19 colonnes): 
        self.schema_v1 = [
            'transaction_id', 'ts', 'user_id', 'country', 'city',
            'product_id', 'category', 'subcategory', 'unit_price', 'qty',
            'discount_code', 'payment_type', 'device', 'channel', 'referrer',
            'is_return', 'return_date', 'status', 'notes'
        ]
        
        # Schéma v2 (21 colonnes) - ajoute coupon_value et shipping_cost:
        self.schema_v2 = self.schema_v1 + ['coupon_value', 'shipping_cost']
        
        # Choisir le schéma approprié: 
        version = self.options.schema_version
        self.expected_schema = self.schema_v2 if version == 'v2' else self.schema_v1
        self.expected_cols = len(self.expected_schema)
    
    def mapper(self, _, line):
        """
        Mapper: Nettoie et valide chaque ligne
        Clé: transaction_id (pour détecter doublons)
        Valeur: ligne nettoyée ou erreur
        """
        self.increment_counter('stats', 'total_lines', 1)
        
        # 1.Ignorer les lignes vides: 
        if not line or not line.strip():
            self.increment_counter('errors', 'empty_lines', 1)
            return
        
        # 2.Ignorer les commentaires:
        if line.strip().startswith('#'):
            self.increment_counter('info', 'comments_skipped', 1)
            return
        
        # 3.Ignorer l'en-tête:
        if line.strip().startswith('transaction_id'):
            self.increment_counter('info', 'headers_skipped', 1)
            return
        
        try:
            # Parser le CSV avec gestion des cas complexes:
            reader = csv.reader([line], quotechar='"', delimiter=',', escapechar='\\',
                                skipinitialspace=True)
            row = next(reader)
        
            # Vérifier le nombre de colonnes:
            if len(row) < self.expected_cols:
                self.increment_counter('errors', 'too_few_columns', 1)
                yield ('REJECT', {'error': 'too_few_columns', 'line': line[:100]})
                return
     
            # Éliminer les extras:
            if len(row) > self.expected_cols:
                row = row[:self.expected_cols]

            # Extraire les champs principaux:
            transaction_id = row[0].strip()
            ts = row[1].strip()
            unit_price = row[8].strip()
            qty = row[9].strip()
            
            # Validation transaction_id:
            if not transaction_id or transaction_id == '':
                self.increment_counter('errors', 'missing_transaction_id', 1)
                yield ('REJECT', {'error': 'missing_transaction_id', 'line': line[:100]})
                return
            
            # Validation date :
            if not self._is_valid_date(ts):
                self.increment_counter('errors', 'invalid_date', 1)
                yield ('REJECT', {'error': 'invalid_date', 'line': line[:100]})
                return
            
            # Validation prix et quantité: 
            try:
                price = float(unit_price)
                quantity = int(qty)
                
                # Vérifier les valeurs aberrantes:
                if price < 0 or price > 1000000:
                    self.increment_counter('errors', 'invalid_price', 1)
                    yield ('REJECT', {'error': 'invalid_price', 'line': line[:100]})
                    return
                
                if abs(quantity) > 1000:
                    self.increment_counter('errors', 'invalid_quantity', 1)
                    yield ('REJECT', {'error': 'invalid_quantity', 'line': line[:100]})
                    return
                    
            except ValueError:
                self.increment_counter('errors', 'non_numeric_values', 1)
                yield ('REJECT', {'error': 'non_numeric_values', 'line': line[:100]})
                return
            
            # Créer un dictionnaire structuré:
            record = {}
            for i, field in enumerate(self.expected_schema):
                if i < len(row):
                    record[field] = row[i].strip()
                else:
                    record[field] = ''
            
            # Ligne valide: émettre avec transaction_id comme clé:
            self.increment_counter('stats', 'valid_lines', 1)
            yield (transaction_id, {'type': 'VALID', 'data': record})
            
        except Exception as e:
            self.increment_counter('errors', 'parse_errors', 1)
            yield ('REJECT', {'error': f'parse_error: {str(e)}', 'line': line[:100]})
    
    
    def reducer(self, key, values):
        """
        Reducer: Déduplique les enregistrements
        - Garde la première occurrence de chaque transaction_id
        - Compte les duplicats
        """
        if key == 'REJECT':
            # Émettre toutes les lignes rejetées
            for value in values:
                self.increment_counter('output', 'rejected_lines', 1)
                yield ('REJECT', value)
        else:
            # Dédupliquer les transactions valides
            records = list(values)
            
            if len(records) > 1:
                self.increment_counter('dedup', 'duplicates_found', 1)
            
            # Garder le premier enregistrement
            first_record = records[0]
            if first_record.get('type') == 'VALID':
                self.increment_counter('output', 'clean_records', 1)
                yield ('CLEAN', first_record['data'])
    
    def _is_valid_date(self, date_str):
        try:
            # Accepter: "YYYY-MM-DD HH:MM:SS", "YYYY-MM-DDTHH:MM:SS",
            # "YYYY-MM-DDTHH:MM:SSZ", "YYYY-MM-DDTHH:MM:SS+HH:MM"
            m = re.match(
                r'^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(Z|[+-]\d{2}:\d{2})?$',
                date_str
            )
            if not m:
                return False
            # normalize to "YYYY-MM-DD HH:MM:SS" for strict parsing
            base = f"{m.group(1)} {m.group(2)}"
            datetime.strptime(base, '%Y-%m-%d %H:%M:%S')
            return True
        except Exception:
            return False

    
    def steps(self):
        """Définit les étapes MapReduce"""
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer)
        ]
    


if __name__ == '__main__':
    DataCleaningJob.run()
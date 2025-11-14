from mrjob.job import MRJob
from mrjob.step import MRStep
import json
from datetime import datetime

class SalesAnalysisJob(MRJob):
    # --- helpers (petits, locaux, sans effet de bord) ---
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

    @staticmethod
    def _to_bool_int(x):
        s = str(x).strip().lower()
        return 1 if s in ('1', 'true', 't', 'yes', 'y') else 0

    def mapper_parse_sales(self, _, line):
        try:
            # ligne MRJob: paire "<KEY>\t<JSON>" :
            parts = line.split('\t', 1)
            if len(parts) < 2:
                return

            key_type = parts[0].strip('"')

            # on ne traite que les enregistrements CLEAN:
            if key_type != 'CLEAN':
                return

            record = json.loads(parts[1])

            country = (record.get('country') or '').upper().strip() or 'UNKNOWN'
            ts = record.get('ts', '')

            # date: on accepte juste le YYYY-MM-DD du début:
            try:
                year_month = datetime.strptime(ts[:10], '%Y-%m-%d').strftime('%Y-%m')
            except Exception:
                return  # ignorer les lignes sans date exploitable

            unit_price = self._to_float(record.get('unit_price', 0))
            qty = self._to_int(record.get('qty', 0))
            is_return = self._to_bool_int(record.get('is_return', 0))

            amount = unit_price * qty

            self.increment_counter('processing', 'sales_processed', 1)
            yield ((country, year_month), {
                'amount': amount,
                'transactions': 1,
                'qty': qty
            })

            yield ('RETURN_STATS', {
                'is_return': is_return,
                'qty': abs(qty),
                'amount': abs(amount)
            })

        except Exception:
            self.increment_counter('errors', 'parse_errors', 1)

    def reducer_aggregate_sales(self, key, values):
        if key == 'RETURN_STATS':
            total_qty = 0
            return_qty = 0
            total_amount = 0.0
            return_amount = 0.0

            for v in values:
                qty = v.get('qty', 0)
                amt = v.get('amount', 0.0)
                total_qty += qty
                total_amount += amt
                if v.get('is_return', 0) == 1:
                    return_qty += qty
                    return_amount += amt

            return_rate_qty = (return_qty / total_qty * 100) if total_qty > 0 else 0.0
            return_rate_amount = (return_amount / total_amount * 100) if total_amount > 0 else 0.0

            yield ('RETURN_RATE', {
                'total_quantity': total_qty, 'returned_quantity': return_qty, 'return_rate_by_qty': round(return_rate_qty, 2), 'total_amount': round(total_amount, 2),
                'returned_amount': round(return_amount, 2), 'return_rate_by_amount': round(return_rate_amount, 2)
            })

        else:
            country, year_month = key
            total_amount = 0.0
            total_transactions = 0
            total_qty = 0

            for v in values:
                total_amount += v.get('amount', 0.0)
                total_transactions += v.get('transactions', 0)
                total_qty += v.get('qty', 0)

            yield ((country, year_month), {
                'country': country, 'month': year_month, 'net_sales': round(total_amount, 2), 'transactions': total_transactions, 'quantity_sold': total_qty
            })

    def mapper_format_output(self, key, value):
        if key == 'RETURN_RATE':
            yield ('METRICS', json.dumps({
                'metric_type': 'return_rate',
                'data': value
            }))
        else:
            country, year_month = key
            yield ('SALES', json.dumps({
                'metric_type': 'sales_by_country_month',
                'country': country,
                'month': year_month,
                'net_sales': value['net_sales'],
                'transactions': value['transactions'],
                'quantity_sold': value['quantity_sold']
            }))

    def reducer_separate_outputs(self, key, values):
        for value in values:
            yield (key, value)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse_sales,
                   reducer=self.reducer_aggregate_sales),
            MRStep(mapper=self.mapper_format_output,
                   reducer=self.reducer_separate_outputs)
        ]

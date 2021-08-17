import sys
from time import time
sys.path.append('/Users/tristanbilot/Desktop/bq_data_fetcher')

from bq_fetcher.bq_fetcher import BigQueryFetcher, BigQueryTable

if __name__ == '__main__':
    table = BigQueryTable(
        "vg1np-apps-priceelas-dev01-52",
        "TRISTAN_EXPLO",
        "2GB"
    )
    fetcher = BigQueryFetcher(
        '/Users/tristanbilot/Desktop/bigquery-fast-fetcher/secrets/bq_service_account.json',
        table
    )
    perfect_nb_chunks = fetcher.get_chunk_size_approximation('barcode')
    print(f'nb chunks: {perfect_nb_chunks}')
    
    chunks = fetcher.chunks(
        column='barcode',
        chunk_size=perfect_nb_chunks
    )
    
    for chunk in chunks:
        start = time()
        df = fetcher.fetch(chunk=chunk, nb_cores=1, parallel_backend='joblib')
        end = time() - start
        print(f'Fetching of len {len(df)} in {end}s')
        
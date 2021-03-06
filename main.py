import sys
sys.path.append('/Users/tristanbilot/Desktop/bq_data_fetcher')
from bqfetch.bqfetch import BigQueryFetcher, BigQueryTable

if __name__ == '__main__':
    table = BigQueryTable(
        "PROJECT",
        "DATASET",
        "TABLE"
    )
    fetcher = BigQueryFetcher(table)
    chunks = fetcher.chunks(
        column='id',
        by_chunk_size_in_GB=15,
        verbose=True
    )
    for chunk in chunks:
        df = fetcher.fetch(chunk=chunk, nb_cores=1, parallel_backend='billiard', verbose=True)
        
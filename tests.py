import sys
sys.path.append('/Users/tristanbilot/Desktop/bq_data_fetcher')

from bq_fetcher.bq_fetcher import BigQueryFetcher, BigQueryTable

if __name__ == '__main__':
    table = BigQueryTable(
        "vg1np-apps-priceelas-dev01-52",
        "TRISTAN_EXPLO",
        "2GB"
    )
    fetcher = BigQueryFetcher('/Users/tristanbilot/Desktop/bigquery-fast-fetcher/secrets/bq_service_account.json', table)
    chunks = fetcher.chunks(
        column='barcode',
        chunk_size=20
    )
    
    for chunk in chunks:
        df = fetcher.fetch(chunk=chunk, nb_cores=1, parallel_backend='billiard')
        print(df)
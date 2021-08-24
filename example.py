import sys
sys.path.append('/Users/tristanbilot/Desktop/bq_data_fetcher')
from bqfetch.bqfetch import BigQueryFetcher, BigQueryTable

if __name__ == '__main__':
    table = BigQueryTable(
        "my_project",
        "DATASET",
        "USERS"
    )
    fetcher = BigQueryFetcher(
        '/Users/tristanbilot/Desktop/bigquery-fast-fetcher/secrets/bq_service_account.json',
        table
    )
    chunks = fetcher.chunks(
        column='id',
        by_chunk_size_in_GB=15,
        verbose=True
    )
    # fetch raw table
    for chunk in chunks:
        rows = fetcher.fetch(chunk=chunk, nb_cores=1, as_pandas_df=False)
        for row in rows:
            print(row)

    # fetch as a pandas df
    for chunk in chunks:
        df = fetcher.fetch(chunk=chunk, nb_cores=1)
        print(df)
    
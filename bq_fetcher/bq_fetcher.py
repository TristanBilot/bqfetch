import os
from typing import Iterator, List, Tuple

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

from bq_fetcher.utils import *

CREDS_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/devstorage.full_control"
]

class BigQueryTable:
    '''
    A simple object containing the path to the requested table.
    `project_id` is the name of the BigQuery project, `dataset`
    the BigQuery dataset entry and `table` the name of the 
    requested table.
    '''
    def __init__(
        self,
        project_id: str,
        dataset: str,
        table: str,
    ) -> None:
        self._variables = {
            "PROJECT_ID": project_id,
            "DATASET": dataset,
            "TABLE": table,
        }

    @property
    def variables(self):
        return self._variables 

class BigQueryClient:
    '''
    Wrapper of BigQuery Client object containing credentials.

    Parameters:
    ----------
    service_account_filename: str
        The path and file name of credentials file bq_service_account.json.
        The path should be absolute.
    '''
    def __init__(
        self,
        service_account_filename: str
    ) -> None:
        assert isinstance(service_account_filename, str)

        credentials = service_account.Credentials.from_service_account_file(
            service_account_filename, scopes=CREDS_SCOPES
        )
        bq_client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id
        )
        self._client = bq_client

    def run(
        self,
        request: str
    ) -> bigquery.table.RowIterator:
        """
        Run a SQL BigQuery request.
        """
        job = self._client.query(request)
        return job.result()

class FetchingChunk:
    '''
    Wrapper object used to store the elements to select in the 
    given column.
    '''
    def __init__(self, elements: List[str], column: str,) -> None:
        self.elements = elements
        self.column = column

class BigQueryFetcher:
    '''
    An object used to fetch BigQuery tables easily and progressively
    in order to handle huge tables that does not fit into memory.
    The fetcher divides the table in chunks of size `chunk_size`
    based on the `column` parameter. Then each chunk is fetched 
    using BigQuery Storage API, sequencially or in parallel using
    child processes running on multiple cores.

    Ex: Fetch a huge table of users: first, all the 'user_id' are 
    fetched and divided in chunks of size 50000 (should fit into memory).
    Then, we fetch each small chunk separately using multiprocessing
    with the number of cores available of the machine.

    >>> table = BigQueryTable("my_project", "dataset1", "users_table")
    >>> fetcher = BigQueryFetcher(table)
    >>> chunks = fetcher.chunks('user_id', 50000)
    >>> for chunk in chunks:
            df = fetcher.fetch(chunk, nb_cores=-1)
            # compute df...
    '''
    def __init__(
        self, 
        service_account_filename: str,
        bq_table: BigQueryTable,
    ) -> None:
        self._client = BigQueryClient(service_account_filename)
        self._bq_table = bq_table
        self._service_account_filename = service_account_filename
        self._creds_scopes = CREDS_SCOPES

    def chunks(
        self,
        column: str,
        chunk_size: int,
    ) -> Iterator:
        '''
        Returns a generator on which iterate to get chunks of `column` items of size
        `chunk_size`. This allows to fetch the whole table by multiple chunks
        that can handle in memory.
        '''
        indexes = self._extract_distinct_chunks(self._bq_table, column)
        chunks = scope_splitter(indexes, chunk_size)
        chunks = [FetchingChunk(x[column].tolist(), column) for x in chunks]
        print(chunks)

        for chunk in chunks:
            yield chunk

    def fetch(
        self,
        chunk: FetchingChunk=None,
        nb_cores: int=1,
        parallel_backend: str='billiard',
        partitioned_table_name: str='TMP_TABLE',
    ) -> pd.DataFrame:
        '''
        Fetch a `chunk` using BigQuery Storage API as a pandas Dataframe.
        The `chunk` can be given using the `chunks()` method.

        Parameters:
        ----------
        chunk: FetchingChunk
            A selection of rows that we want to fetch
        nb_cores: int
            The number of processes to create. By default, each process
            will run on a separate core. It is not recommanded to set `nb_cores`
            to a value larger than the number of vCPUs on the machine.
            Setting this parameter to `-1` will use the number of vCPUs on
            the machine.
        parallel_backend: str
            The framework used to parallelize the fetching.
            >>> Choose 'billiard' to use an old fork of the Python multiprocessing lib
            which allows to use multiprocessing from a process launched as daemon
            (ex: Airflow).
            >>> Choose 'joblib' to use the joblib backend.
            >>> Choose 'multiprocessing' to use the current version of Python
            multiprocessing lib.
        partitioned_table_name: str
            The name of the temporary table that will be created in the same dataset as the 
            fetched `bq_table` at each call to fetch() in order to divide the whole table
            in small chunked tables that can be fetched extremly fast.
            This table will be deleted after each execution so no need to delete it manually
            afterwards.
        
        Returns:
        -------
        pd.DataFrame
            A Dataframe containing all the data fetched from the chunk.
        '''
        assert nb_cores == -1 or nb_cores > 0
        assert isinstance(chunk, FetchingChunk)
        assert parallel_backend in ['billiard', 'joblib', 'multiprocessing']

        column = chunk.column
        if column is not None:
            assert chunk is not None
        if chunk is not None:
            assert column is not None

        self._create_partitioned_table(chunk, partitioned_table_name)

        if nb_cores == 1:
            return _fetch_in_parallel(
                (self._service_account_filename, self._creds_scopes, \
                    partitioned_table_name, self._bq_table, column, chunk.elements)
            )
        if nb_cores == -1:
            nb_cores = os.cpu_count()

        # Division of the chunk in n small chunks, with n the number
        # of cores (`nb_cores`).
        chunks_per_core = divide_in_chunks(chunk.elements, nb_cores)
        partition_list = [(self._service_account_filename, self._creds_scopes, \
            partitioned_table_name, self._bq_table, column, item) for item in chunks_per_core]
        
        parallel_backends = {
            'billiard': do_parallel_billiard,
            'joblib': do_parallel_joblib,
            'multiprocessing': do_parallel_multiprocessing,
        }
        parallel_function = parallel_backends[parallel_backend]
        df = pd.concat(parallel_function(
            _fetch_in_parallel,
            nb_cores,
            partition_list
        ))
        self._delete_partitioned_table(partitioned_table_name)
        return df
        
    def _extract_distinct_chunks(
        self,
        table: BigQueryTable,
        column: str,
    ) -> pd.DataFrame:
        '''
        Returns a Dataframe (with 1 col) of distinct elements on the 
        `column` of the `table`.
        '''

        var = table.variables
        query = f'''
            SELECT DISTINCT `{column}`
            FROM `{var["PROJECT_ID"]}.{var["DATASET"]}.{var["TABLE"]}`
        '''
        query_results = self._client.run(query)
        return query_results.to_dataframe()

    def _create_partitioned_table(
        self,
        chunk: FetchingChunk,
        partitioned_table_name: str,
    ):
        '''
        Create a temporary table used to store one `chunk` of data,
        extracted from the main table to fetch. This step is necessary
        in order to improve performances and avoid network bottleneck.
        The table is created with the name `partitioned_table_name` in the
        same dataset as `bq_table`.
        '''
        sqlify_chunk_elements = ','.join(list(map(lambda x: f'"{x}"', chunk.elements)))
        var = self._bq_table.variables
        query = f'''
            CREATE OR REPLACE TABLE
            `{var["PROJECT_ID"]}.{var["DATASET"]}.{partitioned_table_name}` AS
            SELECT 
            * 
            FROM `{var["PROJECT_ID"]}.{var["DATASET"]}.{var["TABLE"]}`
            WHERE barcode IN ({sqlify_chunk_elements})
        '''
        self._client.run(query)

    def _delete_partitioned_table(
        self, 
        partitioned_table_name: str,
    ):
        '''
        Delete the temporary table used to chunk the table.
        '''
        var = self._bq_table.variables
        table = f'{var["PROJECT_ID"]}.{var["DATASET"]}.{partitioned_table_name}'
        self._client.delete_table(table)
        

def _fetch_in_parallel(
    pickled_parameters: Tuple[BigQueryTable, str, List[str]],
) -> pd.DataFrame:
    '''
    Fetch a BigQuery table using Storage API.
    If `chunk` are given, the fetching will return only
    the chunk matching the given list, based on the `column`.
    Warning: imports should not be removed from the inner function
    because dependencies could not be found outside when running
    in child processes.

    Parameters:
    ----------
    bq_table: BigQueryTable
        Table to query.
    column: str
        Column name used as an index to select only chunk from this column.
    chunk: List[str]
        List of elements to select in the query using a IN sql statement.

    Returns:
    -------
    pd.DataFrame
        A dataframe containing the whole table if not chunk are selected,
        or only the rows matching the chunk if using chunk.
    '''
    from google.cloud.bigquery_storage import BigQueryReadClient, ReadSession, DataFormat

    service_account_filename, creds_scopes, partitioned_table_name, bq_table, column, chunk = pickled_parameters

    credentials = service_account.Credentials.from_service_account_file(
        service_account_filename, scopes=creds_scopes
    )
    var = bq_table.variables
    bqstorageclient = BigQueryReadClient(credentials=credentials)
    stringify_table = f"projects/{var['PROJECT_ID']}/datasets/{var['DATASET']}/tables/{partitioned_table_name}"
    parent = "projects/{}".format(var['PROJECT_ID'])

    requested_session = None
    if chunk is not None:
        sqlify_indexes = ','.join(list(map(lambda x: f'"{x}"', chunk)))
        row_filter = ReadSession.TableReadOptions(row_restriction=f'{column} IN ({sqlify_indexes})')
        requested_session = ReadSession(
            table=stringify_table,
            data_format=DataFormat.ARROW,
            read_options=row_filter,
        )
    else:
        requested_session = ReadSession(
            table=stringify_table,
            data_format=DataFormat.ARROW,
        )
    
    session = bqstorageclient.create_read_session(
        parent=parent,
        read_session=requested_session, 
        max_stream_count=1,
    )
    reader = bqstorageclient.read_rows(session.streams[0].name, timeout=10000)
    return reader.to_dataframe(session)
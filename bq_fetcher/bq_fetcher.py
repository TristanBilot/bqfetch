import os
import psutil
import math
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

    def delete_table(
        self,
        table_name: str,
        not_found_ok: bool=True,
    ):
        '''
        Delete a BigQuery table.
        '''
        self._client.delete_table(table_name, not_found_ok=not_found_ok)

class FetchingChunk:
    '''
    Wrapper object used to store the elements to select in the 
    given column.
    '''
    def __init__(self, elements: List[str], column: str,) -> None:
        self.elements = elements
        self.column = column

class InvalidChunkRangeException(Exception):
    pass

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
    >>> fetcher = BigQueryFetcher('path/to/service_account.json', table)
    >>> chunks = fetcher.chunks('user_id', 50000)
    >>> for chunk in chunks:
            df = fetcher.fetch(chunk, nb_cores=-1)
            # compute df...
    '''
    def __init__(
        self, 
        service_account_filename: str,
        bq_table: BigQueryTable,
    ):
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
        indexes = self._extract_distinct_chunks(column)
        chunks = divide_in_chunks(indexes, chunk_size)
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

        vcpu_count = os.cpu_count()
        if nb_cores > vcpu_count:
            print(f'Warning: `nb_cores` ({nb_cores}) greater than cpus on machine ({vcpu_count})')

        optimized_chunks = self.divide_chunk_from_memory(chunk)
        if nb_cores == 1:
            partitioned_table_name = f'{partitioned_table_name}0'
            self._create_partitioned_table(chunk, partitioned_table_name)
            df = _fetch_in_parallel(
                (self._service_account_filename, self._creds_scopes, \
                    partitioned_table_name, self._bq_table, column, chunk.elements)
            )
            self._delete_partitioned_table(partitioned_table_name)
            return df

        if nb_cores == -1:
            nb_cores = vcpu_count

        for i, small_chunk in enumerate(optimized_chunks):
            small_chunk = FetchingChunk(small_chunk, chunk.column)
            self._create_partitioned_table(small_chunk, f'{partitioned_table_name}{i}')

        partition_list = [(self._service_account_filename, self._creds_scopes, \
            f'{partitioned_table_name}{i}', self._bq_table, column, item) for i, item in enumerate(optimized_chunks)]
        
        parallel_backends = {
            'billiard': do_parallel_billiard,
            'joblib': do_parallel_joblib,
            'multiprocessing': do_parallel_multiprocessing,
        }
        parallel_function = parallel_backends[parallel_backend]
        df = pd.concat(parallel_function(
            _fetch_in_parallel,
            len(optimized_chunks),
            partition_list
        ))
        for i in range(len(optimized_chunks)):
            self._delete_partitioned_table(f'{partitioned_table_name}{i}')
        return df

    def get_chunk_size_approximation(
        self,
        column: str,
        nb_GB_to_save: int = 1,
    ) -> int:
        '''
        Tries to give an estimation for the chunk size to used in order to divide the table.
        The approximation uses the free memory available on the machine.
        This approximation only works in the case where the number of distinct values in `column` is 
        approximatively the same. 
        To perform well, we have to predict an average chunk size, so if the size differs too much, the
        prediction will not be accurate.
        Ex: if `column` contains for a given value thousands rows, for a second value only ten rows etc
        the approximation will not work because there is too much variance between each number of values.
        Ex: if `column` refers to IDs, each row is unique so it is perfect use case to use this function.
        The function will throw if there is more than 25% of values that are 25% too far from the mean.
        Parameters:
        ----------
        column: str
            The column name of the table on which do the approximation.

        Returns:
        -------
        chunk_size: int
            The size of one chunk, used later to fetch the table.
        '''
        var = self._bq_table.variables
        nb_occurences_query = f'''
            SELECT COUNT(*)
            FROM `{var["PROJECT_ID"]}.{var["DATASET"]}.{var["TABLE"]}`
            GROUP BY {column}
        '''
        nb_occurences = [nb_occurences[0] for nb_occurences in self._client.run(nb_occurences_query)]
        mean = sum(nb_occurences) / len(nb_occurences)
        coeff = 0.25
        nb_dispersed_values = sum(not (mean * (1 - coeff) < count < mean * (1 + coeff)) \
            for count in nb_occurences)
        dispersion_quotient = nb_dispersed_values / len(nb_occurences)

        if dispersion_quotient > 0.25:
            raise InvalidChunkRangeException(f'''Difference of range between elements of column {column} \
                is too high: more than {coeff * 100}% of elements are too far from the mean.''')

        free_memory_in_GB = (psutil.virtual_memory()[1] - nb_GB_to_save) / 1024**3
        size_of_table_query = f'''
            SELECT SUM(size_bytes)/{1024**3} AS size_GB
            FROM {var["PROJECT_ID"]}.{var["DATASET"]}.__TABLES__
            WHERE table_id = '{var["TABLE"]}'
        '''
        size_of_table_in_GB = next(self._client.run(size_of_table_query).__iter__())[0]
        chunk_size = math.ceil(size_of_table_in_GB / free_memory_in_GB)

        self._size_per_chunk_in_GB = math.ceil(size_of_table_in_GB / chunk_size) # à modifier plus tard
        return chunk_size

    def divide_chunk_from_memory(
        self,
        chunk: FetchingChunk,
        nb_GB_to_save: int = 1,
    ) -> Iterable:
        free_size = min(self._size_per_chunk_in_GB, (psutil.virtual_memory()[1] - nb_GB_to_save) / 1024**3)
        best_performance_table_size_in_GB = 2
        nb_chunks = math.ceil(free_size / best_performance_table_size_in_GB)
        return divide_in_chunks(chunk.elements, nb_chunks)
        
    def _extract_distinct_chunks(
        self,
        column: str,
    ) -> pd.DataFrame:
        '''
        Returns a Dataframe (with 1 col) of distinct elements on the 
        `column` of the `table`.
        '''

        var = self._bq_table.variables
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
            WHERE {chunk.column} IN ({sqlify_chunk_elements})
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
    pickled_parameters: Tuple,
) -> pd.DataFrame:
    '''
    Fetch a BigQuery table using Storage API.
    If `chunk` is given, the fetching will return only
    the chunk matching the given list, based on the `column`.
    Warning: imports should not be removed from the inner function
    because dependencies could not be found outside when running
    in child processes.
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
from time import time
import os
import psutil
import math
from typing import Iterator, List, Tuple

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

from bqfetch.utils import *

CREDS_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/devstorage.full_control"
]
PREFERED_CHUNK_SIZE_IN_GB = 3

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

class FetchingChunk:
    '''
    Wrapper object used to store the elements to select in the 
    given column.
    '''
    def __init__(self, elements: List[str], column: str,) -> None:
        self.elements = elements
        self.column = column

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
        service_account_filename: str,
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

    def get_nb_occurences_for_column(
        self,
        table: BigQueryTable,
        column: str,
    ) -> List[int]:
        '''
        For each distinct element in `column`, counts the number of occurences
        and returns a list containing all the countings.
        Ex: For a column name contaning: John, John, Louis
        >>> [2, 1]
        '''
        var = table.variables
        nb_occurences_query = f'''
            SELECT COUNT(*)
            FROM `{var["PROJECT_ID"]}.{var["DATASET"]}.{var["TABLE"]}`
            GROUP BY {column}
        '''
        nb_occurences = [nb_occurences[0] for nb_occurences in self.run(nb_occurences_query)]
        return nb_occurences

    def get_table_size_in_GB(
        self,
        table: BigQueryTable,
    ) -> int:
        '''
        Returns the size in GB of `table`.
        '''
        var = table.variables
        size_of_table_query = f'''
            SELECT SUM(size_bytes)/{1024**3} AS size_GB
            FROM {var["PROJECT_ID"]}.{var["DATASET"]}.__TABLES__
            WHERE table_id = '{var["TABLE"]}'
        '''
        size_of_table_in_GB = next(self.run(size_of_table_query).__iter__())[0]
        return size_of_table_in_GB

    def get_column_values(
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
        query_results = self.run(query)
        return query_results.to_dataframe()

    def create_partitioned_table(
        self,
        table: BigQueryTable,
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
        var = table.variables
        query = f'''
            CREATE OR REPLACE TABLE
            `{var["PROJECT_ID"]}.{var["DATASET"]}.{partitioned_table_name}` AS
            SELECT 
            * 
            FROM `{var["PROJECT_ID"]}.{var["DATASET"]}.{var["TABLE"]}`
            WHERE {chunk.column} IN ({sqlify_chunk_elements})
        '''
        self.run(query)

    def delete_partitioned_table(
        self,
        table: BigQueryTable,
        partitioned_table_name: str,
    ):
        '''
        Delete the temporary table used to chunk the table.
        '''
        var = table.variables
        table = f'{var["PROJECT_ID"]}.{var["DATASET"]}.{partitioned_table_name}'
        self.delete_table(table)

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
        existing_client: BigQueryClient=None,
    ):
        self._client = existing_client if existing_client is not None \
            else BigQueryClient(service_account_filename)
        self._bq_table = bq_table
        self._service_account_filename = service_account_filename
        self._creds_scopes = CREDS_SCOPES
        self._cache = {}
        self._first_fetch = True

    def chunks(
        self,
        column: str,
        nb_chunks: int,
        verbose: bool=False,
    ) -> Iterator:
        '''
        Returns a list on which iterate to get `nb_chunks` chunks of `column` items.
        It allows to fetch the whole table with multiple chunks that can handle in memory.
        '''
        indexes = self._client.get_column_values(self._bq_table, column)
        chunks = divide_in_chunks(indexes, nb_chunks)
        chunks = [FetchingChunk(x[column].tolist(), column) for x in chunks]

        if verbose:
            log(
                'Chunking',
                f'Nb values in "{column}":\t {len(indexes)}',
                f'Chunk size:\t\t\t {ft(nb_chunks)}',
                f'Nb chunks:\t\t\t {len(chunks)}')
        return chunks

    def fetch(
        self,
        chunk: FetchingChunk=None,
        nb_cores: int=1,
        prefered_chunk_size_in_GB: int=PREFERED_CHUNK_SIZE_IN_GB,
        memory_to_save: int = 1,
        parallel_backend: str='billiard',
        partitioned_table_name: str='TMP_TABLE',
        verbose: bool=False,
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
        prefered_chunk_size_in_GB: int
            The size in GB of the chunks that will be fetched on each process.
            Sometimes, when this value is too high, serializations error can occur.
        memory_to_save: int
            The amount of memory in GB to not use on the machine to avoid overflows.
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
        if nb_cores == -1:
            nb_cores = vcpu_count

        if verbose and self._first_fetch:
            log(
                'Fetching',
                f'Use multiprocessing : \t{nb_cores > 1}',
                f'Nb cores: \t\t\t{nb_cores}',
                f'Parallel backend: \t\t{parallel_backend}')
            self._first_fetch = False

        start = time()
        df = None
        available_memory_in_GB = (psutil.virtual_memory()[1] - memory_to_save) / 1024**3
        optimized_chunks = self._divide_chunk_for_parallel(chunk, nb_cores, prefered_chunk_size_in_GB, \
            available_memory_in_GB)

        if nb_cores == 1:
            partitioned_table_name = f'{partitioned_table_name}0'
            self._client.create_partitioned_table(self._bq_table, chunk, partitioned_table_name)
            df = _fetch_in_parallel(
                (self._service_account_filename, self._creds_scopes, \
                    partitioned_table_name, self._bq_table, column, chunk.elements)
            )
            self._client.delete_partitioned_table(self._bq_table, partitioned_table_name)
        else:
            for i, small_chunk in enumerate(optimized_chunks):
                small_chunk = FetchingChunk(small_chunk, chunk.column)
                self._client.create_partitioned_table(self._bq_table, small_chunk, f'{partitioned_table_name}{i}')

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
                self._client.delete_partitioned_table(self._bq_table, f'{partitioned_table_name}{i}')
        end = time() - start

        if verbose:
            log(
                f'Time to fetch:\t\t {round(end, 2)}s',
                f'Nb lines in dataframe:\t {len(df)}',
                f'Size of dataframe:\t\t {ft(df.memory_usage(deep=True).sum() / 1024**3)}')
        return df

    def get_chunk_size_approximation(
        self,
        column: str,
        nb_cores: int=1,
        nb_GB_to_save: int = 1,
        prefered_chunk_size_in_GB: int = PREFERED_CHUNK_SIZE_IN_GB,
        verbose: bool=False,
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
        nb_occurences = self._client.get_nb_occurences_for_column(self._bq_table, column)
        mean = sum(nb_occurences) / len(nb_occurences)
        coeff = 0.25
        nb_dispersed_values = sum(not (mean * (1 - coeff) < count < mean * (1 + coeff)) \
            for count in nb_occurences)
        dispersion_quotient = nb_dispersed_values / len(nb_occurences)

        if dispersion_quotient > coeff:
            raise InvalidChunkRangeException(f'''Difference of range between elements of column {column} \
                is too high: more than {coeff * 100}% of elements are too far from the mean.''')

        available_memory_in_GB = (psutil.virtual_memory()[1] - nb_GB_to_save) / 1024**3
        chunk_size, size_of_table_in_GB = self._chunk_size_approximation_formula(nb_cores, prefered_chunk_size_in_GB, \
            available_memory_in_GB)
        self._cache['size_per_chunk_in_GB'] = math.ceil(size_of_table_in_GB / chunk_size)

        if verbose:
            log(
                'Chunk size approximation',
                f'Available memory on device:\t {ft(available_memory_in_GB)}',
                f'Size of table:\t\t {ft(size_of_table_in_GB)}',
                f'Prefered size of chunk:\t {ft(PREFERED_CHUNK_SIZE_IN_GB)}',
                f'Size per chunk:\t\t {ft(self._cache["size_per_chunk_in_GB"])}',
                f'Chunk size approximation:\t {chunk_size}')
        return chunk_size

    def _divide_chunk_for_parallel(
        self,
        chunk: FetchingChunk,
        nb_cores: int,
        prefered_chunk_size_in_GB: int,
        available_memory_in_GB: int,
    ) -> Iterable:
        '''
        Divides a chunk that fit in memory into smaller chunks in order
        to compute them faster when using multiprocessing.
        `nb_GB_to_save`: number of GB to not use on the machine.
        We have to estimate both the chunk and table in GB in order to divide properly.
        `prefered_chunk_size_in_GB`: the size for which the chunk will be divised.
        Ex: for a chunk of size 10GB, and a `prefered_chunk_size_in_GB` of 2
        >>> 5 chunks of size 2GB
        '''
        if 'size_per_chunk_in_GB' not in self._cache:

            # if the size per chunk is not already computed, we have to do it in order
            # to make the calculus
            chunk_size, size_of_table_in_GB = self._chunk_size_approximation_formula( \
                nb_cores, prefered_chunk_size_in_GB, available_memory_in_GB)
            self._cache['size_per_chunk_in_GB'] = math.ceil(size_of_table_in_GB / chunk_size)

        # choose either the full chunk if it fits in memory, or the available space if
        # the chunk is too big
        free_size = min(self._cache['size_per_chunk_in_GB'], available_memory_in_GB)
        nb_chunks = math.ceil(free_size / prefered_chunk_size_in_GB)
        return divide_in_chunks(chunk.elements, nb_chunks)
    
    def _chunk_size_approximation_formula(
        self,
        nb_cores: int,
        prefered_chunk_size_in_GB: int,
        available_memory_in_GB: int,
    ):
        '''
        Returns an estimated chunk_size in the case the number of occurences are correctly 
        dispersed. This estimation is based on the free memory and the number of cores.
        Returns also the size of the table for cache and performance reasons.
        '''
        size_of_table_in_GB = self._client.get_table_size_in_GB(self._bq_table)
        sum_of_GB_for_cores = prefered_chunk_size_in_GB * nb_cores
        chunk_size = math.ceil(size_of_table_in_GB / min(sum_of_GB_for_cores, available_memory_in_GB))
        return chunk_size, size_of_table_in_GB
        

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
    Function should be global, not inside a class.
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
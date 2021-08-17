from dfetch.dfetch import BigQueryFetcher, BigQueryTable, FetchingChunk
import unittest

bq_table_mock = BigQueryTable(
    "mock",
    "mock",
    "mock"
)

class BigQueryClientMock:
    def __init__(self, table_size) -> None:
        self.table_size = table_size

    def get_table_size_in_GB(
        self,
        _: BigQueryTable,
    ) -> int:
            return self.table_size

class TestFetcher(unittest.TestCase):

    def test_chunk_size_approx1(self):
        table_size = 100
        client = BigQueryClientMock(table_size)
        fetcher = BigQueryFetcher("mock", bq_table_mock, client)
        nb_cores = 2
        chunk_size = 2
        free_memory = 10
        chunk_approx, _ = fetcher._chunk_size_approximation_formula(nb_cores, chunk_size, free_memory)
        self.assertEqual(chunk_approx, 25)

    def test_chunk_size_approx2(self):
        table_size = 250
        client = BigQueryClientMock(table_size)
        fetcher = BigQueryFetcher("mock", bq_table_mock, client)
        nb_cores = 96
        chunk_size = 2
        free_memory = 240
        chunk_approx, _ = fetcher._chunk_size_approximation_formula(nb_cores, chunk_size, free_memory)
        self.assertEqual(chunk_approx, 2)

    def test_chunk_size_approx3(self):
        table_size = 250
        client = BigQueryClientMock(table_size)
        fetcher = BigQueryFetcher("mock", bq_table_mock, client)
        nb_cores = 6
        chunk_size = 2
        free_memory = 240
        chunk_approx, _ = fetcher._chunk_size_approximation_formula(nb_cores, chunk_size, free_memory)
        self.assertEqual(chunk_approx, 21)

    def test_divide_chunk_for_parallel1(self):
        table_size = 250
        client = BigQueryClientMock(table_size)
        fetcher = BigQueryFetcher("mock", bq_table_mock, client)
        nb_cores = 6
        chunk_size = 2
        free_memory = 240
        elements = list(range(30000))
        chunk = FetchingChunk(elements, 'column')
        parallel_chunks = fetcher._divide_chunk_for_parallel(chunk, nb_cores, chunk_size, free_memory)
        self.assertEqual(len(parallel_chunks), 6)

    def test_divide_chunk_for_parallel2(self):
        table_size = 10
        client = BigQueryClientMock(table_size)
        fetcher = BigQueryFetcher("mock", bq_table_mock, client)
        nb_cores = 3
        chunk_size = 2
        free_memory = 3
        elements = list(range(300))
        chunk = FetchingChunk(elements, 'column')
        parallel_chunks = fetcher._divide_chunk_for_parallel(chunk, nb_cores, chunk_size, free_memory)
        self.assertEqual(len(parallel_chunks), 2)

if __name__ == '__main__':
    unittest.main()
<p align="center"><img width="15%" src="https://raw.githubusercontent.com/TristanBilot/bqfetch/master/.github/logo.svg"/></p>

<p align="center">
  <a href=""><img src="https://img.shields.io/github/last-commit/tristanbilot/bqfetch" alt="Last commit"></a>
  <a href="https://img.shields.io/github/languages/count/tristanbilot/bqfetch"><img src="https://img.shields.io/github/languages/count/tristanbilot/bqfetch" alt="Languages"></a>
  <a href=""><img src="https://img.shields.io/github/release-date/tristanbilot/bqfetch" alt="Release date"></a>
  <br>
  <a href=""><img src="https://img.shields.io/badge/Python-%3E%3D3.6-blue" alt="Python version"></a>
  <a href=""><img src="https://img.shields.io/github/license/tristanbilot/bqfetch" alt="Python version"></a>
</p>

# <p align="center">bqfetch<p>
**A lightweight tool to fetch tables from BigQuery as pandas DataFrames very fast using BigQuery Storage API combined with multiprocessing.**


## Installation
```
pip install bqfetch
pip install -r requirements.txt
```
  
## Algorithm
  * Fetch all distinct values from the given index `column`.
  * Divide these indices in `chunks` based on the available memory and the number of cores on the machine.
  * `If multiprocessing`:
      * Each chunk will be divided in multiple sub-chunks based on the parameter `nb_cores` and the available memory.
      * For each sub-chunk, create a temporary table containing all the matching rows in the whole table.
      * Fetch these temporary tables using BigQuery Storage as dataframes.
      * Merge the dataframes.
      * Delete temporary tables.
  * `If !multiprocessing`:
      * Same process with only one temporary table and no parallel processes created.

## Use case

### Fetching a huge table of users using multiple cores
|  id |   Name  | Age |
|:---:|:-------:|:---:|
| 187 | Bartolomé |  30 |
| 188 | Tristan |  22 |
| ... |   ...   | ... |

```python
>>> table = BigQueryTable("my_project", "dataset1", "users_table")
>>> fetcher = BigQueryFetcher('/path/to/service_account.json', table)
>>> nb_chunks = 10
>>> chunks = fetcher.chunks('id', nb_chunks)

>>> for chunk in chunks:
        df = fetcher.fetch(chunk, nb_cores=-1, parallel_backend='billiard')
        # ...
```
  
* First, we have to create a `BigQueryTable` object which contains the path to the BigQuery table stored in GCP.
* A fetcher is created, given in parameter the absolute path to the service_account.json file, the file is mandatory in order to do operations in GCP.
* Define the number of chunks to divide the table. Ex: if `nb_chunks` is set to 10, then the whole values in the index column will be fetched and divised in 10. However, setting `nb_chunks` to 10 does not mean that the table will necessarly be divided equally in 10 parts because some values in the index column can appear more than other and vice versa, causing that a value containing multiple row will be considered the same as a value containing only one row in the table.
* Chunk the whole table, given the `column` name and the chunk size. In this case, choosing the **id** column is perfect because this each value of this column appears the same number of times: 1 time.
* For each chunk, fetch it.
    * `nb_cores`=-1 will use the number of cores available on the machine.
    * `parallel_backend`='billiard' | 'joblib' | 'multiprocessing' specify the backend framework to use.

## Chunk size approximation function
In some cases, choosing the good `chunk_size` can be difficult, so a function is available to approximate the perfect size to chunk the table. However, this function will throw if there is too much variance between the number of values in the index column (if more than 25% of the values appear more or less than 25% of the mean of the appearance of all the values in the column).
  
```python
>>> table = BigQueryTable("my_project", "dataset1", "users_table")
>>> fetcher = BigQueryFetcher('/path/to/service_account.json', table)
>>> perfect_nb_chunks = fetcher.get_chunk_size_approximation('id')
>>> chunks = fetcher.chunks('id', perfect_nb_chunks)
```

## Verbose mode

```python
>>> perfect_nb_chunks = fetcher.get_chunk_size_approximation('barcode', verbose=True)
# Available memory on device:  7.04GB
# Size of table:               2.19GB
# Prefered size of chunk:      3GB
# Size per chunk:              3GB
# Chunk size approximation:    1

>>> chunks = fetcher.chunks(column='id', chunk_size=perfect_nb_chunks, verbose=True)
# Nb values in "id":           96
# Chunk size:                  1GB
# Nb chunks:                   1
  
>>> for chunk in chunks:
>>>        df = fetcher.fetch(chunk=chunk, nb_cores=1, parallel_backend='joblib', verbose=True)
# Use multiprocessing :        False
# Nb cores:                    1
# Parallel backend:            joblib

# Time to fetch:               102.21s
# Nb lines in dataframe:       3375875
# Size of dataframe:           2.83GB
```
  
## Warning
We recommend to use this tool only in the case where the table to fetch contains a column that can be easily chunked (divided in small parts). Thus the perfect column to achieve this is a column containing distinct values or values that appear ~ the same number of time. **If some values appear thousands of times and some only fews, then the chunking will not be reliable** because we need to make the assumption that each chunk will be approximatively the same size in order to estimate the needed memory necessary to fetch in an optimize way the table.
  
### A good index colum:
This column contains distinct values so can be divided in chunks easily.
  
|  Card number |
|:---:|
|  |
| 4390 3849 ... |
| 2903 1182 ... |
| 0562 7205 ... |
| ... |
  
### A bad index colum:
This column can contains a lot of variance between values so the chunking will not be reliable.

|  Age |
|:---:|
|  |
| 18 |
| 18 |
| 64 |
| 18 |
| ... |

### More cores != faster
I remind you that adding more cores to the fetching process will not necessarily gain performance and most of the time it will even be slower. The reason is that the fetching is directly dependent on the Internet bandwidth available on your network, not the number of working cores or the computer power. However, you can do your own tests and in some cases the multiprocessing can gain time (ex: in the case where cloud machines allow only an amount of bandwidth by core, multiplying the number of cores will also multiplying the bandwidth, ex: GCP compute engines).

## Contribution
bqfetch is open to new contributors, especially for bug fixing or implementation of new features. Do not hesitate to open an issue/pull request :)
  
## License
  <a href="https://opensource.org/licenses/MIT">MIT</a>
  
  Copyright (c) 2021-present, Tristan Bilot

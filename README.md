<p align="center"><img width="15%" src="readme/logo.svg"/></p>

<p align="center">
  <a href=""><img src="https://img.shields.io/github/last-commit/tristanbilot/dfetch" alt="Last commit"></a>
  <a href="https://img.shields.io/github/languages/count/tristanbilot/dfetch"><img src="https://img.shields.io/github/languages/count/tristanbilot/dfetch" alt="Languages"></a>
  <a href=""><img src="https://img.shields.io/github/release-date/tristanbilot/dfetch" alt="Release date"></a>
  <br>
  <a href=""><img src="https://img.shields.io/badge/Python-%3E%3D3.6-blue" alt="Python version"></a>
  <a href=""><img src="https://img.shields.io/github/license/tristanbilot/dfetch" alt="Python version"></a>
</p>

# <p align="center">dfetch<p>
##### A small tool to fetch tables from BigQuery very fast using BigQuery Storage API combined with multiprocessing.

## Use cases

### Fetching a huge table of users using multiple cores
|  id |   Name  | Age |
|:---:|:-------:|:---:|
| 188 | Tristan |  22 |
| ... |   ...   | ... |

First, we have to divide the whole table in chunks based on a column name. If the table contains 10M lines and we want
first, all the 'user_id' are 
    fetched and divided in chunks of size 50000 (should fit into memory).
    Then, we fetch each small chunk separately using multiprocessing
    with the number of cores available of the machine.
    
```python
>>> table = BigQueryTable("my_project", "dataset1", "users_table")
>>> fetcher = BigQueryFetcher('path/to/service_account', table)
>>> chunk_size = 50000
>>> chunks = fetcher.chunks('user_id', chunk_size)

>>> for chunk in chunks:
        df = fetcher.fetch(chunk, nb_cores=-1)
        # ...
```

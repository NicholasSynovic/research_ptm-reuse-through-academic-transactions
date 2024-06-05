import sqlite3
from sqlite3 import Connection, Cursor
from typing import Iterator
from urllib.parse import urlparse

import pandas as pd
from pandas import DataFrame, Series


def count_papers_in_db(column: str, table: str, conn: Connection) -> int:
    query = f"SELECT COUNT(DISTINCT {column}) FROM {table}"
    cursor: Cursor = conn.execute(query)
    return cursor.fetchone()[0]


# papers per publication for PM//no publication specification for OA
def count_papers_per_journalPM(conn: Connection):
    def extract_base_url(url):
        parsed_url = urlparse(url)
        return f"{parsed_url.scheme}://{parsed_url.netloc}"

    query = f"SELECT * FROM paper"
    df = pd.read_sql_query(query, con=conn)
    print(df["url"].apply(extract_base_url).value_counts())


# function that creates and return dataframe without making changes and pass in as parameter to other functions, won't have to do
# like query below over and over again


def count_PMarxiv_papers_in_OA(PMconn: Connection, OAconn: Connection):
    paper_count = 0

    query = f"SELECT * FROM paper"
    PM_df: DataFrame = pd.read_sql_query(query, con=PMconn)
    arxiv_series: Series = PM_df["url"].str.lower()
    arxiv_formatted: Series = arxiv_series.str.replace(
        pat="https://arxiv.org/abs/", repl="10.48550/arxiv."
    )

    OA_query = f"SELECT oa_id, doi FROM works"
    OA_df: Iterator[DataFrame] = pd.read_sql_query(
        OA_query, con=OAconn, chunksize=10000
    )

    df: DataFrame
    for df in OA_df:
        df_lower: Series = df["doi"].str.lower()
        paper_count += df["doi"][df_lower.isin(arxiv_formatted)].size
    return paper_count


def proportion_PM_papers_in_OA(PMconn: Connection):
    # num of unique PM papers in OA db LEFT OFF HERE ON SLIDDECK
    PMproportion = count_papers_in_db(
        column="paper_id", table="model_to_paper", conn=PMconn
    )
    OAproportion = count_papers_in_db(column="doi", table="works", conn=OAconn)
    print("Proportion of PM papers in OA dataset: ", PMproportion / OAproportion)


# total citations across the board
def generate_total_cites():
    OA_works_column = "work"
    query = f"SELECT {OA_works_column} FROM cites"
    OA_works_df = pd.read_sql_query(query, con=OAconn)

    total_cites = OA_works_df.shape[0]
    print("Total number of citations from any work to any other work: ", total_cites)


OA_file_path = "/Users/fran-pellegrino/Desktop/ptm-reuse_academic_transactions/research_ptm-reuse-through-academic-transactions/nature/db/feedStorage/prod.db"
OAconn = sqlite3.Connection(database=OA_file_path)

PM_file_path = "/Users/fran-pellegrino/Desktop/ptm-reuse_academic_transactions/research_ptm-reuse-through-academic-transactions/nature/db/feedStorage/PeaTMOSS.db"
PMconn = sqlite3.Connection(database=PM_file_path)

if __name__ == "__main__":
    print("PM results per publication:")
    count_papers_per_journalPM(PMconn)
    quit()

    print("PM arxiv papers in OA: ", count_PMarxiv_papers_in_OA(PMconn, OAconn))

    print(
        "PM database count: ",
        count_papers_in_db(column="paper_id", table="model_to_paper", conn=PMconn),
    )
    print(
        "OA database count: ",
        count_papers_in_db(column="doi", table="works", conn=OAconn),
    )

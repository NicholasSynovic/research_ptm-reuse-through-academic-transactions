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
    # num of unique PM papers in OA db
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


# papers per publication for PM//not impt for OA
def count_papers_per_journalPM(conn: Connection):
    def extract_base_url(url):
        parsed_url = urlparse(url)
        return f"{parsed_url.scheme}://{parsed_url.netloc}"

    query = f"SELECT * FROM paper"
    df = pd.read_sql_query(query, con=conn)
    print(df["url"].apply(extract_base_url).value_counts())


# access PM titles of papers without URLs, cross-ref with OA titles and access those papers' DOIs
def unknown_URL_PMpapers_getting_DOI_from_OA(PMconn: Connection, OAconn: Connection):
    PMquery = f"SELECT title, url FROM paper WHERE (url IS NULL OR url = '')"
    PM_titles_nullURLs_df = pd.read_sql_query(PMquery, PMconn)

    OAquery = f"SELECT doi, title FROM works WHERE (doi IS NOT NULL)"
    OA_titles_df = pd.read_sql_query(OAquery, OAconn)

    PM_titles_nullURLs_df["title_normalized"] = (
        PM_titles_nullURLs_df["title"].str.strip().str.lower()
    )
    OA_titles_df["title_normalized"] = OA_titles_df["title"].str.strip().str.lower()

    # Filter OA DataFrame based on  normalized titles in PM_df
    filtered_df = OA_titles_df[
        OA_titles_df["title_normalized"].isin(PM_titles_nullURLs_df["title_normalized"])
    ]
    filtered_df_unique = filtered_df.drop_duplicates(subset="title_normalized")
    print(filtered_df_unique)


OA_file_path = "/Users/fran-pellegrino/Desktop/ptm-reuse_academic_transactions/research_ptm-reuse-through-academic-transactions/nature/db/feedStorage/prod.db"
OAconn = sqlite3.Connection(database=OA_file_path)

PM_file_path = "/Users/fran-pellegrino/Desktop/ptm-reuse_academic_transactions/research_ptm-reuse-through-academic-transactions/nature/db/feedStorage/PeaTMOSS.db"
PMconn = sqlite3.Connection(database=PM_file_path)

if __name__ == "__main__":
    unknown_URL_PMpapers_getting_DOI_from_OA(PMconn, OAconn)
    quit()

    print("PM results per publication:")
    count_papers_per_journalPM(PMconn)

    print("PM arxiv papers in OA: ", count_PMarxiv_papers_in_OA(PMconn, OAconn))

    print(
        "PM database count: ",
        count_papers_in_db(column="paper_id", table="model_to_paper", conn=PMconn),
    )
    print(
        "OA database count: ",
        count_papers_in_db(column="doi", table="works", conn=OAconn),
    )

import pickle
import sqlite3
from math import ceil
from sqlite3 import Connection, Cursor
from typing import Iterator
from urllib.parse import urlparse

import numpy
import pandas as pd
import requests
from pandas import DataFrame, Series
from progress.bar import PixelBar


def getNumberOfCitations(file_path: str) -> int:
    # sqlQuery: str = "SELECT id FROM cites ORDER BY id DESC LIMIT 1"
    # conn = sqlite3.Connection(database=file_path)
    # cursor: Cursor = conn.execute(sqlQuery)
    # return cursor.fetchone()[0]
    return 113563323


def getNumberOfWorks(file_path: str) -> int:
    # sqlQuery: str = "SELECT COUNT(oa_id) FROM works"
    # conn = sqlite3.Connection(database=file_path)
    # cursor: Cursor = conn.execute(sqlQuery)
    # return cursor.fetchone()[0]
    return 13435534


def createDFGeneratorFromSQL(
    file_path: str, column: str, table_from_db: str, chunksize: int
) -> Iterator[DataFrame]:  # incorporate across functions
    conn = sqlite3.Connection(database=file_path)
    query = f"SELECT {column} FROM {table_from_db}"
    df = pd.read_sql_query(query, con=conn, chunksize=chunksize)
    return df


def create_df_from_db(
    file_path: str, column: str, table_from_db: str
) -> DataFrame:  # incorporate across functions
    conn = sqlite3.Connection(database=file_path)
    query = f"SELECT {column} FROM {table_from_db}"
    df = pd.read_sql_query(query, con=conn)
    return df


def count_papers_in_db(column: str, table: str, conn: Connection) -> int:
    query = f"SELECT COUNT(DISTINCT {column} FROM {table}"
    cursor: Cursor = conn.execute(query)
    return cursor.fetchone()[0]


def proportion_PM_papers_in_OA(PMconn: Connection):
    # num of unique PM papers in OA db
    PMproportion = count_papers_in_db(
        column="paper_id", table="model_to_paper", conn=PMconn
    )
    OAproportion = count_papers_in_db(column="doi", table="works", conn=OAconn)
    print("Proportion of PM papers in OA dataset: ", PMproportion / OAproportion)


# papers per publication for PM
def count_papers_per_journalPM(conn: Connection, db_file_path: str):
    def extract_base_url(url):
        parsed_url = urlparse(url)
        return f"{parsed_url.scheme}://{parsed_url.netloc}"

    df = create_df_from_db(db_file_path, "*", "paper")
    print(df["url"].apply(extract_base_url).value_counts())


def count_PMarxiv_papers_in_OA(
    PMconn: Connection, OAconn: Connection
):  # option - do for other major publications in PM
    paper_count = 0

    query = f"SELECT * FROM paper"
    PM_df = create_df_from_db(PM_file_path, "*", "paper")  # PM_df: Dataframe = syntax?
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


# access PM titles of papers without URLs, cross-ref with OA titles and access those papers' DOIs//new vis for publications of papers with no URLs for underreported/misreported vis
# find publications for entries in OA that have no DOI that correspond to entries in PM without URLs by brute force publication identification, still want to vis in underreported/general
def unknown_URL_PMpapers_getting_DOI_from_OA(PMconn: Connection, OAconn: Connection):
    print("Hello WOrld")
    PMquery = f"SELECT title, url FROM paper WHERE (url IS NULL OR url = '')"
    PM_titles_nullURLs_df = pd.read_sql_query(PMquery, PMconn)

    print("hello WOrld")
    OAquery = f"SELECT doi, title FROM works WHERE (doi IS NOT NULL)"
    OA_titles_df = pd.read_sql_query(OAquery, OAconn)

    PM_titles_nullURLs_df["title_normalized"] = (
        PM_titles_nullURLs_df["title"].str.strip().str.lower()
    )
    OA_titles_df["title_normalized"] = OA_titles_df["title"].str.strip().str.lower()

    # Filter OA DataFrame based on  normalized titles in PM_df
    print("Hello Oworld")
    filtered_df = OA_titles_df[
        OA_titles_df["title_normalized"].isin(PM_titles_nullURLs_df["title_normalized"])
    ]

    # return df with redirected urls based on doi
    doi_array = filtered_df["doi"].to_numpy()
    doi_URLs_df = pd.DataFrame(columns=["url"])
    doi_URLs_list = []
    for doi in doi_array:
        initial_url = "https://doi.org/" + doi
        redirection_with_doi = requests.get(initial_url, allow_redirects=True)
        final_url = redirection_with_doi.url
        doi_URLs_list.append(pd.DataFrame({"url": [final_url]}))
    doi_URLs_df = pd.concat(doi_URLs_list, ignore_index=True)

    def extract_base_url(url):
        parsed_url = urlparse(url)
        return f"{parsed_url.scheme}://{parsed_url.netloc}"

    print(doi_URLs_df["url"].apply(extract_base_url).value_counts())


# Citation relationships


# total citations across the board
def generate_total_cites():
    OA_works_column = "work"

    OA_works_df = create_df_from_db(OA_file_path, OA_works_column, "cites")

    total_cites = OA_works_df.shape[0]
    print("Total number of citations from any work to any other work: ", total_cites)


# tally of citations made in OA that correspond to papers in PM - total number and organized dataframe based on number of unique matches
def total_citations_of_PM_papers():
    def standardize_columns(df):
        return df.map(lambda x: x.strip().lower() if isinstance(x, str) else x)

    titleStore: list[DataFrame] = []
    citesStore: list[DataFrame] = []

    chunksize: int = 1000
    numberOfCitations: int = getNumberOfCitations(OA_file_path)
    numberOfWorks: int = getNumberOfWorks(file_path=OA_file_path)

    PM_titles_df = create_df_from_db(PM_file_path, "title", "paper")
    peatmossDF: DataFrame = standardize_columns(PM_titles_df)

    OA_titles_df: Iterator[DataFrame] = createDFGeneratorFromSQL(
        OA_file_path, "oa_id, title", "works", chunksize=chunksize
    )

    with PixelBar(
        "Iterating through OA Works table...", max=ceil(numberOfWorks / chunksize)
    ) as bar:
        df: DataFrame
        for df in OA_titles_df:
            df.replace(to_replace=" ", value=None, inplace=True)
            df.dropna(inplace=True)
            df = standardize_columns(df)
            titleStore.append(df[df["title"].isin(peatmossDF["title"])])
            bar.next()

    oaTitlesDF: DataFrame = pd.concat(objs=titleStore)

    # print(oaTitlesDF)

    oaCitesDFs: Iterator[DataFrame] = createDFGeneratorFromSQL(
        OA_file_path, "work, reference", "cites", chunksize=chunksize * 10
    )
    with PixelBar(
        "Iterating through OA Cites table... ",
        max=ceil(numberOfCitations / (chunksize * 10)),
    ) as bar:
        df: DataFrame
        for df in oaCitesDFs:
            df = standardize_columns(df)
            citesStore.append(df[df["reference"].isin(oaTitlesDF["oa_id"])])
            bar.next()

    oaCitesDF: DataFrame = pd.concat(objs=citesStore)

    oaCitesDF["reference"].value_counts().to_json(path_or_buf="OA_Citing_PM.json")

    PMcitesOAStore: list[DataFrame] = []
    with PixelBar(
        "Iterating through OA Cites table... ",
        max=ceil(numberOfCitations / (chunksize * 10)),
    ) as bar:
        df: DataFrame
        for df in oaCitesDFs:
            df = standardize_columns(df)
            PMcitesOAStore.append(df[df["work"].isin(oaTitlesDF["oa_id"])])
            bar.next()

    oaCitesDF: DataFrame = pd.concat(objs=citesStore)

    oaCitesDF["work"].value_counts().to_json(path_or_buf="PM_Citing_OA.json")

    # use name of json file and read json pandas function for stats, functionality within names of json themselves!

    # quit()

    # print('Hellow World')
    # OA_cites_df = create_df_from_db(OA_file_path, 'work, reference', 'cites')

    # print("Got cites")

    # OA_titles_df = create_df_from_db(OA_file_path, 'oa_id, title', 'works')

    # print("Got titles")

    # with open(file="oa_cites.pickle", mode="wb") as pfile:
    #     pickle.dump(obj=OA_cites_df)
    #     pfile.close()

    # with open(file="oa_title.pickle", mode="wb") as pfile:
    #     pickle.dump(obj=OA_titles_df)
    #     pfile.close()

    # print('Hellow World')

    # quit()

    # # merging for titles in work column of cites table
    # merged1 = OA_cites_df.merge(OA_titles_df, left_on='work', right_on='oa_id', how='left')
    # merged1 = merged1.rename(columns={'title': 'work_titles'})

    # print('Hellow World')

    # # merging for titles in reference column of cites
    # OA_titles_of_cites_merged = merged1.merge(OA_titles_df, left_on='reference', right_on='oa_id', how='left')
    # OA_titles_of_cites_merged = OA_titles_of_cites_merged.rename(columns={'title': 'ref_titles'})

    # print('Hellow World')

    # # delete extra columns created by merge & oa_id columns
    # OA_titles_of_cites_merged = OA_titles_of_cites_merged.drop(columns=['oa_id_x', 'oa_id_y', 'work', 'reference'])

    # print('Hellow World')

    # # strip whitespace and convert to lowercase
    # def standardize_columns(df):
    #     return df.applymap(lambda x: x.strip().lower() if isinstance(x, str) else x)
    # OA_cites_titles_standardized = standardize_columns(OA_titles_of_cites_merged)
    # PM_titles_standardized = standardize_columns(PM_titles_df)

    # print('Hellow World')

    # # total number of papers being cited that're also in PM
    # matches = PM_titles_standardized["title"].isin(OA_cites_titles_standardized["ref_titles"])
    # total_num_cited_inPM = matches.sum()

    # print('The total number of papers cited in OA that correspond to papers in PM is: ', total_num_cited_inPM)

    # # organize matches based on title and number of unique occurrences in both
    # OA_match_column = OA_cites_titles_standardized['ref_titles']
    # PM_match_column = PM_titles_standardized['title']
    # unique_occurrences = OA_match_column.value_counts().reindex(PM_match_column).fillna(0).astype(int)

    # print('Hellow World')

    # occurrences_df = pd.DataFrame({
    #     'title': PM_match_column,
    #     'cited': unique_occurrences.values
    # })

    # print('Hellow World')

    # occurrences_df = occurrences_df.sort_values(by='cited', ascending=False).reset_index(drop=True)

    # print('Dataframe of PM papers used as citations in OA db organized by unique occurrence per paper: ', occurrences_df)
    # OA_titles_of_cites_merged = OA_titles_of_cites_merged.drop(columns=['oa_id_x', 'oa_id_y', 'work', 'reference'])

    # print('Hellow World')

    # # strip whitespace and convert to lowercase
    # def standardize_columns(df):
    #     return df.applymap(lambda x: x.strip().lower() if isinstance(x, str) else x)
    # OA_cites_titles_standardized = standardize_columns(OA_titles_of_cites_merged)
    # PM_titles_standardized = standardize_columns(PM_titles_df)

    # print('Hellow World')

    # # total number of papers being cited that're also in PM
    # matches = PM_titles_standardized["title"].isin(OA_cites_titles_standardized["ref_titles"])
    # total_num_cited_inPM = matches.sum()

    # print('The total number of papers cited in OA that correspond to papers in PM is: ', total_num_cited_inPM)

    # # organize matches based on title and number of unique occurrences in both
    # OA_match_column = OA_cites_titles_standardized['ref_titles']
    # PM_match_column = PM_titles_standardized['title']
    # unique_occurrences = OA_match_column.value_counts().reindex(PM_match_column).fillna(0).astype(int)

    # print('Hellow World')

    # occurrences_df = pd.DataFrame({
    #     'title': PM_match_column,
    #     'cited': unique_occurrences.values
    # })

    # print('Hellow World')

    # occurrences_df = occurrences_df.sort_values(by='cited', ascending=False).reset_index(drop=True)

    # print('Dataframe of PM papers used as citations in OA db organized by unique occurrence per paper: ', occurrences_df)


OA_file_path = "/Users/fran-pellegrino/Desktop/ptm-reuse_academic_transactions/research_ptm-reuse-through-academic-transactions/nature/db/feedStorage/prod.db"
OAconn = sqlite3.Connection(database=OA_file_path)


PM_file_path = "/Users/fran-pellegrino/Desktop/ptm-reuse_academic_transactions/research_ptm-reuse-through-academic-transactions/nature/db/feedStorage/PeaTMOSS.db"
PMconn = sqlite3.Connection(database=PM_file_path)


if __name__ == "__main__":
    total_citations_of_PM_papers()
    quit()

    unknown_URL_PMpapers_getting_DOI_from_OA(PMconn, OAconn)

    print("PM arxiv papers in OA: ", count_PMarxiv_papers_in_OA(PMconn, OAconn))

    print(
        "PM database count: ",
        count_papers_in_db(column="paper_id", table="model_to_paper", conn=PMconn),
    )
    print(
        "OA database count: ",
        count_papers_in_db(column="doi", table="works", conn=OAconn),
    )

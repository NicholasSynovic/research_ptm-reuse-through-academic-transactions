import json
import pickle
import sqlite3
from math import ceil
from pathlib import Path
from sqlite3 import Connection, Cursor
from typing import Any, Iterator
from urllib.parse import ParseResult, urlparse

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import seaborn as sns
from pandas import DataFrame, Series
from progress.bar import Bar
from progress.spinner import Spinner

from src.stats import OA_CITATION_COUNT, OA_OAID_COUNT


def _runOneValueSQLQuery(db: Connection, query: str) -> Iterator[Any]:
    """
    _runOneValueSQLQuery Execute an SQL query that returns one value

    :param db: An sqlite3.Connection object
    :type db: Connection
    :param query: A SQLite3 compatible query
    :type query: str
    :return: An iterator containing any value
    :rtype: Iterator[Any]
    """
    cursor: Cursor = db.execute(query)
    return cursor.fetchone()


def _createDFGeneratorFromSQL(
    db: Connection,
    query: str,
    chunkSize: int = 10000,
) -> Iterator[DataFrame]:
    """
    _createDFGeneratorFromSQL Return a generator of Pandas DataFrames to process large SQL query results

    :param db: An sqlite3.Connection object
    :type db: Connection
    :param query: The SQLite3 compatible query to run
    :type query: str
    :param chunkSize: The number of rows per DataFrame to return, defaults to 10000
    :type chunkSize: int, optional
    :return: A generator of Pandas DataFrames
    :rtype: _type_
    :yield: A pandas.DataFrame
    :rtype: Iterator[DataFrame]
    """
    return pd.read_sql_query(query, con=db, chunksize=chunkSize)


def _createDFFromSQL(db: Connection, query: str) -> DataFrame:
    """
    _createDFFromSQL Return a Pandas DataFrame of the results from a SQL query

    Only useful for small SQL query results (less than 10,000 rows returned)

    :param db: An sqlite3.Connection object
    :type db: Connection
    :param query: The SQLite3 compatible query to run
    :type query: str
    :return: A pandas.DataFrame of the SQL query results
    :rtype: DataFrame
    """
    return pd.read_sql_query(query, con=db)


def connectToDB(dbPath: Path) -> Connection:
    """
    connectToDB Connect to a SQLite3 database and return the sqlite3.Connection object

    :param dbPath: Filepath to a SQLite3 database
    :type dbPath: Path
    :return: The sqlite3.Connection object
    :rtype: Connection
    """
    return Connection(database=dbPath)


def oa_CountPapersByDOI(oaDB: Connection) -> int:
    """
    oa_CountPapersByDOI Count the number of papers within an OpenAlex dataset by the unique DOI

    DOIs that contain a space are excluded from the count

    :param oaDB: A sqlite3.Connection object to an OpenAlex dataset
    :type oaDB: Connection
    :return: The number of papers in the dataset that have a unqiue DOI
    :rtype: int
    """
    doiCount: int = 0
    query: str = "SELECT DISTINCT doi FROM works"
    dfs: Iterator[DataFrame] = _createDFGeneratorFromSQL(db=oaDB, query=query)

    with Spinner(message="Counting number of query...") as spinner:
        df: DataFrame
        for df in dfs:
            df["doi"].replace(to_replace=" ", value=None, inplace=True)
            df.dropna(inplace=True)
            doiCount += df.shape[0]
            spinner.next()

    return doiCount


def oa_CountPapersByOAID(
    oaDB: Connection,
    returnDefault: bool = True,
) -> int:
    """
    oa_CountPapersByOAID Count the number of papers within an OpenAlex dataset by the unique OpenAlex ID

    :param oaDB: A sqlite3.Connection object to an OpenAlex dataset
    :type oaDB: Connection
    :param returnDefault: Skip computing the value and use the pre-computed value, defaults to True
    :type returnDefault: bool, optional
    :return: The number of papers in the dataset that have a unqiue OpenAlex ID
    :rtype: int
    """
    query: str = "SELECT COUNT(DISTINCT oa_id) FROM works"
    if returnDefault:
        return OA_OAID_COUNT
    else:
        return _runOneValueSQLQuery(db=oaDB, query=query)[0]


def oa_ProportionOfValidPapers(oaIDCount: int, oaDOICount: int) -> float:
    """
    oa_ProportionOfValidPapers Compute the proportion of papers with a valid DOI in an OpenAlex dataset

    :param oaIDCount: Number of papers that have an OpenAlex ID
    :type oaIDCount: int
    :param oaDOICount: Number of papers that have a DOI
    :type oaDOICount: int
    :return: The proportion of DOI papers over OpenAlex ID papers
    :rtype: float
    """
    return oaDOICount / oaIDCount


def oa_CountCitations(
    oaDB: Connection,
    returnDefault: bool = True,
) -> int:
    """
    oa_CountCitations Return the number of citations in an OpenAlex database

    :param oaDB: A  sqlite3.Connection object to an OpenAlex dataset
    :type oaDB: Connection
    :param returnDefault: Skip computing the value and use the pre-computed value, defaults to True
    :type returnDefault: bool, optional
    :return: The number of citations in the OpenAlex dataset
    :rtype: int
    """
    query: str = "SELECT id FROM cites ORDER BY id DESC LIMIT 1"
    if returnDefault:
        return OA_CITATION_COUNT
    else:
        return _runOneValueSQLQuery(db=oaDB, query=query)


def pm_CountPapersByID(pbDB: Connection) -> int:
    """
    pm_CountPapersByID Count the number of PeaTMOSS papers by their paper ID

    :param pbDB: A sqlite3.Connection to a PeaTMOSS database
    :type pbDB: Connection
    :return: The number of papers in a PeaTMOSS database
    :rtype: int
    """
    query: str = "SELECT COUNT(DISTINCT paper_id) FROM model_to_paper"
    return _runOneValueSQLQuery(db=pbDB, query=query)


def oapm_ProportionOfPMPapersInOA(
    oaPapers: int,
    pmPapers: int,
) -> float:
    """
    oapm_ProportionOfPMPapersInOA Compute the proportion of PeaTMOSS papers captured by OpenAlex

    :param oaPapers: Number of papers in a OpenAlex dataset
    :type oaPapers: int
    :param pmPapers: Number of papers in a PeaTMOSS dataset
    :type pmPapers: int
    :return: The proportion of PeaTMOSS papers over OpenAlex papers
    :rtype: float
    """
    return pmPapers / oaPapers


def pm_CountPapersPerJournal(pmDB: Connection) -> Series[int]:
    """
    pm_CountPapersPerJournal Count the number of papers per journal in PeaTMOSS

    :param pmDB: A sqlite3.Connection object to a PeaTMOSS database
    :type pmDB: Connection
    :return: A pandas.Series[int] object of the number of papers per journal
    :rtype: Series[int]
    """

    def _extractNetLoc(url: str) -> str:
        """
        _extractNetLoc Return the netloc attribute of a URL if it exists

        :param url: A URL to parse
        :type url: str
        :return: the netloc attribute of the URL
        :rtype: str
        """
        return urlparse(url=url).netloc

    query: str = "SELECT url FROM paper"
    df: DataFrame = _createDFFromSQL(db=pmDB, query=query)
    df["url"] = df["url"].apply(_extractNetLoc)
    return df["url"].value_counts(sort=True, dropna=False)


# def count_PMarxiv_papers_in_OA(
#     PMconn: Connection, OAconn: Connection
# ):  # option - do for other major publications in PM
#     paper_count = 0

#     query = f"SELECT * FROM paper"
#     PM_df = create_df_from_db(PM_file_path, "*", "paper")  # PM_df: Dataframe = syntax?
#     arxiv_series: Series = PM_df["url"].str.lower()
#     arxiv_formatted: Series = arxiv_series.str.replace(
#         pat="https://arxiv.org/abs/", repl="10.48550/arxiv."
#     )

#     OA_query = f"SELECT oa_id, doi FROM works"
#     OA_df: Iterator[DataFrame] = pd.read_sql_query(
#         OA_query, con=OAconn, chunksize=10000
#     )

#     df: DataFrame
#     for df in OA_df:
#         df_lower: Series = df["doi"].str.lower()
#         paper_count += df["doi"][df_lower.isin(arxiv_formatted)].size
#     return paper_count


# # access PM titles of papers without URLs, cross-ref with OA titles and access those papers' DOIs//new vis for publications of papers with no URLs for underreported/misreported vis
# # find publications for entries in OA that have no DOI that correspond to entries in PM without URLs by brute force publication identification, still want to vis in underreported/general
# def unknown_URL_PMpapers_getting_DOI_from_OA(PMconn: Connection, OAconn: Connection):
#     print("Hello WOrld")
#     PMquery = f"SELECT title, url FROM paper WHERE (url IS NULL OR url = '')"
#     PM_titles_nullURLs_df = pd.read_sql_query(PMquery, PMconn)

#     print("hello WOrld")
#     OAquery = f"SELECT doi, title FROM works WHERE (doi IS NOT NULL)"
#     OA_titles_df = pd.read_sql_query(OAquery, OAconn)

#     PM_titles_nullURLs_df["title_normalized"] = (
#         PM_titles_nullURLs_df["title"].str.strip().str.lower()
#     )
#     OA_titles_df["title_normalized"] = OA_titles_df["title"].str.strip().str.lower()

#     # Filter OA DataFrame based on  normalized titles in PM_df
#     print("Hello Oworld")
#     filtered_df = OA_titles_df[
#         OA_titles_df["title_normalized"].isin(PM_titles_nullURLs_df["title_normalized"])
#     ]

#     # return df with redirected urls based on doi
#     doi_array = filtered_df["doi"].to_numpy()
#     doi_URLs_df = pd.DataFrame(columns=["url"])
#     doi_URLs_list = []
#     for doi in doi_array:
#         initial_url = "https://doi.org/" + doi
#         redirection_with_doi = requests.get(initial_url, allow_redirects=True)
#         final_url = redirection_with_doi.url
#         doi_URLs_list.append(pd.DataFrame({"url": [final_url]}))
#     doi_URLs_df = pd.concat(doi_URLs_list, ignore_index=True)

#     def extract_base_url(url):
#         parsed_url = urlparse(url)
#         return f"{parsed_url.scheme}://{parsed_url.netloc}"

#     print(doi_URLs_df["url"].apply(extract_base_url).value_counts())


# # Citation relationships


# # total citations across the board
# def generate_total_cites():
#     OA_works_column = "work"

#     OA_works_df = create_df_from_db(OA_file_path, OA_works_column, "cites")

#     total_cites = OA_works_df.shape[0]
#     print("Total number of citations from any work to any other work: ", total_cites)


# # tally of citations made in OA that correspond to papers in PM - total number and organized dataframe based on number of unique matches
# def total_citations_of_PM_papers():
#     def standardize_columns(df):
#         return df.map(lambda x: x.strip().lower() if isinstance(x, str) else x)

#     titleStore: list[DataFrame] = []
#     citesStore: list[DataFrame] = []

#     chunksize: int = 1000
#     numberOfCitations: int = getNumberOfCitations(OA_file_path)
#     numberOfWorks: int = getNumberOfWorks(file_path=OA_file_path)

#     PM_titles_df = create_df_from_db(PM_file_path, "title", "paper")
#     peatmossDF: DataFrame = standardize_columns(PM_titles_df)

#     OA_titles_df: Iterator[DataFrame] = createDFGeneratorFromSQL(
#         OA_file_path, "oa_id, title", "works", chunksize=chunksize
#     )

#     with PixelBar(
#         "Iterating through OA Works table...", max=ceil(numberOfWorks / chunksize)
#     ) as bar:
#         df: DataFrame
#         for df in OA_titles_df:
#             df.replace(to_replace=" ", value=None, inplace=True)
#             df.dropna(inplace=True)
#             df = standardize_columns(df)
#             titleStore.append(df[df["title"].isin(peatmossDF["title"])])
#             bar.next()

#     oaTitlesDF: DataFrame = pd.concat(objs=titleStore)

#     # print(oaTitlesDF)

#     oaCitesDFs: Iterator[DataFrame] = createDFGeneratorFromSQL(
#         OA_file_path, "work, reference", "cites", chunksize=chunksize * 10
#     )
#     with PixelBar(
#         "Iterating through OA Cites table... ",
#         max=ceil(numberOfCitations / (chunksize * 10)),
#     ) as bar:
#         df: DataFrame
#         for df in oaCitesDFs:
#             df = standardize_columns(df)
#             citesStore.append(df[df["reference"].isin(oaTitlesDF["oa_id"])])
#             bar.next()

#     oaCitesDF: DataFrame = pd.concat(objs=citesStore)

#     oaCitesDF["reference"].value_counts().to_json(path_or_buf="OA_Citing_PM.json")

#     PMcitesOAStore: list[DataFrame] = []
#     with PixelBar(
#         "Iterating through OA Cites table... ",
#         max=ceil(numberOfCitations / (chunksize * 10)),
#     ) as bar:
#         df: DataFrame
#         for df in oaCitesDFs:
#             df = standardize_columns(df)
#             PMcitesOAStore.append(df[df["work"].isin(oaTitlesDF["oa_id"])])
#             bar.next()

#     oaCitesDF: DataFrame = pd.concat(objs=citesStore)

#     oaCitesDF["work"].value_counts().to_json(path_or_buf="PM_Citing_OA.json")

#     # use name of json file and read json pandas function for stats, functionality within names of json themselves


# def dataset_comparison():
#     # labels = ['OpenAlex Dataset', 'PeaTMOSS Dataset']
#     # values = [7885681, 1937]

#     # fig, ax = plt.subplots()
#     # ax.bar(labels, values)
#     # ax.set_xlabel('Dataset')
#     # ax.set_ylabel('Number of Unique Papers')
#     # ax.set_title('Comparison of Paper Counts between OpenAlex & PeaTMOSS')
#     # ax.set_yscale('log')
#     # #ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: '{:.16g}'.format(y)))
#     # plt.savefig('dataset_comparison.png')
#     # plt.show()

#     data = {
#         "Dataset": ["OpenAlex Dataset", "PeaTMOSS Dataset"],
#         "Number of Unique Papers": [7885681, 1937],
#     }
#     df = pd.DataFrame(data)
#     sns.set(style="darkgrid")

#     plt.figure(figsize=(10, 6))
#     bar_plot = sns.barplot(x="Dataset", y="Number of Unique Papers", data=df)

#     # Set the title and labels
#     bar_plot.set_title("Comparison of Paper Counts betwen OpenAlex & PeaTMOSS")
#     bar_plot.set_xlabel("Dataset")
#     bar_plot.set_ylabel("Number of Unique Papers")

#     bar_plot.set_yscale("log")

#     # Rotate x-axis labels if needed
#     bar_plot.set_xticklabels(
#         bar_plot.get_xticklabels(), rotation=45, horizontalalignment="right"
#     )

#     # Save the plot as a PNG file
#     plt.savefig("dataset_comparison", bbox_inches="tight")


# def PM_publication_venues():
#     data = {
#         "Publication": ["arXiv", "Other", "ACL Anthology", "GitHub", "Hugging Face"],
#         "Unique Papers": [1151, 418, 152, 64, 57],
#     }
#     df = pd.DataFrame(data)
#     sns.set(style="darkgrid")

#     plt.figure(figsize=(10, 6))
#     bar_plot = sns.barplot(x="Publication", y="Unique Papers", data=df)

#     # Set the title and labels
#     bar_plot.set_title("Popular Publication Venues")
#     bar_plot.set_xlabel("Publication")
#     bar_plot.set_ylabel("Unique Papers")

#     # Rotate x-axis labels if needed
#     bar_plot.set_xticklabels(
#         bar_plot.get_xticklabels(), rotation=45, horizontalalignment="right"
#     )

#     # Save the plot as a PNG file
#     plt.savefig("PM_publication_venues", bbox_inches="tight")


# def PM_DOIs_citedby_OA(top_num_of_models: int):
#     def standardize_columns(df):
#         return df.map(lambda x: x.strip().lower() if isinstance(x, str) else x)

#     OA_doi_df: Iterator[DataFrame] = createDFGeneratorFromSQL(
#         OA_file_path, "oa_id, doi", "works", 10000
#     )
#     standardized_chunks = []
#     for chunk in OA_doi_df:
#         standardized_chunk = standardize_columns(chunk)
#         standardized_chunks.append(standardized_chunk)
#     OA_doi_df_stand = pd.concat(standardized_chunks)

#     with open(OA_citing_PM, "r") as f:
#         citation_data = json.load(f)

#     # JSON -> df
#     citation_df = pd.DataFrame(
#         list(citation_data.items()), columns=["oa_id", "citation_count"]
#     )
#     citation_df_stand = standardize_columns(citation_df)

#     # putting OA and JSON together based on JSON
#     filtered_OA_JSON = pd.merge(OA_doi_df_stand, citation_df_stand, on="oa_id")
#     filtered_OA_JSON_sort = filtered_OA_JSON.sort_values(
#         by="citation_count", ascending=False
#     )

#     top_models_df = filtered_OA_JSON_sort.head(top_num_of_models)

#     # bar plot
#     sns.set(style="darkgrid")
#     plt.figure(figsize=(10, 6))
#     sns.barplot(x="doi", y="citation_count", data=top_models_df)
#     plt.title(
#         "Top "
#         + str(top_num_of_models)
#         + " PeaTMOSS Models by Citation Count".format(top_num_of_models)
#     )
#     plt.xlabel("DOI")
#     plt.ylabel("Number of Citations")
#     plt.xticks(rotation=45, ha="center")
#     plt.tight_layout()
#     plt.savefig("top_PMmodels_cited_byOA", bbox_inches="tight")


# OA_citing_PM = "/Users/fran-pellegrino/Desktop/ptm-reuse_academic_transactions/research_ptm-reuse-through-academic-transactions/OA_Citing_PM.json"

# OA_file_path = "/Users/fran-pellegrino/Desktop/ptm-reuse_academic_transactions/research_ptm-reuse-through-academic-transactions/nature/db/feedStorage/prod.db"
# OAconn = sqlite3.Connection(database=OA_file_path)


# PM_file_path = "/Users/fran-pellegrino/Desktop/ptm-reuse_academic_transactions/research_ptm-reuse-through-academic-transactions/nature/db/feedStorage/PeaTMOSS.db"
# PMconn = sqlite3.Connection(database=PM_file_path)


def main() -> None:
    pass


if __name__ == "__main__":
    main()

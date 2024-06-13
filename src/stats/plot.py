import json
import sqlite3
from pathlib import Path
from sqlite3 import Connection
from typing import Iterator, List

import click
import matplotlib.pyplot as plt
import pandas as pd
import requests
import seaborn
from humanize import intcomma
from matplotlib.axes import Axes
from pandas import DataFrame

from src.stats.stats import *


def _humanizeInt(number: int | float) -> str:
    """
    _humanizeInt Humanize a number into an interger string

    :param number: A value to humanize
    :type number: int | float
    :return: A humanized version of the `number` parameter
    :rtype: str
    """
    return intcomma(value=number, ndigits=0)


def _renameURL(url: str) -> str:
    url = url.strip()

    if url == "":
        return "Unknown"

    hostname: str = url.split(".")[0]

    match hostname:
        case "arxiv":
            return "arXiv"
        case "aclanthology":
            return "ACL\nAnthology"
        case "github":
            return "GitHub"
        case "huggingface":
            return "Hugging Face"
        case _:
            return hostname


def plot_DatasetSizes(
    oaSize: int,
    pmSize: int,
    filepath: Path,
) -> None:
    """
    plot_DatasetSizes Plot the number of papers of OpenAlex and PeaTMOSS

    :param oaSize: Number of papers in the OpenAlex dataset
    :type oaSize: int
    :param pmSize: Number of papers in the PeaTMOSS dataset
    :type pmSize: int
    :param filepath: A path to save the output figure
    :type filepath: Path
    """
    data: dict[str, List[str | int]] = {
        "x": ["OpenAlex", "PeaTMOSS"],
        "y": [oaSize, pmSize],
    }

    df: DataFrame = DataFrame(data=data)

    graph: Axes = seaborn.barplot(data=df, x="x", y="y")
    graph.set_yscale(value="log")
    plt.title(label="Number of Papers per Dataset")
    plt.xlabel(xlabel="Dataset")
    plt.ylabel(ylabel="Number of Papers")
    graph.bar_label(
        container=graph.containers[0],
        fmt=_humanizeInt,
    )
    plt.savefig(filepath)
    plt.clf()


def plot_PMPublicationVenuePaperCount(
    venuePaperCounts: Series,
    filepath: Path,
) -> None:
    data: Series = venuePaperCounts.iloc[0:4]

    url: str
    for url in data.index:
        data = data.rename({url: _renameURL(url=url)})

    other: int = venuePaperCounts[4:].sum()
    data["Other"] = other
    data.sort_values(inplace=True, ascending=False)

    df: DataFrame = DataFrame(data=data)
    graph: Axes = seaborn.barplot(
        data=df,
        x="url",
        y="count",
    )
    plt.title(label="Number of PeaTMOSS Papers per Venue")
    plt.xlabel(xlabel="Venue", labelpad=7)
    plt.ylabel(ylabel="Number of Papers")
    graph.bar_label(
        container=graph.containers[0],
        fmt=_humanizeInt,
    )

    plt.tight_layout()
    plt.savefig(filepath)
    plt.clf()


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


@click.command()
@click.option(
    "-p",
    "--peatmoss",
    "pmPath",
    type=Path,
    help="Path to PeaTMOSS database",
    required=True,
)
@click.option(
    "-o",
    "--openalex",
    "oaPath",
    type=Path,
    help="Path to OpenAlex database",
    required=True,
)
def main(pmPath: Path, oaPath: Path) -> None:
    seaborn.set_style(style="darkgrid")

    absPMPath: Path = resolvePath(path=pmPath)
    absOAPath: Path = resolvePath(path=oaPath)

    assert isFile(path=absPMPath)
    assert isFile(path=absOAPath)

    pmDB: Connection = connectToDB(dbPath=absPMPath)
    oaDB: Connection = connectToDB(dbPath=absOAPath)

    oaPaperCounts: int = oa_CountPapersByDOI(oaDB=oaDB)
    pmPaperCounts: int = pm_CountPapersByID(pmDB=pmDB)

    pmVenueCounts: Series = pm_CountPapersPerJournal(
        pmDB=pmDB,
    )
    plot_PMPublicationVenuePaperCount(
        venuePaperCounts=pmVenueCounts,
        filepath="numberofPeaTMOSSPapersPerVenue.png",
    )

    plot_DatasetSizes(
        oaSize=oaPaperCounts,
        pmSize=pmPaperCounts,
        filepath=Path("comparisonOfDatasetPaperCounts.png"),
    )


if __name__ == "__main__":
    main()

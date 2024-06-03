import sqlite3

import pandas as pd

OA_file_path = "/Users/fran-pellegrino/Desktop/ptm-reuse_academic_transactions/research_ptm-reuse-through-academic-transactions/nature/db/feedStorage/prod.db"
conn = sqlite3.Connection(database=OA_file_path)

PM_file_path = "/Users/fran-pellegrino/Desktop/ptm-reuse_academic_transactions/research_ptm-reuse-through-academic-transactions/nature/db/feedStorage/PeaTMOSS.db"
PMconn = sqlite3.Connection(database=PM_file_path)


# OpenAlex database read-in and calculations
def count_papers_in_OA():
    OA_DOI_column = "doi"
    query = f"SELECT {OA_DOI_column} FROM works"
    OA_doi_df = pd.read_sql_query(query, con=conn)

    # # of unique papers in OA
    OA_unique_paper_num = OA_doi_df[OA_DOI_column].nunique()
    print("# of unique papers in OA db: ", OA_unique_paper_num)


# PeaTMOSS database read-in and calculations
def generate_PM_stats():
    PM_id_column = "paper_id"
    query = f"SELECT {PM_id_column} FROM model_to_paper"
    PM_model_df = pd.read_sql_query(query, con=PMconn)

    # # of unique papers in PM
    PM_unique_paper_num = PM_model_df[PM_id_column].nunique()
    print("# of unique papers in PM db: ", PM_unique_paper_num)


# total citations across the board
def generate_total_cites():
    OA_works_column = "work"
    query = f"SELECT {OA_works_column} FROM cites"
    OA_works_df = pd.read_sql_query(query, con=conn)

    total_cites = OA_works_df.shape[0]
    print("Total number of citations from any work to any other work: ", total_cites)


# unique PM papers in OA dataset
def generate_unique_PMpapers_OAdataset():
    # OA titles

    OA_works_column = "title"
    query = f"SELECT {OA_works_column} FROM works"
    OA_titles_df = pd.read_sql_query(query, con=conn)

    # PM titles

    PM_id_column = "title"
    query = f"SELECT {PM_id_column} FROM model_to_paper"
    PM_titles_df = pd.read_sql_query(query, con=PMconn)

    # finding PM papers that are also in OA dataset; filter duplicate titles
    OA_titles_unique = OA_titles_df.drop_duplicates()
    PM_titles_unique = PM_titles_df.drop_duplicates()

    paper_intersection = PM_titles_unique[PM_titles_unique.isin(OA_titles_unique)]
    num_common_titles = paper_intersection.nunique()
    print("total num of unique PM papers in OA dataset: ", num_common_titles)


if __name__ == "__main__":
    # generate_OA_stats()
    # generate_PM_stats()
    # generate_total_cites()
    count_papers_in_OA()

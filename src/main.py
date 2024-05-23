import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import Engine, create_engine
from streamlit.runtime.uploaded_file_manager import UploadedFile

from src import APP_AUTHORS, APP_DESCRIPTION, APP_TITLE


def debug_PrintFileStats(file: UploadedFile) -> None:
    st.write(file.file_id)
    st.write(file.name)
    st.write(file)


def buildPage() -> None:
    st.title(body=APP_TITLE)
    st.markdown(
        body=f"> {APP_DESCRIPTION}",
        help=f"Created by {', '.join(APP_AUTHORS)}",
    )
    st.divider()

    st.markdown(body="## SQLite Database Picker")
    st.markdown(body="> Pick a SQLite3 database to utilize")
    dbFile: UploadedFile | None = st.file_uploader(
        label="SQLite3 Database Uploader",
        type="db",
        accept_multiple_files=False,
        help="No database will be created or copied, however, the filename of \
                the database will be used to establish a connection",
        on_change=debug_PrintFileStats,
    )

    st.markdown(body="## DOI Search")
    st.markdown(body="> Search for DOIs captured within our database")
    with st.form(key="doi-search", clear_on_submit=False, border=True):
        doiInput: str | None = st.text_input(
            "DOI Search Bar",
            value="10.48550/arXiv.2404.14619",
            help='Input can be in the form of \
"https://doi.org/10.48550/arXiv.2404.14619" or \
"10.48550/arXiv.2404.14619"',
        )
        formSubmit: bool = st.form_submit_button(label="Search")


def print_doi_df():
    pathToDB = "/Users/fran-pellegrino/Library/CloudStorage/OneDrive-LoyolaUniversityChicago/Internship-Awards/LUC USRE 2024/Code/research_ptm-reuse-through-academic-transactions/nature/db/feedStorage/nature.db"
    sql_column_read_in = "SELECT doi FROM entries"

    with sqlite3.connect(database=pathToDB) as conn:
        df = pd.read_sql_query(sql_column_read_in, conn)
    return df


def convert_sqlDB_to_pdDF():
    pathToDB = "/Users/fran-pellegrino/Library/CloudStorage/OneDrive-LoyolaUniversityChicago/Internship-Awards/LUC USRE 2024/Code/research_ptm-reuse-through-academic-transactions/nature/db/feedStorage/nature.db"
    sql_read_in = "SELECT * FROM entries"

    with sqlite3.connect(database=pathToDB) as conn:
        full_db = pd.read_sql(sql_read_in, conn)
    # with-structure managing context of closing conn
    return full_db


def main() -> None:
    st.title("DOI Search Engine")
    st.write(
        """
    Filter for DOI of specific PTM ~ filter DOIs @ bottom
    """
    )

    st.write("Original Dataframe:")

    # Printing initial DF test commit comitt e there
    df = print_doi_df()
    st.dataframe(df)

    # reading in entire sql db as pd df
    full_db = convert_sqlDB_to_pdDF()

    # Enter button alignment CSS
    st.markdown(
        """
    <style>
    .search-bar-container {
        display: flex;
        align-items: center;
    }
    .search-bar-container > div {
        flex-grow: 1;
    }
    .search-bar-container button {
        margin-left: 10px;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )

    with st.form(key="search_form"):
        st.markdown('<div class="search-bar-container">', unsafe_allow_html=True)

        # columns = st.columns([1,1])
        # with columns[0]:
        input_search = st.text_input("Search through PTM DOIs here", "")

        # with columns[1]:
        submit_button = st.form_submit_button("Enter")

        st.markdown("</div>", unsafe_allow_html=True)

    if submit_button:
        if input_search:
            result = df.apply(
                lambda row: row.astype(str).str.contains(input_search).any(), axis=1
            )
            filtered_df = full_db[result]

            if not filtered_df.empty:
                st.write("Search Results:")
                st.dataframe(filtered_df)
            else:
                st.write("No result.")
        else:
            st.write("Please enter a search and then press Enter.")
    else:
        st.write("Enter a search query or possible DOI and press Enter.")

    # Given a row from the SQL database, query a Neo4J graph
    # Visualize relevant data


if __name__ == "__main__":
    buildPage()


# streamlit run main.py
# git stage -A
# git commit -m "commit text here"
# git commit -m "text --no-verify"

# source env/bin/activate
# deactivate

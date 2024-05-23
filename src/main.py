import sqlite3
from pathlib import Path
from typing import Literal

import pandas as pd
import streamlit as st
from streamlit.delta_generator import DeltaGenerator

from src import APP_AUTHORS, APP_DESCRIPTION, APP_TITLE
from src.components import USER_HOME
from src.components.filepicker import tk_FilePicker


def updateFilePathInputLabel() -> None:
    filePath: Path | Literal[False] = tk_FilePicker()

    if filePath != False:
        st.session_state["db_filepath_label"] = filePath


def createSessionState() -> None:
    if "db_filepath_label" not in st.session_state:
        st.session_state["db_filepath_label"] = USER_HOME


def buildPage() -> None:
    st.title(body=APP_TITLE)
    st.markdown(
        body=f"> {APP_DESCRIPTION}",
        help=f"Created by {', '.join(APP_AUTHORS)}",
    )
    st.divider()

    st.markdown(body="## SQLite Database Picker")
    st.markdown(body="> Pick a SQLite3 database to utilize")
    with st.container(border=True):
        st.text_input(
            label="Select an SQLite3 database file",
            value=st.session_state["db_filepath_label"],
            help='Use the "Select Database" button to open a file picker widget',
            disabled=True,
        )
        column1: DeltaGenerator
        column2: DeltaGenerator
        column1, column2 = st.columns(spec=2, gap="large")

        with column1:
            st.button(
                label="Select Database",
                use_container_width=True,
                on_click=updateFilePathInputLabel,
            )

        with column2:
            st.button(
                label="Confirm Database Selection",
                use_container_width=True,
            )


#     st.markdown(body="## DOI Search")
#     st.markdown(body="> Search for DOIs captured within our database")
#     with st.form(key="doi-search", clear_on_submit=False, border=True):
#         doiInput: str | None = st.text_input(
#             "DOI Search Bar",
#             value="10.48550/arXiv.2404.14619",
#             help='Input can be in the form of \
# "https://doi.org/10.48550/arXiv.2404.14619" or \
# "10.48550/arXiv.2404.14619"',
#         )
#         formSubmit: bool = st.form_submit_button(label="Search")


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
    createSessionState()
    buildPage()


# streamlit run main.py
# git stage -A
# git commit -m "commit text here"
# git commit -m "text --no-verify"

# source env/bin/activate
# deactivate

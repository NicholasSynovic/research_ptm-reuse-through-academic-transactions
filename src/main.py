import sqlite3
from pathlib import Path
from typing import Literal

import pandas
import streamlit as st
from pandas import DataFrame
from sqlalchemy import Connection, Engine, TextClause, create_engine, text
from sqlalchemy.exc import DatabaseError, OperationalError
from streamlit.delta_generator import DeltaGenerator

from src import (
    APP_AUTHORS,
    APP_DESCRIPTION,
    APP_TITLE,
    ERROR_DB_CONN,
    ERROR_DB_QUERYING,
)
from src.components import USER_HOME
from src.components.filepicker import tk_FilePicker


def updateFilePathInputLabel() -> None:
    filePath: Path | Literal[False] = tk_FilePicker()

    if filePath != False:
        st.session_state["db_filepath_label"] = filePath


def validateDBPath() -> None:
    st.session_state["db_valid"] = False
    dbFilepath: str = st.session_state["db_filepath_label"]

    try:
        engine: Engine = create_engine(
            url=f'sqlite:///{st.session_state["db_filepath_label"]}',
        )
    except OperationalError:
        st.error(ERROR_DB_CONN.format(dbFilepath), icon="🚨")
        return
    except DatabaseError:
        st.error(ERROR_DB_CONN.format(dbFilepath), icon="🚨")
        return
    except sqlite3.OperationalError:
        st.error(ERROR_DB_CONN.format(dbFilepath), icon="🚨")
        return
    except sqlite3.DatabaseError:
        st.error(ERROR_DB_CONN.format(dbFilepath), icon="🚨")
        return

    try:
        conn: Connection = engine.connect()
        conn.execute(
            statement=text(text="SELECT name FROM sqlite_master WHERE type='table';")
        )
    except OperationalError:
        st.error(ERROR_DB_QUERYING.format(dbFilepath), icon="🚨")
        return
    except DatabaseError:
        st.error(ERROR_DB_QUERYING.format(dbFilepath), icon="🚨")
        return
    except sqlite3.OperationalError:
        st.error(ERROR_DB_QUERYING.format(dbFilepath), icon="🚨")
        return
    except sqlite3.DatabaseError:
        st.error(ERROR_DB_QUERYING.format(dbFilepath), icon="🚨")
        return
    else:
        st.session_state["db_valid"] = True
        st.session_state["db_conn"] = engine

    try:
        conn.close()
    except UnboundLocalError:
        return


def searchDatabase():
    doi: str | None = st.session_state["doi_search_bar"]

    if doi is None:
        st.error(
            body=ERROR_DB_QUERYING.format(st.session_state["db_filepath_label"]),
            icon="🚨",
        )
        st.session_state["doi_query_result"] = None
        return

    sqlQuery: str = f'SELECT * FROM works WHERE doi = "{doi}";'

    if doi is None:
        st.error(ERROR_DB_QUERYING.format(doi), icon="🚨")
        return

    dbConn: Engine = st.session_state["db_conn"]
    df: DataFrame = pandas.read_sql_query(
        sql=sqlQuery,
        con=dbConn,
    )

    if df.empty:
        st.warning(body="Query returned no results")
        st.session_state["doi_query_result"] = None
    else:
        st.session_state["doi_query_result"] = df


def configApp() -> None:
    st.set_page_config(
        page_title="PeaT RAT",
        page_icon="🐀",
        layout="centered",
        initial_sidebar_state="auto",
        # menu_items={
        #     'Get Help': 'https://www.extremelycoolapp.com/help',
        #     'Report a bug': "https://www.extremelycoolapp.com/bug",
        #     'About': "# This is a header. This is an *extremely* cool app!"
        # }
    )


def createSessionState() -> None:
    if "db_filepath_label" not in st.session_state:
        st.session_state["db_filepath_label"] = USER_HOME
    if "db_valid" not in st.session_state:
        st.session_state["db_valid"] = False
    if "db_conn" not in st.session_state:
        st.session_state["db_conn"] = create_engine(url="sqlite:///:memory:")
    if "doi_query" not in st.session_state:
        st.session_state["doi_query"] = "10.48550/arXiv.2404.14619"
    if "doi_query_result" not in st.session_state:
        st.session_state["doi_query_result"] = None


def main() -> None:
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
                on_click=validateDBPath,
            )

    if st.session_state["db_valid"]:
        st.divider()

        st.markdown(body="## DOI Search")
        st.markdown(body="> Search for DOIs captured within our database")
        with st.form(key="doi-search", clear_on_submit=False, border=True):
            st.text_input(
                label="DOI Search Bar",
                value=st.session_state["doi_query"],
                key="doi_search_bar",
                help='Input can be in the form of \
"https://doi.org/10.48550/arXiv.2404.14619" or "10.48550/arXiv.2404.14619"',
            )
            st.form_submit_button(
                label="Search",
                on_click=searchDatabase,
            )

        if st.session_state["doi_query_result"] is not None:
            st.dataframe(st.session_state["doi_query_result"])


if __name__ == "__main__":
    configApp()
    createSessionState()
    main()


# streamlit run main.py
# git stage -A
# git commit -m "commit text here"
# git commit -m "text --no-verify"

# source env/bin/activate
# deactivate

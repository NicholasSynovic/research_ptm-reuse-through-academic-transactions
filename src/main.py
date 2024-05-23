import sqlite3
from pathlib import Path
from typing import Literal

import streamlit as st
from sqlalchemy import Connection, Engine, create_engine, text
from sqlalchemy.exc import DatabaseError, OperationalError
from streamlit.delta_generator import DeltaGenerator

from src import APP_AUTHORS, APP_DESCRIPTION, APP_TITLE
from src.components import USER_HOME
from src.components.filepicker import tk_FilePicker


def updateFilePathInputLabel() -> None:
    filePath: Path | Literal[False] = tk_FilePicker()

    if filePath != False:
        st.session_state["db_filepath_label"] = filePath


def validateDBPath() -> None:
    st.session_state["db_valid"] = False

    errorMessage: str = f'Error connecting to {st.session_state["db_filepath_label"]}'

    try:
        engine: Engine = create_engine(
            url=f'sqlite:///{st.session_state["db_filepath_label"]}',
        )
    except OperationalError:
        st.error(errorMessage, icon="🚨")
        return
    except DatabaseError:
        st.error(errorMessage, icon="🚨")
        return
    except sqlite3.OperationalError:
        st.error(errorMessage, icon="🚨")
        return
    except sqlite3.DatabaseError:
        st.error(errorMessage, icon="🚨")
        return

    try:
        conn: Connection = engine.connect()
        conn.execute(
            statement=text(text="SELECT name FROM sqlite_master WHERE type='table';")
        )
    except OperationalError:
        st.error(errorMessage, icon="🚨")
        return
    except DatabaseError:
        st.error(errorMessage, icon="🚨")
        return
    except sqlite3.OperationalError:
        st.error(errorMessage, icon="🚨")
        return
    except sqlite3.DatabaseError:
        st.error(errorMessage, icon="🚨")
        return
    else:
        st.session_state["db_valid"] = True
        st.session_state["db_conn"] = engine

    try:
        conn.close()
    except UnboundLocalError:
        return


def createSessionState() -> None:
    if "db_filepath_label" not in st.session_state:
        st.session_state["db_filepath_label"] = USER_HOME
    if "db_valid" not in st.session_state:
        st.session_state["db_valid"] = False
    if "db_conn" not in st.session_state:
        st.session_state["db_conn"] = create_engine(url="sqlite:///:memory:")


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
                on_click=validateDBPath,
            )

    if st.session_state["db_valid"]:
        st.divider()

        st.markdown(body="## DOI Search")
        st.markdown(body="> Search for DOIs captured within our database")
        with st.form(key="doi-search", clear_on_submit=False, border=True):
            doiInput: str | None = st.text_input(
                "DOI Search Bar",
                value="10.48550/arXiv.2404.14619",
                help='Input can be in the form of \
"https://doi.org/10.48550/arXiv.2404.14619" or "10.48550/arXiv.2404.14619"',
            )
            formSubmit: bool = st.form_submit_button(label="Search")


if __name__ == "__main__":
    createSessionState()
    buildPage()


# streamlit run main.py
# git stage -A
# git commit -m "commit text here"
# git commit -m "text --no-verify"

# source env/bin/activate
# deactivate

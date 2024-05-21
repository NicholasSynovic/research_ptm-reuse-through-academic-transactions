import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import Engine, create_engine

# Solution2 for reading in db into pd df
# Goal: Read SQLite DB into Pandas DataFrame
# pathToDB: str = "sqlite:///../nature/db/feedStorage/nature.db"
# pathToDB: str = "../nature/db/feedStorage/nature.db"
# dbConn: Engine = create_engine(url=pathToDB)
# dbConn = sqlite3.Connection(database=pathToDB)
# df: pd.DataFrame = pd.read_sql_query(sql="SELECT doi FROM entries", con=dbConn)
# print(df)
# quit()


def main() -> None:
    st.title("DOI Search Engine")
    st.write(
        """
    Filter for DOI of specific PTM ~ filter DOIs @ bottom
    """
    )

    st.write("Original Dataframe:")

    # Printing initial DF
    conn = sqlite3.Connection(
        database="/Users/fran-pellegrino/Library/CloudStorage/OneDrive-LoyolaUniversityChicago/Internship-Awards/LUC USRE 2024/Code/research_ptm-reuse-through-academic-transactions/nature/db/feedStorage/nature.db"
    )
    df = pd.read_sql_query("SELECT doi FROM entries", con=conn)
    st.dataframe(df)
    conn.close()

    # reading in entire sql db as pd df
    conn = sqlite3.Connection(
        database="/Users/fran-pellegrino/Library/CloudStorage/OneDrive-LoyolaUniversityChicago/Internship-Awards/LUC USRE 2024/Code/research_ptm-reuse-through-academic-transactions/nature/db/feedStorage/nature.db"
    )
    sql_read_in = "SELECT * FROM entries"
    full_db = pd.read_sql(sql_read_in, conn)
    conn.close()

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
    main()


# streamlit run main.py
# git stage -A
# git commit -m "commit text here"

# source env/bin/activate
# deactivate

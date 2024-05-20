import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import Engine, create_engine

conn = sqlite3.Connection(
    database="/Users/fran-pellegrino/Library/CloudStorage/OneDrive-LoyolaUniversityChicago/Internship-Awards/LUC USRE 2024/Code/research_ptm-reuse-through-academic-transactions/nature/db/feedStorage/nature.db"
)
df = pd.read_sql_query("SELECT doi FROM entries", con=conn)
print(df)
conn.close()


# Goal: Read SQLite DB into Pandas DataFrame
# pathToDB: str = "sqlite:///../nature/db/feedStorage/nature.db"
# pathToDB: str = "../nature/db/feedStorage/nature.db"
# # dbConn: Engine = create_engine(url=pathToDB)
# dbConn = sqlite3.Connection(database=pathToDB)
# df: pd.DataFrame = pd.read_sql_query(sql="SELECT doi FROM entries", con=dbConn)
# print(df)
quit()


def main() -> None:
    st.title("DOI Search Engine")
    st.write(
        """
    Filter for DOI of specific PTM ~ filter DOIs @ bottom
    """
    )

    st.write("Original Dataframe:")
    st.write(df)

    columns = st.columns([2])
    with columns[0]:
        input_search = st.text_input("Search through PTM DOIs here", "")

    with columns[1]:
        submit_button = st.button("Enter")

    if submit_button:
        if input_search:
            result = df.apply(
                lambda row: row.astype(str).str.contains(input_search).any(), axis=1
            )
            filtered_df = df[result]

            if not filtered_df.empty:
                st.write("Search Results:")
                st.dataframe(filtered_df)
        else:
            st.write("No result.")
    else:
        st.write("Please enter a search and then press Enter.")


if __name__ == "__main__":
    main()

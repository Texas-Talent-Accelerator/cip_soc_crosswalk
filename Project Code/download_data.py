## the aim of this code is to download all programs from THECB
##source location = https://www.txhigheredaccountability.org/AcctPublic/InteractiveReport/AddReport
import sys
sys.path.append(r"C:\Users\cmg0530\Code Library\gis_suite")
from analysis_main import *
from export_dataframes import *
import pandas as pd


def read_in_cip_soc():
    cs = pd.read_excel(r"https://nces.ed.gov/ipeds/cipcode/Files/CIP2020_SOC2018_Crosswalk.xlsx",
                       sheet_name="CIP-SOC")
    return cs
    
#create sqlite database
def create_db(db_path=r"C:\Users\cmg0530\Projects\cip_soc_crosswalk\Databases\cipsoc.sqlite"):
    con = create_conn(db_path)
    print(f"DB at {db_path}")
    return con
    

#upload standard cip-soc to database
def upload_to_sqlite(
    crsr, con, df, table_name="test__", schema="dbo", drop=True, chunk_print_size=50
):
    """
    chunk_print_size = number of rows to print count when uploading
    """

    pd.options.mode.chained_assignment = None

    print("Writing DataFrame into sql table.")
    print(f"Table name: {schema}.{table_name}")

    # rename columns
    df.columns = [
        y.replace(" ", "_").replace(":", "").replace("(", "").replace(")", "").lower()
        for y in df.columns
    ]

    # default all to varchars
    columns = df.columns.tolist()
    format_columns = [f"{x} TEXT" for x in columns]

    # adjust formatting
    for x in columns:
        df[x] = df[x].astype(str)
        # adjust datetime
        if x in df.select_dtypes(include=["datetime64[ns, UTC]"]).columns.tolist():
            df[x] = df[x].dt.strftime("%Y-%m-%d")

    # if the drop parameter is True, then drop the existing table
    if drop == True:
        try:
            print("Trying to drop old table.")
            crsr.execute(
                f"""SELECT CASE 
                WHEN EXISTS (
                    SELECT 1 
                    FROM sqlite_master 
                    WHERE type = 'table' 
                    AND name = '{table_name}'
                        ) THEN 1 
                        ELSE 0 
                    END;""")
            con.commit()
            print("Dropped!")
        except Exception as e:
            print("No existing table to drop.")
            print(f"Exception: {e}")

        try:
            print("Creating new sql table...")
            crsr.execute(
                fr"""CREATE TABLE IF NOT EXISTS {schema}_{table_name}(
                {",".join(format_columns)});""")
            con.commit()
            print("Done.")
        except Exception as e:
            print("Could not create new table.")
            print(f"Exception: {e}")

    print(f"Writing in {len(df)} rows.")

    print("Uploading...")
    for index, row in df.reset_index().iterrows():
        try:
            if index % chunk_print_size == 0:
                print(f"{index} out of {len(df)}")
            crsr.execute(
                f"""INSERT INTO {schema}_{table_name}
            ({",".join(columns)}) values ({",".join(["?"] * len(columns))})""",
                row[1:].tolist(),
            )
        except Exception as e:
            print(f"Could not upload {index}")
            print(f"Exception: {e}")

    con.commit()
    print("Done!")


#%%
#download from database 
cs = read_in_cip_soc()
#upload into database
con = create_db()
upload_to_sqlite(con.cursor(), 
                 con, 
                 cs, 
                 table_name="cip_soc",
                 schema="test", 
                 drop=True, 
                 chunk_print_size=10)

#upload to interactive portal for editing
    #(link to interactive portal for editing)
    #function that pulls from the sqlite3

#re-download and upload into database with edits/additions
    #function that pulls from wherever this works

#download programs from thecb

#upload into database

#link programs to occupations via cip soc

#save to database

#pull from database

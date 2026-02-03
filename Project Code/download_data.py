## the aim of this code is to download all programs from THECB
##source location = https://www.txhigheredaccountability.org/AcctPublic/InteractiveReport/AddReport
import sys
sys.path.append(r"C:\Users\cmg0530\Code Library\gis_suite")
from analysis_main import *
from export_dataframes import *
import pandas as pd
import os

#%%new functions to be moved into their own file
def read_in_cip_soc() -> pd.core.frame.DataFrame:
    #this is drawn from the nces website
    cs = pd.read_excel(r"https://nces.ed.gov/ipeds/cipcode/Files/CIP2020_SOC2018_Crosswalk.xlsx",
                       sheet_name="CIP-SOC")
    return cs

def process_cip_soc(cs) -> pd.core.frame.DataFrame:
    #lets process cip to be ##.#### with trailing zeros for consistency
    #split along the decimal
    #first part of decimal - make sure its 2 digits with leading 0
    #second part of decimal - make sure its 4 digits with trailing 0
    cs["cip_join_key"] = cs['CIP2020Code'].astype(str).apply(lambda x: f"{x.split('.')[0].zfill(2)}.{x.split('.')[1].ljust(4,'0')}")
    return cs

def read_in_thecb(fp=r"C:\Users\cmg0530\Projects\cip_soc_crosswalk\Data Downloads\THECB") -> dict:
    #this is drawn from a series of downloads located here, but pulled from thecb site
    #source location = https://www.txhigheredaccountability.org/AcctPublic/InteractiveReport/AddReport
    
    #list all files
    all_files = os.listdir(fp)
    files = [y for y in all_files if y.endswith("csv")]
    
    #read into a dictionary named by filename
    f_dict = {}
    for z in files:
        try:
            print(f"reading in {z}")
            f = pd.read_csv(os.path.join(fp,z),header=1,encoding='ISO-8859-1')
            f_dict[z.split(".csv")[0]] = f
        except:
            print(f"couldn't read {z}")
    print("returning dict")
    return f_dict

def process_thecb(thecb_dict) -> pd.core.frame.DataFrame:

    #make one big dataframe
    bf = pd.DataFrame()
    for q in thecb_dict.keys():
        z = thecb_dict[q]
        z['table_name'] = q
        bf = pd.concat([bf,z])

    #process cip 
    #cip should be in format ##.#### with trailing zeros
    bf["cip_join_key"] = bf['CIPDesc'].apply(lambda x: x.split(" -")[0])
    bf["cip_join_key"] = bf['cip_join_key'].apply(lambda x: f"{x[:2]}.{x[2:6]}".ljust(7, '0'))

    #to add - processing name of institution to a geographic code (tbd which one)
    return bf  

def map_programs_to_schools(con):
    crsr = con.cursor()
    crsr.execute("DROP TABLE IF EXISTS join_program_occupations")
    crsr.execute("""CREATE TABLE 
                 join_program_occupations
                 AS 
                 select 
                    thecb.cip_join_key as cip_join_key,
                    thecb.dimyear as year,
                    thecb.levelgroupdesc as institution_type,
                    thecb.instlist as institution,
                    thecb.cipdesc as cip_description,
                    thecb.count as grad_count,
                    cip.soc2018code as soc_code,
                    cip.soc2018title as soc_title  FROM
          (SELECT * FROM ref_thecb_data) as thecb
          left join
          (SELECT * FROM ref_cip_soc_nces) as cip
          on thecb.cip_join_key = cip.cip_join_key""")
    con.commit()

#%%
#download cip soc and process 
csdf = read_in_cip_soc()
csdf = process_cip_soc(csdf)

#download thecb and process
thecb_dict = read_in_thecb()
tdf = process_thecb(thecb_dict)

#connect to database
db_path = r'C:\\Users\\cmg0530\\Projects\\cip_soc_crosswalk\\Databases\\cipsoc.sqlite'
con = create_conn(db_path)

#upload two tables cip to soc to database: cip-soc and thecb data
upload_to_sqlite(con.cursor(), 
                 con, 
                 csdf, 
                 table_name="cip_soc_nces",
                 schema="ref", 
                 drop=True, 
                 chunk_print_size=1000)

upload_to_sqlite(con.cursor(), 
                 con, 
                 tdf, 
                 table_name="thecb_data",
                 schema="ref", 
                 drop=True, 
                 chunk_print_size=1000)

#need a general function that can map education data with occupation data 
map_programs_to_schools(con)

#build out a final table that looks like:
    #program - institution - occupation
all_df = pd.read_sql("select * from join_program_occupations",con=con)
all_df_filt = all_df.query("year == '2023'")
all_df_filt.to_excel(r"C:\Users\cmg0530\Projects\cip_soc_crosswalk\Data Downloads\Sample Exports\test.xlsx")


#%%
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

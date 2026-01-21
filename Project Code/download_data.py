## the aim of this code is to download all programs from THECB
##source location = https://www.txhigheredaccountability.org/AcctPublic/InteractiveReport/AddReport

import pandas as pd


def read_in_cip_soc():
    cs = pd.read_excel(r"https://nces.ed.gov/ipeds/cipcode/Files/CIP2020_SOC2018_Crosswalk.xlsx",
                       sheet_name="CIP-SOC")
    
#create sqlite database
#upload standard cip-soc to database
#download from database 
#upload to interactive portal for editing
    #(link to interactive portal for editing)
#re-download and upload into database with edits/additions
#download programs from thecb
#upload into database
#link programs to occupations via cip soc
#save to database
#pull from database
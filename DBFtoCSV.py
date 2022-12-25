import pandas as pd
from simpledbf import Dbf5
import os
from pathlib import Path
import re

# read all the files in the folder and append them to a list

def read_files(path):
    files = []
    for file in os.listdir(path):
        if file.endswith(".dbf"):
            files.append(file)
    return files

def get_df_name(df): # get the name of the file
    name = Path(df).stem
    return name

def get_date_format(date): # get the date format
    if re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        return "%Y-%m-%d"
    elif re.match(r"^\d{2}-\d{2}-\d{4}$", date):
        return "%d-%m-%Y"
    elif re.match(r"^\d{2}/\d{2}/\d{4}$", date):
        return "%m/%d/%Y"
    elif re.match(r"^\d{4}/\d{2}/\d{2}$", date):
        return "%Y/%d/%m"
    elif re.match(r"^\d{4}\d{2}\d{2}$", date):
        return "%Y%m%d"
    elif re.match(r"^\d{2}\d{2}\d{4}$", date):
        return "%d%m%Y"
    elif re.match(r"^\d{4}/\d{2}/\d{4}$", date):
        return "%Y/%m/%d"
    elif re.match(r"D_\d{4}\d{2}\d{2}$", date):
        return "D_%Y%m%d"
    else:
        return None


def VtoH(df):
    tota = get_df_name(df)
    df = Dbf5(df).to_dataframe()
    df = df.stack().reset_index()
    df.columns = ['index','Date', tota]
    df = df.drop('index', axis=1)
    df = df.iloc[1:]
    df = df.reset_index(drop=True)
    return df

def get_date(df):
    for i in range(len(df)):
        if get_date_format(df['Date'][i]) is not None:
            df['Date'][i] = pd.to_datetime(df['Date'][i], format=get_date_format(df['Date'][i])).date()
        else:
            df = df.drop(i, axis=0)
    return df
            

all = read_files(r".\\")
for file in all:
    df = VtoH(file)
    df = get_date(df)
    df.to_csv(file[:-4]+".csv", index=False)
    print(file[:-4]+".csv")

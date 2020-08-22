#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Dict, Iterable, Optional

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from scripts import CONN_URL

LOAD_QUERY = """
select distinct u.*, ci.inn innovative from
unofficial u 
left join company_innovation ci 
on u.inn = ci.inn;
"""
hyphen_columns = [
    "proceed_2017",
    "proceed_2016",
    "proceed_2015",
    "changes_profit",
    "profit_2016",
    "profit_2015",
]
int_type_columns = [
    "inn",
    "ogrn",
    "employee_number",
    "proceed",
    "create_year",
    "proceed_2017",
    "proceed_2016",
    "proceed_2015",
    "changes_profit",
    "profit_2016",
    "profit_2015",
]
bad_int_columns = [
    "employee_number",
    "proceed",
]


def _get_data(url: str, query: str) -> pd.DataFrame:
    return pd.read_sql(sql=query, con=create_engine(url).connect())


def _change_hyphen(df: pd.DataFrame, col_list: Iterable[str]) -> pd.DataFrame:
    def change_need(x: str) -> bool:
        if x == "-":
            return True
        else:
            return False

    df = df.copy()

    for col in col_list:
        mask = df[col].apply(change_need)
        df.loc[mask, col] = None

    return df


def _change_other_bad_cols(df: pd.DataFrame, col_list: Iterable[str]) -> pd.DataFrame:
    df = df.copy()

    def remove_less(x: Optional[str]) -> Optional[str]:
        if x is not None and x.startswith("<"):
            return x[1:].strip()
        else:
            return x

    def change_interval(x: Optional[str]) -> Optional[str]:
        if x is not None and " - " in x:
            return x.split("-")[0].strip()
        else:
            return x

    for col in col_list:
        df[col] = df[col].apply(remove_less)
        df[col] = df[col].apply(change_interval)

    return df


def _type_cast(df: pd.DataFrame, col_list: Iterable[str]) -> pd.DataFrame:
    df = df.copy()

    def cast_to_int(x: Optional[str]) -> np.float64:
        if x is None or x == "Н/Д":
            return np.nan
        else:
            return float(x.replace(" ", ""))

    for col in col_list:
        df[col] = df[col].apply(cast_to_int)

    return df


def load_and_clean_data() -> pd.DataFrame:
    df = _get_data(CONN_URL, LOAD_QUERY)

    df = df.drop_duplicates(subset=["inn"])
    df["target"] = df["innovative"].apply(lambda x: int(x is not None))
    df = df.drop(columns=["innovative"])

    df["has_soc_net"] = df["soc_networks"].apply(lambda x: int(x is not None))
    df["has_website"] = df["website"].apply(lambda x: int(x is not None))
    df = df.drop(columns=["soc_networks", "website"])

    df = _type_cast(
        _change_other_bad_cols(_change_hyphen(df, hyphen_columns), bad_int_columns),
        int_type_columns,
    )

    df["age"] = 2020 - df["create_year"]
    df["has_filial"] = df["has_filial"].apply(lambda x: int(x is not None))

    return df

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine
from typing import Optional
import numpy as np

from scripts import CONN_URL


def load_official() -> pd.DataFrame:
    query = """
    select distinct o.*, ci.inn innovative from
    organisation o 
    left join company_innovation ci 
    on o.inn = ci.inn;
    """
    df = pd.read_sql(query, create_engine(CONN_URL).connect())
    df = df.drop_duplicates(subset=["inn"])
    df["target"] = df["innovative"].apply(lambda x: x is not None)
    df = df.drop(columns=["innovative"])

    df = df.loc[df["warning"].apply(lambda x: "ликвид" not in x)]

    return df

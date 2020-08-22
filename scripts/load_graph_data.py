#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from igraph import Graph
from sqlalchemy import create_engine

from scripts import CONN_URL

LOAD_EDGE_LIST_QUERY = """
select o.inn src, o2.inn dst from
organisation o 
inner join organisation o2 
on o.director_name = o2.director_name 
and o.inn > o2.inn;
"""
LOAD_NODES_ATTR = """
select ci.inn from company_innovation ci
"""


def _load_data(url: str, query: str) -> pd.DataFrame:
    return pd.read_sql(query, create_engine(url).connect())


def load_graph() -> Graph:
    edge_list = _load_data(CONN_URL, LOAD_EDGE_LIST_QUERY)
    nodes = set(edge_list["src"].unique()).union(set(edge_list["dst"].unique()))
    node_ids = dict()

    innovative = set(_load_data(CONN_URL, LOAD_NODES_ATTR)["inn"].unique())

    g = Graph()
    back_map = dict()
    for n in nodes:
        id_ = g.add_vertex()
        id_["inn"] = n
        id_["innovative"] = int(n in innovative)
        back_map[n] = id_.index

    for edge in edge_list.iterrows():
        g.add_edge(back_map[edge[1]["src"]], back_map[edge[1]["dst"]])

    return g

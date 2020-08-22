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
innovative_inn_list = [
    6163084897,
    6142019008,
    6161021690,
    6154026578,
]
innov_name_tags = [
    "нпп",
    "технологии",
    "нпо",
    "системы",
    "нтц",
    "нпф",
    "фирма",
    "инжиниринг",
    "лаборатория",
]
bad_subrubrics = [
    "Авиабилеты",
    "Гостиницы",
    "Дезинфекция, дезинсекция, дератизация",
    "Дизайн рекламы",
    "Директ-мэйл / Директ mail",
    "Косметические услуги",
    "Магазины подарков",
    "Продовольственные магазины",
    "Продукты питания оптом",
    "Продукты пчеловодства",
    "Ремонт компьютерной техники",
    "Ремонт мобильных устройств связи",
    "Спортивные клубы и секции",
    "Товары для творчества и рукоделия",
    "Услуги гравировки",
    "Установка входных групп",
    "CМИ / Средства массовой информации",
    "Системы отопления, водоснабжения, канализации",
    "Нефтепродукты, Газ, ГСМ",
    "Услуги телефонной связи",
    "Компьютерный ремонт и услуги",
    "Фотомагазины",
]
bad_rubrics = [
    "Магазин одежды и обуви",
    "Повседневные, коммунальные и ритуальные услуги",
    "Социальная помощь / Благотворительность",
    "Торговые центры, комплексы / Спецмагазины",
    "Хозяйственные товары / Канцелярия / Упаковка",
    "Юридические / Бизнес / Финансовые услуги",
    "CМИ / Средства массовой информации",
    "Юридические / Бизнес / Финансовые услуги",
]
stop_words = [
    ["табак"],
    ["табач"],
    ["алког"],
    ["автотатнспорт"],
    ["торговл"],
    ["недвиж"],
    ["реклам"],
    ["консультир"],
    ["религ"],
    ["турист"],
    ["строит"],
    ["парикмах"],
    ["самоуправлен"],
    ["общественн"],
    ["вспомогательная"],
    ["прочих вспомогательных"],
    ["архитектур"],
    ["гостин"],
    ["посредничество"],
    ["полиграфич"],
    ["бухгалтер"],
    ["кондитер"],
    ["санитарно"],
    ["издание газет"],
    ["зрелищно-развлекательная"],
    ["ресторанов и кафе с полным ресторанным обслуживанием"],
    ["издательская"],
    ["ювелирных"],
    ["кинофильм"],
    ["карьеров"],
    ["аудит"],
    ["прокуратур"],
    ["финанс"],
    ["подбору персонала"],
    ["радиовещан"],
    ["малярных и стекольных"],
    ["искусств"],
    ["музеев"],
    ["чистке и уборке"],
    ["мебел"],
    ["займ"],
    ["архив"],
    ["фотограф"],
    ["подача напитков"],
    ["клубного типа"],
    ["рисков и ущерб"],
    ["прочих персональных услуг"],
    ["судебно"],
    ["дискотек"],
    ["телефонн"],
    ["электромонтаж"],
    ["торгов", "неспец"],
    ["област", "прав"],
    ["строите", "здани"],
    ["жил", "фонд"],
    ["част" "охран"],
    ["магазин", "одежд"],
    ["салон", "красот"],
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


def _filter_data_based_on_okved(x: Optional[str]) -> bool:
    if x is None:
        return False

    x = x.lower()

    for i in stop_words:
        if _word_contain_all_array_elements(x, i):
            return False
    return True


def _word_contain_all_array_elements(word, array) -> bool:
    for i in array:
        if i not in word:
            return False
    return True


def _bad_name_filter(x: Optional[str]) -> bool:
    if x is None:
        return False

    bad_names_tags = [
        "продаж",
        "касса",
        "Донской Сувенир",
        "астерская",
        "торгов",
        "мини-маркет",
        "палата",
        "Ростовской",
        "союз",
        "красот",
        "студи",
    ]

    if x in bad_names_tags:
        return False

    return True


def _innovative_name_tag(x: Optional[str]) -> int:
    if x is None:
        return 0

    for tag in innov_name_tags:
        if tag in x:
            return 1

    return 0


def _filter_bad_subrubrics(x: Optional[str]) -> bool:
    if x is None:
        return False

    if x in bad_subrubrics:
        return False

    return True


def _filter_bad_rubrics(x: Optional[str]) -> bool:
    if x is None:
        return False

    if x in bad_rubrics:
        return False

    return True


def load_and_clean_data() -> pd.DataFrame:
    df = _get_data(CONN_URL, LOAD_QUERY)
    print("Load {:d} rows".format(df.shape[0]))

    df = df.drop_duplicates(subset=["inn"])
    print("Filter complete. New shape is {:d}.".format(df.shape[0]))
    df["target"] = df["innovative"].apply(lambda x: int(x > 0))
    print(
        "Compute target. Shape is {:d}, target sum is {:d}".format(
            df.shape[0], df["target"].sum()
        )
    )
    df = df.drop(columns=["innovative"])

    df["has_soc_net"] = df["soc_networks"].apply(lambda x: int(x is not None))
    df["has_website"] = df["website"].apply(lambda x: int(x is not None))
    df = df[df["website"].apply(lambda x: (x is not None) and ("narod.ru" not in x))]
    df = df.drop(columns=["soc_networks", "website"])
    print("Another filter complete. New shape is {:d}".format(df.shape[0]))

    df = _type_cast(
        _change_other_bad_cols(_change_hyphen(df, hyphen_columns), bad_int_columns),
        int_type_columns,
    )

    df["age"] = 2020 - df["create_year"]
    df["has_filial"] = df["has_filial"].apply(lambda x: int(x is not None))

    pure_okveds = set(filter(_filter_data_based_on_okved, df["okved_name"].unique()))
    df = df[df["okved_name"].isin(pure_okveds)]

    df["innovative_name_tag"] = df["name"].apply(_innovative_name_tag)

    for company in innovative_inn_list:
        df.loc[df["inn"] == company, "target"] = 1

    df["reg_code"] = df["ogrn"].astype("str").str.slice(3, 5)
    df = df[df["sub_rubric"].apply(_filter_bad_subrubrics)]
    df = df[df["rubric"].apply(_filter_bad_rubrics)]
    df = df[df["name"].apply(_bad_name_filter)]

    df["employee_number"] = df["employee_number"].fillna(0).apply(np.log1p)
    df["age"] = df["age"].fillna(0)

    df = df.fillna({"city": "NONE", "okved_name": "NONE", "rubric": "NONE",}).fillna(0)

    return df

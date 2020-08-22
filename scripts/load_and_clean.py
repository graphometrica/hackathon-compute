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

    if "табак" in x:
        return False
    elif "табач" in x:
        return False
    elif "алког" in x:
        return False
    elif "торгов" in x and "неспец" in x:
        return False
    elif "област" in x and "прав" in x:
        return False
    elif "строите" in x and "здани" in x:
        return False
    elif "жил" in x and "фонд" in x:
        return False
    elif "автотатнспорт" in x:
        return False
    elif "торговл" in x:
        return False
    elif "недвиж" in x:
        return False
    elif "реклам" in x:
        return False
    elif "част" and "охран" in x:
        return False
    elif "консультир" in x:
        return False
    elif "религ" in x:
        return False
    elif "турист" in x:
        return False
    elif "строит" in x:
        return False
    elif "парикмах" in x:
        return False
    elif "самоуправлен" in x:
        return False
    elif "общественн" in x:
        return False
    elif "вспомогательная" in x:
        return False
    elif "прочих вспомогательных" in x:
        return False
    elif "архитектур" in x:
        return False
    elif "гостин" in x:
        return False
    elif "посредничество" in x:
        return False
    elif "полиграфич" in x:
        return False
    elif "бухгалтер" in x:
        return False
    elif "кондитер" in x:
        return False
    elif "санитарно" in x:
        return False
    elif "издание газет" in x:
        return False
    elif "зрелищно-развлекательная" in x:
        return False
    elif "ресторанов и кафе с полным ресторанным обслуживанием" in x:
        return False
    elif "издательская" in x:
        return False
    elif "ювелирных" in x:
        return False
    elif "кинофильм" in x:
        return False
    elif "карьеров" in x:
        return False
    elif "аудит" in x:
        return False
    elif "прокуратур" in x:
        return False
    elif "финанс" in x:
        return False
    elif "подбору персонала" in x:
        return False
    elif "радиовещан" in x:
        return False
    elif "малярных и стекольных" in x:
        return False
    elif "искусств" in x:
        return False
    elif "музеев" in x:
        return False
    elif "чистке и уборке" in x:
        return False
    elif "мебел" in x:
        return False
    elif "займ" in x:
        return False
    elif "архив" in x:
        return False
    elif "фотограф" in x:
        return False
    elif "подача напитков" in x:
        return False
    elif "клубного типа" in x:
        return False
    elif "рисков и ущерб" in x:
        return False
    elif "прочих персональных услуг" in x:
        return False
    elif "судебно" in x:
        return False
    elif "дискотек" in x:
        return False
    elif "телефонн" in x:
        return False
    elif "электромонтаж" in x:
        return False
    elif "магазин" in x and "одежд" in x:
        return False
    elif "салон" in x and "красот" in x:
        return False

    else:
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

    df = df.drop_duplicates(subset=["inn"])
    df["target"] = df["innovative"].apply(lambda x: int(x is not None))
    df = df.drop(columns=["innovative"])

    df["has_soc_net"] = df["soc_networks"].apply(lambda x: int(x is not None))
    df["has_website"] = df["website"].apply(lambda x: int(x is not None))
    df = df[df["website"].apply(lambda x: (x is not None) and ("narod.ru" not in x))]
    df = df.drop(columns=["soc_networks", "website"])

    df = _type_cast(
        _change_other_bad_cols(_change_hyphen(df, hyphen_columns), bad_int_columns),
        int_type_columns,
    )

    df["age"] = 2020 - df["create_year"]
    df["has_filial"] = df["has_filial"].apply(lambda x: int(x is not None))

    pure_okveds = set(filter(_filter_data_based_on_okved, df["okved_name"].unique()))
    df = df[df["okved_name"].isin(pure_okveds)]

    df["inn"] = df["inn"].astype("int")
    df["innovative_name_tag"] = df["name"].apply(_innovative_name_tag)

    for company in innovative_inn_list:
        df.loc[df["inn"] == company, "target"] = 1

    df["reg_code"] = df["ogrn"].astype("str").str.slice(3, 5)
    df = df[df["sub_rubric"].apply(_filter_bad_subrubrics)]
    df = df[df["rubric"].apply(_filter_bad_rubrics)]
    df = df[df["name"].apply(_bad_name_filter)]

    df["employee_number"] = df["employee_number"].fillna(0).apply(np.log1p)
    df["age"] = df["age"].fillna(0)

    return df

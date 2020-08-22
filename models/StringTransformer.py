#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from typing import Dict, Iterable, Optional

import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


class StringTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, string_cols: Iterable[str]) -> None:
        self.encoders: Optional[Dict[str, Diict[str, float]]] = None
        self.string_cols = string_cols

    def fit(self, X: pd.DataFrame, y=None) -> "StringTransformer":
        self.encoders = dict()

        for col in self.string_cols:
            if col in X.columns:
                self.encoders[col] = X.groupby(col)[col].count().to_dict()
            else:
                sys.stderr.write("Warning! Col {:s} not in columns!\n".format(col))

        return self

    def is_fitted(self) -> bool:
        return self.encoders is not None

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        if not self.is_fitted():
            raise ValueError("Not fitted!")

        X = X.copy()

        for col, encoder in self.encoders.items():

            def trf(x):
                return encoder.get(x, -1000)

            if col in X.columns:
                X[col] = X[col].apply(trf)
            else:
                sys.stderr.write("Warning! Col {:s} not in columns!\n".format(col))

        return X

    def fit_transform(self, X, y=None) -> pd.DataFrame:
        return self.fit(X).transform(X)

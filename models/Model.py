#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pickle

import numpy as np
import pandas as pd
from interpret.glassbox import ExplainableBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import StratifiedKFold

from scripts import load_and_clean_data

train_cols = [
    "proceed",
    "city",
    "okved_name",
    "has_soc_net",
    "has_website",
    "age",
    "has_filial",
    "rubric",
    "employee_number",
    "innovative_name_tag",
    "reg_code",
]


def run_training_process():
    df = load_and_clean_data()
    X = df[train_cols].reset_index(drop=True)
    y = df["target"].to_numpy()

    clf = ExplainableBoostingClassifier()

    for tr, tst in StratifiedKFold(n_splits=3).split(X, y):
        print(
            "Shape of train data: {:d}\nShape of test data: {:d}\n".format(
                len(tr), len(tst)
            )
        )
        print(
            "Sum of labels in train: {:d}\nSum of labels in test: {:d}".format(
                y[tr].sum(), y[tst].sum()
            )
        )

        clf.fit(X.loc[tr], y[tr])
        print(
            "ROC AUC Score: {:4f}".format(
                roc_auc_score(y[tst], clf.predict_proba(X.loc[tst])[:, 1])
            )
        )

    clf.fit(X, y)

    with open("model_file", "bw") as file:
        pickle.dump(clf, file)

    df.to_csv("features_file.csv", index=False)
    df["preds"] = clf.predict_proba(X)[:, 1]

    df[["inn", "preds", "target",]].to_csv("score.csv", index=False)

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pickle
import sys
from pathlib import Path

import pandas as pd
from flask import app as flask_app, render_template, request, abort
from interpret import preserve


App = flask_app.Flask(__name__)
root_path = Path(__file__).parent.parent


if str(root_path.absolute()) not in sys.path:
    sys.path.append(str(root_path.absolute()))

from models import train_cols


data = pd.read_csv(str(root_path.joinpath("features_file.csv").absolute()))
with root_path.joinpath("model_file").open("br") as file:
    model = pickle.load(file)


@App.route("/explain/")
def get_explanation():
    inn = int(request.args.get("inn"))

    try:
        explanation = model.explain_local(data.loc[data["inn"] == inn, train_cols])
        return explanation.visualize(0).to_html()
    except:
        abort(404)


if __name__ == "__main__":
    App.run()

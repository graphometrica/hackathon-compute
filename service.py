#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pickle
import sys

from interpret import show_link

if __name__ == "__main__":
    with open(sys.argv[1], "rb") as file:
        model = pickle.load(file)

    explanation = model.explain_global()
    print(show_link(explanation))

    while True:
        pass

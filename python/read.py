
m __future__ import division
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import NullFormatter
import scipy
import collections


def classify(edits, users):
    return classify_pop(edits) + classify_ratio(edits, users)


def classify_ratio(edits, users):
    ratio = users / edits
    if ratio > 0.90:  # few edits per user (many users, few edits)
        return "A"
    if ratio > 0.8:
        return "B"
    if ratio > 0.7:
        return "C"
    if ratio > 0.6:
        return "D"
    if ratio > 0.5:
        return "E"
    return "F"      # many edits per user (many edits, few users)


def classify_pop(edits):
    if edits > 1000:
        return "A"
    if edits > 100:
        return "B"
    if edits > 10:
        return "C"
    return "D"


def read_row(row):
    return [int(n) for n in row.split(',')]


chart = {}
x = []
y = []

with open('res/data', 'r') as data:
    for row in data:
        row = read_row(row)
        edits = row[1]
        users = row[2]
        x.append(edits)
        y.append(users/edits)

        data_class = classify(edits, users)
        if data_class not in chart:
            chart[data_class] = 0
        chart[data_class] += 1
od = collections.OrderedDict(sorted(chart.items()))
print od

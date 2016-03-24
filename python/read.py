from __future__ import division
import punch

def classify(edits, users):
    return classify_pop(edits) + classify_ratio(edits, users)


def classify_ratio(edits, users):
    ratio = users / edits
    if ratio > 0.90:  # few edits per user (many users, few edits)
        return "F"
    if ratio > 0.8:
        return "E"
    if ratio > 0.7:
        return "D"
    if ratio > 0.6:
        return "C"
    if ratio > 0.5:
        return "B"
    return "A"  # many edits per user (many edits, few users)


def classify_pop(edits):
    if edits >= 1000:
        return "G"
    if edits > 500:
        return "F"
    if edits > 100:
        return "E"
    if edits > 50:
        return "D"
    if edits > 20:
        return "C"
    if edits > 10:
        return "B"
    return "A"


def read_row(row):
    return [int(n) for n in row.split(',')]


def categorize_data(url):
    chart = {}

    with open(url, 'r') as data:
        for row in data:
            row = read_row(row)
            edits = row[1]
            users = row[2]

            data_class = classify(edits, users)
            if data_class not in chart:
                chart[data_class] = 0
            chart[data_class] += 1
    return chart


def to_key(i):
    return ord(i[0]) - 65, ord(i[1]) - 65

def create_punch_data(chart):
    key_values = {}
    for i, j in chart.iteritems():
        # print to_key(i), j
        key_values[to_key(i)] = j

    return key_values

languages = ['it', 'es', 'fr', 'de']
for lang in languages:
    url = '../processed_data/%s-article-edits-users' % lang
    chart = categorize_data(url)
    key_values = create_punch_data(chart)
    punch.draw_punchcard(key_values, '%s wiki' % lang)

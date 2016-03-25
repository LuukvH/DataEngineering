import matplotlib.pyplot as plt
import numpy as np

def read_row(row):
    return [int(n) for n in row.split(',')]


MAX = 500

def categorize_data(url):
    article_per_edits = {}

    with open(url, 'r') as data:
        for row in data:
            row = read_row(row)
            edits = row[1]
            if edits < MAX:
                if edits not in article_per_edits:
                    article_per_edits[edits] = 0
                article_per_edits[edits] += 1
    return article_per_edits

languages = ['it', 'es', 'fr', 'de']
for lang in languages:
    url = '../processed_data/%s-article-edits-users' % lang
    data = categorize_data(url)
    x = np.array(data.keys())
    y = np.array(data.values())
    fig, ax = plt.subplots()
    ax.set_xlim([0, 80])
    ax.plot(x, y)
    plt.gcf().canvas.set_window_title(lang)
    plt.show()
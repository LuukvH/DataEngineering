from __future__ import division
import matplotlib.pyplot as plt
import numpy as np


def draw_punchcard(infos, title, ax1=range(7), ax2=range(24),
                   ax1_ticks=['0-10', '11-20', '21-50', '51-100', '101-500', '501-1000', '>1000'],
                   ax2_ticks=['0-0.5', '0.5-0.6', '0.6-0.7', '0.7-0.8', '0.8-0.9', '>=0.9'],
                   ax1_label='users/edits', ax2_label='Amount'):
    # build the array which contains the values
    data = np.zeros((len(ax1), len(ax2)))
    for key in infos:
        data[key[0], key[1]] = infos[key]
    data = data / float(np.max(data))

    # shape ratio
    r = float(data.shape[1]) / data.shape[0]

    # Draw the punchcard (create one circle per element)
    # Ugly normalisation allows to obtain perfect circles instead of ovals....
    for y in range(data.shape[0]):
        for x in range(data.shape[1]):
            circle = plt.Circle((x / float(data.shape[1]) * data.shape[0], y / r),
                                data[y][x] / float(data.shape[1]) * data.shape[0] / 2)
            plt.gca().add_artist(circle)

    plt.ylim(0 - 0.5, data.shape[0] - 0.5)
    plt.xlim(0, data.shape[0])
    plt.yticks(np.arange(0, len(ax1) / r - .1, 1 / r), ax1_ticks)
    plt.xticks(np.linspace(0, len(ax1), len(ax2)) + 0.5 / float(data.shape[1]), ax2_ticks)
    plt.xlabel(ax1_label)
    plt.ylabel(ax2_label)

    # make sure the axes are equal, and resize the canvas to fit the plot
    plt.axis('equal')
    plt.axis([-.5, 2, -3.5, 5])
    # scale = 0.5
    # plt.gcf().set_size_inches(data.shape[1] * scale, data.shape[0] * scale, forward=True)
    plt.gcf().canvas.set_window_title(title)
    plt.show()

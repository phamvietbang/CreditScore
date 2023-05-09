import matplotlib.pyplot as plt
import numpy as np
from backtest.charts.decorators import Decorator
import seaborn as sns


class ChartDrawer:
    @staticmethod
    def draw_pie(recipe, data, label, file='pie.png'):
        fig, ax = plt.subplots(figsize=(10, 5), subplot_kw=dict(aspect="equal"))
        wedges, texts, _ = ax.pie(data, startangle=-40, autopct=lambda pct: Decorator.func(pct, data),
                                  pctdistance=1.1, wedgeprops=dict(width=0.5), textprops=dict(color="r"))

        bbox_props = dict(boxstyle="square,pad=0.3", fc="w", ec="k", lw=0.72)
        kw = dict(arrowprops=dict(arrowstyle="-"),
                  bbox=bbox_props, zorder=0, va="center")

        for i, p in enumerate(wedges):
            ang = (p.theta2 - p.theta1) / 2. + p.theta1
            y = np.sin(np.deg2rad(ang))
            x = np.cos(np.deg2rad(ang))
            horizontal_alignment = {-1: "right", 1: "left"}[int(np.sign(x))]
            connection_style = f"angle,angleA=0,angleB={ang}"
            kw["arrowprops"].update({"connectionstyle": connection_style})
            ax.annotate(recipe[i], xy=(x, y), xytext=(1.35 * np.sign(x), 1.4 * y),
                        horizontalalignment=horizontal_alignment, **kw)

        ax.set_title(label, y=-0.250)

        plt.savefig(file)

    @staticmethod
    def draw_bar(species, counts, title, x_label, y_label, width=0.6, file='bar.png'):
        fig, ax = plt.subplots(figsize=(20, 10))
        bottom = np.zeros(7)

        for sex, count in counts.items():
            p = ax.bar(species, count, width, label=sex, bottom=bottom)
            bottom += count

            ax.bar_label(p, label_type='center')

        ax.set_title(title, y=-0.15)
        ax.set_xlabel(x_label)
        ax.set_ylabel(y_label)
        ax.legend(bbox_to_anchor=(1, 1.1), ncols=5)

        plt.savefig(file)

    @staticmethod
    def draw_boxenplot(data_frame, title, x_label, y_label, order=None, color='#D3756B', file='boxenplot.png'):
        fig = sns.boxenplot(data=data_frame, x=x_label, y=y_label, order=order, color=color)
        fig.set_title(title)
        fig = fig.get_figure()

        fig.savefig(file)

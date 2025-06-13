import numpy as np
import matplotlib.pyplot as plt

class LineChart:
    def plot(self, x: list[int | float], title: str = 'Line Chart', show_block: bool = True):
        plt.plot(x, label='x')
        plt.title(title)
        plt.legend()
        plt.show(block=show_block)

if __name__ == "__main__":
    x = [1, 3, 2, 5, 4]
    chart = LineChart()
    chart.plot(x)
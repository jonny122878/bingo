import numpy as np
import matplotlib.pyplot as plt

class LineChart:
    def plot(self, x: list[int | float]):
        plt.plot(x, label='x')
        plt.title('Line Chart')
        plt.legend()
        plt.show()

if __name__ == "__main__":
    x = [1, 3, 2, 5, 4]
    chart = LineChart()
    chart.plot(x)
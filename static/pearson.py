import numpy as np
import matplotlib.pyplot as plt

x = [1, 2, 3, 4, 5]
y = [2, 4, 6, 8, 10]
corr = np.corrcoef(x, y)[0, 1]

plt.plot(x, label='x')
# plt.plot(y, label='y')
plt.title(f'Line Chart (Pearson r={corr:.2f})')
plt.legend()
plt.show()
import numpy as np
from scipy import stats

# 示例数据
# data1 = [2.3, 2.5, 2.8, 3.0, 3.2, 3.5, 3.7, 3.8, 4.0, 4.2]
# data2 = [2.1, 2.4, 2.6, 2.9, 3.1, 3.3, 3.6, 3.9, 4.1, 4.3]
# data1 = [1, 2, 3, 4, 5, 6, 7, 8]
# data2 = [1, 2,  3, 4, 5, 6, 7, 8]
data1 = [1, 2, 3]
data2 = [1, 2, 3]

# 计算皮尔逊相关系数
correlation_coefficient, p_value = stats.pearsonr(data1, data2)

print(f"Pearson correlation coefficient: {correlation_coefficient}")
print(f"P-value: {p_value}")

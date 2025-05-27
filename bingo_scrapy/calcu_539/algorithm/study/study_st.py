# 單樣本t檢定


import numpy as np
from scipy import stats

# 示例数据
data = [2.3, 2.5, 2.8, 3.0, 3.2, 3.5, 3.7, 3.8, 4.0, 4.2]

# 假设总体均值为3.0
population_mean = 3.0

# 进行单样本t检定
t_statistic, p_value = stats.ttest_1samp(data, population_mean)

print(f"T-statistic: {t_statistic}")
print(f"P-value: {p_value}")


# 雙樣本t檢定

# 示例数据
data1 = [2.3, 2.5, 2.8, 3.0, 3.2, 3.5, 3.7, 3.8, 4.0, 4.2]
data2 = [2.1, 2.4, 2.6, 2.9, 3.1, 3.3, 3.6, 3.9, 4.1, 4.3]

# 进行双样本t检定
t_statistic, p_value = stats.ttest_ind(data1, data2)

print(f"T-statistic: {t_statistic}")
print(f"P-value: {p_value}")

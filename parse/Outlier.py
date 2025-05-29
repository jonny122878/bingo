import numpy as np

class Outlier:
    def calcu(self, data: list) -> list:
        Q1 = np.percentile(data, 25)
        Q3 = np.percentile(data, 75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        outliers = [x for x in data if x < lower_bound or x > upper_bound]
        return outliers
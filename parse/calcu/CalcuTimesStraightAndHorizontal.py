from pandas import DataFrame
import pandas as pd
import numpy as np
from parse.ball_mark.IBallMark import IBallMark
from collections import Counter

class CalcuTimesStraightAndHorizontal:
    def __init__(self) -> None:
        self._dfMerged = None
        self._dfSorted = None
        self._dfSortedVariant = None
        self._dfDictUniqueAll = None
        self._dfDictUniqueGroup = None

    @property
    def dfMerged(self):
        return self._dfMerged

    @dfMerged.setter
    def dfMerged(self, value):
        self._dfMerged = value

    @property
    def dfSorted(self):
        return self._dfSorted

    @dfSorted.setter
    def dfSorted(self, value):
        self._dfSorted = value

    @property
    def dfSortedVariant(self):
        return self._dfSortedVariant

    @dfSortedVariant.setter
    def dfSortedVariant(self, value):
        self._dfSortedVariant = value

    @property
    def dfDictUniqueAll(self):
        return self._dfDictUniqueAll

    @dfDictUniqueAll.setter
    def dfDictUniqueAll(self, value):
        self._dfDictUniqueAll = value

    @property
    def dfDictUniqueGroup(self):
        return self._dfDictUniqueGroup

    @dfDictUniqueGroup.setter
    def dfDictUniqueGroup(self, value):
        self._dfDictUniqueGroup = value

    def Calcu(self, dfStraight: DataFrame, dfHorizontal: DataFrame, Horizontal: IBallMark, Straight: IBallMark) -> None:
        # merge DataFrame
        dfMerged = pd.concat([dfStraight, dfHorizontal], axis=1)
        self.dfMerged = dfMerged
        # 橫向遍歷，整併 dfSortedVariant foreach 到主 for 迴圈，產生 KeyN, SortedN, BallsN 共 9 欄
        sorted_dict = {}
        variant_dict = {}
        dfDictUnique_list = []
        for i in range(dfMerged.shape[0]):
            row_np = dfMerged.iloc[i].to_numpy()
            sorted_indices = np.argsort(row_np)
            sorted_keys = dfMerged.columns[sorted_indices]
            sorted_values = np.sort(row_np)
            sorted_dict[f'Row {i+1}'] = sorted_keys
            sorted_dict[f'Sorted{i+1}'] = sorted_values
            # variant: 跳過前5個元素後取8個元素
            variant_indices = list(range(5, 13))
            variant_values = sorted_values[5:13]
            # 從 variant_values index 反推 column name sorted_keys
            variant_keys = [sorted_keys[idx] for idx in range(5, 13)]
            variant_balls = []
            for k in variant_keys:
                balls = Horizontal.markToBalls(str(k))
                if not balls:
                    balls = Straight.markToBalls(str(k))
                variant_balls.append(balls)
            variant_dict[f'Key{i+1}'] = variant_keys
            variant_dict[f'Sorted{i+1}'] = variant_values
            variant_dict[f'Balls{i+1}'] = variant_balls
            # 每一列都計算 dictUnique 並存入 list
            dictUnique = {}
            for balls in variant_balls:
                for b in balls:
                    dictUnique[b] = dictUnique.get(b, 0) + 1
            dfDictUnique = pd.DataFrame(list(dictUnique.items()), columns=["Ball", "Count"])
            dfDictUnique = dfDictUnique[dfDictUnique["Count"] > 1]  # 移除 Count == 1
            dfDictUnique["RowIndex"] = i+1
            dfDictUnique_list.append(dfDictUnique)
        dfSorted = pd.DataFrame(sorted_dict)
        self.dfSorted = dfSorted
        dfSortedVariant = pd.DataFrame(variant_dict)
        self.dfSortedVariant = dfSortedVariant
        # 合併所有 rows 的 dfDictUnique
        if dfDictUnique_list:
            dfDictUniqueAll = pd.concat(dfDictUnique_list, ignore_index=True)
        else:
            dfDictUniqueAll = pd.DataFrame()
        self.dfDictUniqueAll = dfDictUniqueAll
        # 新增：依 RowIndex group by，將 Ball 合併成 array
        if not dfDictUniqueAll.empty:
            dfDictUniqueGroup = dfDictUniqueAll.groupby('RowIndex')['Ball'].apply(list).reset_index()
            dfDictUniqueGroup.rename(columns={'Ball': 'Balls'}, inplace=True)
        else:
            dfDictUniqueGroup = pd.DataFrame()
        self.dfDictUniqueGroup = dfDictUniqueGroup
        # 輸出到 Excel
        with pd.ExcelWriter(r"C:\Programs\test_data\CalcuTimesStraightAndHorizontal.xlsx") as writer:
            self.dfMerged.to_excel(writer, sheet_name="Merged", index=False)
            self.dfSorted.to_excel(writer, sheet_name="SortedFirstRow", index=False)
            self.dfSortedVariant.to_excel(writer, sheet_name="SortedFirstRowVariant", index=False)
            self.dfDictUniqueAll.to_excel(writer, sheet_name="ElementCountAllRows", index=False)
            self.dfDictUniqueGroup.to_excel(writer, sheet_name="ElementCountGroupByRow", index=False)
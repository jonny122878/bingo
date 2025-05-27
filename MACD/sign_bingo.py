from typing import List


class SingleBingoMatch:
    def __init__(self, Balls: List[str], MatchBalls: List[str], Profit: int):
        self._Balls = Balls
        self._MatchBalls = MatchBalls
        self._Profit = Profit

    @property
    def Balls(self):
        return self._Balls

    @Balls.setter
    def Balls(self, value: List[str]):
        self._Balls = value

    @property
    def MatchBalls(self):
        return self._MatchBalls

    @MatchBalls.setter
    def MatchBalls(self, value: List[str]):
        self._MatchBalls = value

    @property
    def Profit(self):
        return self._Profit

    @Profit.setter
    def Profit(self, value: int):
        self._Profit = value

    def __eq__(self, other):
        if not isinstance(other, SingleBingoMatch):
            return False
        return (
            sorted(self.Balls) == sorted(other.Balls) and
            sorted(self.MatchBalls) == sorted(other.MatchBalls) and
            self.Profit == other.Profit
        )


class BatchBingoMatch:
    def __init__(self, Results: List['SingleBingoMatch'], SumProfit: int):
        self._Results = Results
        self._SumProfit = SumProfit

    @property
    def Results(self):
        return self._Results

    @Results.setter
    def Results(self, value: List['SingleBingoMatch']):
        self._Results = value

    @property
    def SumProfit(self):
        return self._SumProfit

    @SumProfit.setter
    def SumProfit(self, value: int):
        self._SumProfit = value

    def __eq__(self, other):
        if not isinstance(other, BatchBingoMatch):
            return False
        return self.SumProfit == other.SumProfit


class BingoSignal:
    def __init__(self):
        self._Stds = ['01', '02', '03', '04',
                      '05', '06', '07', '08', '09', '10']
        pass


def SignBatch(self, inputs: List[List[str]]) -> BatchBingoMatch:
    batchBingoMatch = BatchBingoMatch()
    for input_set in inputs:
        singleBingoMatch = self._SignSingle(input_set)
        batchBingoMatch.Results.append(singleBingoMatch)
    pass  # Placeholder for actual implementation


def _SignSingle(self, input_set: List[str]) -> SingleBingoMatch:
    input_set = ['01', '03']
    matchBalls = [item for item in self._Stds if item in input_set]
    if len(matchBalls) == 3:
        return SingleBingoMatch(Balls=input_set, MatchBalls=matchBalls, Profit=1000)
    elif len(matchBalls) == 2:
        return SingleBingoMatch(Balls=input_set, MatchBalls=matchBalls, Profit=50)
    else:
        return SingleBingoMatch(Balls=input_set, MatchBalls=matchBalls, Profit=0)


if __name__ == '__main__':
    bingoSignal = BingoSignal()
    inputs = [['01', '02', '03'], ['04', '05', '06']]
    bingoSignal.SignBatch(inputs)
    pass

from typing import List
from xmlrpc.client import boolean


class BingoModel:
    """
    Bingo table representation
    """

    def __init__(self, drawTerm: int = 0, dDate: str = '', bigShowOrder: str = '', createDate: str = ''):
        self.drawTerm = drawTerm
        self.dDate = dDate
        self.bigShowOrder = bigShowOrder
        self.createDate = createDate
        """衍生"""
        self.bigShowOrders: List[int] = []
        self.strBigShowOrders: List[int] = []
        self.bigQty: int = 0
        self.smallQty: int = 0
        self.isBig: bool = False
        self.isSmall: bool = False

    def __eq__(self, other):
        if not isinstance(other, BingoModel):
            return False
        return (self.drawTerm == other.drawTerm and
                self.dDate == other.dDate and
                self.bigShowOrder == other.bigShowOrder and
                self.createDate == other.createDate)

    def __hash__(self) -> int:
        return hash((self.drawTerm, self.dDate, self.bigShowOrder, self.createDate))

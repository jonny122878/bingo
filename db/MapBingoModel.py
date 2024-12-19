from typing import Dict
from db.BingoModel import BingoModel


class MapBingoModel:
    def __init__(self):
        pass

    def dict_to_bingo_model(self, data: dict) -> BingoModel:
        return BingoModel(
            drawTerm=data.get('drawTerm', 0),
            dDate=data.get('dDate', ''),
            bigShowOrder=data.get('bigShowOrder', ''),
            createDate=data.get('createDate', '')
        )

    def set_big_show_orders_list(self, bingo_model: BingoModel) -> BingoModel:
        bingo_model.bigShowOrders = list(
            map(int, bingo_model.bigShowOrder.split(',')))
        return bingo_model

    def set_big_samll_qty(self, bingo_model: BingoModel) -> BingoModel:
        bingo_model.bigQty = sum(
            1 for order in bingo_model.bigShowOrders if 41 <= order <= 80)
        bingo_model.smallQty = 20 - bingo_model.bigQty
        return bingo_model

    def qty_big_small_mark(self, bingo_model: BingoModel) -> BingoModel:
        if bingo_model.bigQty == 13:
            bingo_model.isBig = True
        if bingo_model.smallQty == 13:
            bingo_model.isSmall = True
        return bingo_model
    pass

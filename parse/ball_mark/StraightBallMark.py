from IBallMark import IBallMark
from BallGroup import BallGroup

class StraightBallMark(IBallMark):
    """
    直向條為一個群組，例如: 1~71、2~72
    """

    def loadStds(self) -> list[str]:
        return [
            BallGroup('01', 0),
            BallGroup('02', 0),
            BallGroup('03', 0),
            BallGroup('04', 0),
            BallGroup('05', 0),
            BallGroup('06', 0),
            BallGroup('07', 0),
            BallGroup('08', 0),
            BallGroup('09', 0),
            BallGroup('10', 0),
        ]

    def ballToMark(self, ball: str) -> str:
        if ball in ['01', '11', '21', '31', '41', '51', '61', '71']:
            return '01'
        elif ball in ['02', '12', '22', '32', '42', '52', '62', '72']:
            return '02'
        elif ball in ['03', '13', '23', '33', '43', '53', '63', '73']:
            return '03'
        elif ball in ['04', '14', '24', '34', '44', '54', '64', '74']:
            return '04'
        elif ball in ['05', '15', '25', '35', '45', '55', '65', '75']:
            return '05'
        elif ball in ['06', '16', '26', '36', '46', '56', '66', '76']:
            return '06'
        elif ball in ['07', '17', '27', '37', '47', '57', '67', '77']:
            return '07'
        elif ball in ['08', '18', '28', '38', '48', '58', '68', '78']:
            return '08'
        elif ball in ['09', '19', '29', '39', '49', '59', '69', '79']:
            return '09'
        elif ball in ['10', '20', '30', '40', '50', '60', '70', '80']:
            return '10'
        return ""

    def markToBalls(self, mark: str) -> list[str]:
        if mark == '01':
            return ['01', '11', '21', '31', '41', '51', '61', '71']
        elif mark == '02':
            return ['02', '12', '22', '32', '42', '52', '62', '72']
        elif mark == '03':
            return ['03', '13', '23', '33', '43', '53', '63', '73']
        elif mark == '04':
            return ['04', '14', '24', '34', '44', '54', '64', '74']
        elif mark == '05':
            return ['05', '15', '25', '35', '45', '55', '65', '75']
        elif mark == '06':
            return ['06', '16', '26', '36', '46', '56', '66', '76']
        elif mark == '07':
            return ['07', '17', '27', '37', '47', '57', '67', '77']
        elif mark == '08':
            return ['08', '18', '28', '38', '48', '58', '68', '78']
        elif mark == '09':
            return ['09', '19', '29', '39', '49', '59', '69', '79']
        elif mark == '10':
            return ['10', '20', '30', '40', '50', '60', '70', '80']
        return []

from .IBallMark import IBallMark

class StraightBallMark(IBallMark):
    """
    直向條為一個群組，例如: 1~71、2~72
    """

    def loadStds(self) -> dict[str, int]:
        return {
            '01S': 0,
            '02S': 0,
            '03S': 0,
            '04S': 0,
            '05S': 0,
            '06S': 0,
            '07S': 0,
            '08S': 0,
            '09S': 0,
            '10S': 0,
        }

    def ballToMark(self, ball: str) -> str:
        if ball in ['01', '11', '21', '31', '41', '51', '61', '71']:
            return '01S'
        elif ball in ['02', '12', '22', '32', '42', '52', '62', '72']:
            return '02S'
        elif ball in ['03', '13', '23', '33', '43', '53', '63', '73']:
            return '03S'
        elif ball in ['04', '14', '24', '34', '44', '54', '64', '74']:
            return '04S'
        elif ball in ['05', '15', '25', '35', '45', '55', '65', '75']:
            return '05S'
        elif ball in ['06', '16', '26', '36', '46', '56', '66', '76']:
            return '06S'
        elif ball in ['07', '17', '27', '37', '47', '57', '67', '77']:
            return '07S'
        elif ball in ['08', '18', '28', '38', '48', '58', '68', '78']:
            return '08S'
        elif ball in ['09', '19', '29', '39', '49', '59', '69', '79']:
            return '09S'
        elif ball in ['10', '20', '30', '40', '50', '60', '70', '80']:
            return '10S'
        return ""

    def markToBalls(self, mark: str) -> list[str]:
        if mark == '01S':
            return ['01', '11', '21', '31', '41', '51', '61', '71']
        elif mark == '02S':
            return ['02', '12', '22', '32', '42', '52', '62', '72']
        elif mark == '03S':
            return ['03', '13', '23', '33', '43', '53', '63', '73']
        elif mark == '04S':
            return ['04', '14', '24', '34', '44', '54', '64', '74']
        elif mark == '05S':
            return ['05', '15', '25', '35', '45', '55', '65', '75']
        elif mark == '06S':
            return ['06', '16', '26', '36', '46', '56', '66', '76']
        elif mark == '07S':
            return ['07', '17', '27', '37', '47', '57', '67', '77']
        elif mark == '08S':
            return ['08', '18', '28', '38', '48', '58', '68', '78']
        elif mark == '09S':
            return ['09', '19', '29', '39', '49', '59', '69', '79']
        elif mark == '10S':
            return ['10', '20', '30', '40', '50', '60', '70', '80']
        return []

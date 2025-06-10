from IBallMark import IBallMark
from BallGroup import BallGroup

class HorizontalBallMark(IBallMark):
    """
    橫向條為一個群組，例如: 01~09、11~19
    """

    def loadStds(self) -> list[str]:
        return [
            BallGroup('01', 0),
            BallGroup('11', 0),
            BallGroup('21', 0),
            BallGroup('31', 0),
            BallGroup('41', 0),
            BallGroup('51', 0),
            BallGroup('61', 0),
            BallGroup('71', 0),
        ]

    def ballToMark(self, ball: str) -> str:
        if ball in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10']:
            return '01'
        elif ball in ['11', '12', '13', '14', '15', '16', '17', '18', '19', '20']:
            return '11'
        elif ball in ['21', '22', '23', '24', '25', '26', '27', '28', '29', '30']:
            return '21'
        elif ball in ['31', '32', '33', '34', '35', '36', '37', '38', '39', '40']:
            return '31'
        elif ball in ['41', '42', '43', '44', '45', '46', '47', '48', '49', '50']:
            return '41'
        elif ball in ['51', '52', '53', '54', '55', '56', '57', '58', '59', '60']:
            return '51'
        elif ball in ['61', '62', '63', '64', '65', '66', '67', '68', '69', '70']:
            return '61'
        elif ball in ['71', '72', '73', '74', '75', '76', '77', '78', '79', '80']:
            return '71'
        return ""

    def markToBalls(self, mark: str) -> list[str]:
        if mark == '01':
            return ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10']
        elif mark == '11':
            return ['11', '12', '13', '14', '15', '16', '17', '18', '19', '20']
        elif mark == '21':
            return ['21', '22', '23', '24', '25', '26', '27', '28', '29', '30']
        elif mark == '31':
            return ['31', '32', '33', '34', '35', '36', '37', '38', '39', '40']
        elif mark == '41':
            return ['41', '42', '43', '44', '45', '46', '47', '48', '49', '50']
        elif mark == '51':
            return ['51', '52', '53', '54', '55', '56', '57', '58', '59', '60']
        elif mark == '61':
            return ['61', '62', '63', '64', '65', '66', '67', '68', '69', '70']
        elif mark == '71':
            return ['71', '72', '73', '74', '75', '76', '77', '78', '79', '80']
        return []

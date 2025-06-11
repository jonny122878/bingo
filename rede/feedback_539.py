import sys
import unittest
from unittest.mock import Mock
import pandas as pd
import random
from dotenv import load_dotenv
import os
import logging
import pdb
from datetime import datetime

# 動態組合路徑
if getattr(sys, 'frozen', False):
    base_dir = sys._MEIPASS
else:
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(base_dir)
from calcu_539.prize.match import FiveThreeNineMatch
from db.db import MSSQLDbContext

# Configure logging
log_filename = datetime.now().strftime('feedback_539_%Y%m%d_%H%M%S.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_filename,  # 指定 log 文件名稱
    filemode='a'  # 可選：以附加模式寫入
)

class FeedBack539:
    """
    """
    def __init__(self, period: int, times: int, dbContext: MSSQLDbContext, fiveThreeNineMatch: FiveThreeNineMatch) -> None:
        """
        設定參數:period測試期間,times測試次數,注入和取得資料庫溝通的物件,注入計算539是否中獎物件
        """
        self._period = period
        self._times = times
        self._dbContext = dbContext
        self._fiveThreeNineMatch = fiveThreeNineMatch
        logging.info(f'Initialized FeedBack539 with period={period}, times={times}')

    def calcu(self) -> pd.DataFrame:
        """
        大致流程:
        先從db將539資料給取出來
        loop times測試次數
        開頭期數用random (要做防呆,開頭期數+測試期間,不能超出總期數否則重新random)
        用開頭期數+測試區間取出要測試期數
        sign 簽注號碼暫時先寫死留接口 每期2維array,內有2 array 1~39亂數 [['01','02','03','04','05'],['11','12','13','14','15']]
        用簽注號碼和取出期數用現有期數去計算獲利FiveThreeNineMatch物件可計算結果
        return DataColumn 期數:List[str],簽注號碼:List[List[str]],計算結果:MatchInfo,profit:int,參數資訊:List[str]
        """
        logging.info('Starting calculation')
        # 先從db將539資料給取出來
        query = os.getenv("DB_QUERY")
        if not query:
            raise ValueError("DB_QUERY environment variable is not set. Please check your .env file.")
        data = self._dbContext.select(query)
        data = [item['period'] for item in data]
        logging.info(f'Retrieved {len(data)} periods from database')
        results = []
        for i in range(self._times):
            # 開頭期數用random (要做防呆,開頭期數+測試期間,不能超出總期數否則重新random)
            while True:
                start_period = random.randint(0, len(data) - self._period)
                if start_period + self._period <= len(data):
                    break
            logging.info(f'Iteration {i+1}: Selected start period {start_period}')
            # 用開頭期數+測試區間取出要測試期數
            test_periods = data[start_period:start_period + self._period]
            logging.info(f'Iteration {i+1}: Test periods {test_periods}')
            # 簽注號碼暫時先寫死留接口 每期2維array,內有2 array 1~39亂數 [['01', '02', '03', '04', '05'],['11', '12', '13', '14', '15']]
            sign_numbers = [['01', '02', '03', '04', '05'], ['11', '12', '13', '14', '15']]
            logging.info(f'Iteration {i+1}: Sign numbers {sign_numbers}')
            # 用簽注號碼和取出期數用現有期數去計算獲利FiveThreeNineMatch物件可計算結果
            # match(self, inputs: List[str], sign2Ds: List[List[str]]) -> MatchInfo:
            match_info = self._fiveThreeNineMatch.match(test_periods, sign_numbers)
            # 計算profit
            profit = match_info.profit
            logging.info(f'Iteration {i+1}: Calculated profit {profit}')
            # 收集結果
            results.append({
                '期數': test_periods,
                '簽注號碼': sign_numbers,
                'profit': profit,
                '參數資訊': [self._period, self._times]
            })
        # return DataColumn 期數:List[str],簽注號碼:List[List[str]],計算結果:MatchInfo,profit:int,參數資訊:List[str]
        logging.info('Calculation completed')
        return pd.DataFrame(results)

class TestFiveThreeNineMatch(unittest.TestCase):
    """
    此處先不管它,後續有時間再補上單元測試
    """
    pass

if __name__ == '__main__':
 
    load_dotenv()

    db_setting = {
        "server": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_DATABASE")
    }
    # 檢查環境變數是否正確載入
    if None in db_setting.values():
        raise ValueError("One or more environment variables are not set. Please check your .env file.")

    dbContext = MSSQLDbContext(db_setting)
    fiveThreeNineMatch = FiveThreeNineMatch()
    
    ENV_PERIOD = int(os.getenv("PERIOD"))
    ENV_TIMES = int(os.getenv("TIMES"))

    feedback = FeedBack539(period=ENV_PERIOD, times=ENV_TIMES, dbContext=dbContext, fiveThreeNineMatch=fiveThreeNineMatch)

    result_df = feedback.calcu()
    print(result_df)
 
    # 先清空
    result_file_path = os.path.join(base_dir,'calcu_539' ,'feedback', 'results.csv')
    result_dir = os.path.dirname(result_file_path)
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
    if os.path.exists(result_file_path):
        os.remove(result_file_path)
    logging.info(f"Saving result to {result_file_path}")
    # 儲存結果
    result_df.to_csv(result_file_path, index=False)
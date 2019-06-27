import pymysql
import copy
from threading import Thread, Lock
# from twisted.enterprise import adbapi

import settings


class MySQL(object):
    insert_template = """
        INSERT INTO bilibili_user_info VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """
    test_template = """
            INSERT INTO bilibili_user_info VALUES({},'{}','{}','{}',{},'{}',{},{},{},{},{},{},'{}',{},{},{},{},{},{},'{}'){new_item}
        """
    base = ",({},'{}','{}','{}',{},'{}',{},{},{},{},{},{},'{}',{},{},{},{},{},{},'{}'){new_item}"

    def __init__(self, recv_pipe):
        self.db = pymysql.Connect(
            host=settings.sqlsetting["HOST"],
            port=settings.sqlsetting["PORT"],
            db=settings.sqlsetting["DB"],
            user=settings.sqlsetting["USER"],
            password=settings.sqlsetting["PASSWORD"],
            charset=settings.sqlsetting["CHARSET"], )
        self.cache = []
        self.recv_pipe = recv_pipe
        self.lock = Lock()

    def run(self):
        def recv():
            while True:
                try:
                    data = self.recv_pipe.recv()
                except Exception as e:
                    print(e)
                    continue
                if data == "exit":
                    self.flush()
                    continue
                self.insert(data)

        th = Thread(target=recv)
        th.start()

    def insert(self, item):
        self.cache.append(item)
        # 缓冲区满时提交插入申请
        if len(self.cache) >= settings.INSERT_CACHE:
            items = self.cache[:settings.INSERT_CACHE]
            del self.cache[:settings.INSERT_CACHE]
            self._insert_data(items)

    def _insert_data(self, items):
        # sql = copy.deepcopy(self.test_template)
        # for item in items:
        #     sql = sql.format(*item, new_item=self.base)
        # sql = sql.replace(self.base, "").strip()+ ";"
        result = 0

        # lists = [items[i:i + 500] for i in range(0, len(items), 500)]
        # for tasks in lists :
        sql = copy.deepcopy(self.test_template)
        for item in items:
            sql = sql.format(*item, new_item=self.base)
        sql = sql.replace(self.base, "").strip() + ";"

        self.db.ping(reconnect=True)
        cursor = self.db.cursor()
        result += cursor.execute(sql)
        self.db.commit()
        print("数据库插入完成，返回值：", result, "数据长度：", len(items))


if __name__ == "__main__":
    # 测试代码。
    _defult_item = {"uid": -1,
                    "user_name": "None",  # 用户名陈
                    "user_sign": "",  # 用户签名
                    "level": -1,  # 用户等级
                    "video": -1,  # 投稿视频
                    "audio": -1,  # 音频
                    "article": -1,  # 专栏
                    "album": -1,  # 相册
                    "follow": 0,  # 关注
                    "coins": -1,  # 硬币
                    "vip": -1,  # 大会员状态
                    "fans": 0,  # 粉丝
                    "play_count": 0,  # 总播放量
                    "read_count": 0,
                    "live_title": "",
                    "favorite_list": -1,  # 收藏夹数量
                    "favorite_sum": -1,  # 总计收藏视频
                    "birthday": "01-01",  # 生日
                    "gender": "保密",  # 性别
                    "time": "1970-01-01 00:00:00", }  # 数据获取时间

    data_format = ["uid", "user_name", "user_sign", "gender", "level", "birthday", "coins", "vip", "favorite_list",
                   "favorite_sum", "follow", 'fans',
                   "live_title", "audio", "video", "album", "article", "play_count", "read_count", "time"]
    testdb = MySQL("test")
    import time

    current = time.time()
    for i in range(1, 10000001):
        item = [str(-404) for _ in data_format]
        item[0] = str(i)
        item[-1] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        testdb.insert(item)

        # INSERT INTO bilibili_user_info VALUES(1,'-404','-404','-404',-404,'-404',-404,-404,-404,-404,-404,-404,'-404',-404,-404,-404,-404,-404,-404,'2019-04-19 16:25:37'),(2,'-404','-404','-404',-404,'-404',-404,-404,-404,-404,-404,-404,'-404',-404,-404,-404,-404,-404,-404,'2019-04-19 16:25:37');

from threading import Thread

from twisted.enterprise import adbapi

import settings


class MySQL(object):
    insert_template = """
        INSERT INTO bilibili_user_info VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """

    def __init__(self, recv_pipe):
        self.dbpool = adbapi.ConnectionPool("pymysql",
                                            host=settings.sqlsetting["HOST"],
                                            port=settings.sqlsetting["PORT"],
                                            db=settings.sqlsetting["DB"],
                                            user=settings.sqlsetting["USER"],
                                            password=settings.sqlsetting["PASSWORD"],
                                            charset=settings.sqlsetting["CHARSET"],
                                            cp_reconnect=True)
        self.dbpool.start()
        self.cache = []
        self.recv_pipe = recv_pipe
        self.item_cout = 0

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
        self.item_cout += 1

        # 缓冲区满时提交插入申请
        if len(self.cache) >= settings.INSERT_CACHE:
            print("提交插入申请,item计数：", self.item_cout)
            items = self.cache[:settings.INSERT_CACHE]
            del self.cache[:settings.INSERT_CACHE]
            query = self.dbpool.runInteraction(self._insert_data, items)
            query.addErrback(self.errback, items)

    # 立即插入缓存冲区数据
    def flush(self):
        print("插入缓冲区数据")
        if self.cache:
            items = self.cache[:]
            del self.cache[:]
            self.dbpool.runInteraction(self._insert_data, items)

    def _insert_data(self, cursor, items):
        global result
        try:
            result = cursor.executemany(self.insert_template, items)
        except Exception as e:
            print("错误信息：", e, items)
            query = self.dbpool.runInteraction(self._insert_data, items)
            query.addErrback(self.errback, items)
        print("数据库插入完成，返回值：", result, "数据长度：", len(items))

    def errback(self, failure, items):
        print(failure)
        with open("log.text", "a+", encoding="utf8") as fp:
            fp.write("错误信息：" + str(failure))
            fp.write("错误时间：" + str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "\n"))
            sort_list = sorted(items, key=lambda x: x[0])
            fp.write("起止URL：" + str(sort_list[0][0]) + "到" + str(items[-1][0] + "\n\n"))

        # 检测到错误时重新插入数据
        query = self.dbpool.runInteraction(self._insert_data, items)
        query.addErrback(self.errback, items)


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
    for i in range(1, 50000001):
        item = [-404 for _ in data_format]
        item[0] = i
        item[-1] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        testdb.insert(item)
        if i % 5000000 == 0:
            print("已插入:", i)

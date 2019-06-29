import pymysql
import json
import threading
import queue


class MySQL(object):
    sqlsetting = None
    db = None
    uid_count = 1
    max_uid = 1382231
    lock = threading.Lock()
    data_list = queue.Queue()
    thread_pool = []
    sql_template = "select * from bilibili_user_info WHERE uid >= {0} AND uid < {1}"

    def __init__(self):
        self.readConfig()
        self.connection()

    def start(self):
        # for i in range(1):
            th = threading.Thread(target=self.work_thread)
            th.start()
            self.thread_pool.append(th)

    def readConfig(self):
        with open("DataBaseSettings.ini", "r") as fp:
            self.sqlsetting = json.loads(fp.read())["default"]

    def connection(self):
        self.db = pymysql.Connection(host=self.sqlsetting["HOST"],
                                     port=self.sqlsetting["PORT"],
                                     db=self.sqlsetting["DB"],
                                     user=self.sqlsetting["USER"],
                                     password=self.sqlsetting["PASSWORD"],
                                     charset=self.sqlsetting["CHARSET"])

    def get_cursor(self):
        self.db.ping(True)
        return self.db.cursor()

    def get_uid_interval(self):
        if self.uid_count <= self.max_uid - 5000:
            self.lock.acquire()
            self.uid_count += 5000
            self.lock.release()
            return self.uid_count - 5000, self.uid_count
        elif self.uid_count <= self.max_uid:
            self.lock.acquire()
            temp = self.uid_count
            self.uid_count += self.max_uid - self.uid_count
            self.lock.release()
            return temp, self.uid_count
        return -1, -1

    def work_thread(self):
        while True :
            uid = self.get_uid_interval()
            cursor = self.get_cursor()
            cursor.execute(self.sql_template.format(*uid))
            self.data_list.put(cursor.fetchall())

    def ReadData(self):
        while self.uid_count <= self.max_uid:
            data = self.data_list.get()
            print("正在导出：", data[0][0], "-", data[-1][0])
            yield data
        print("迭代结束，当前UID：",self.uid_count," 最大UID：",self.max_uid)


if __name__ == "__main__":
    sql = MySQL()

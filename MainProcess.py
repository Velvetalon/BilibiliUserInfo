import socket
from multiprocessing import Pipe, Process

import sys

import mysql_api
from CrawlProcess import *
from requests_api import *


class MainProcess(object):
    def __init__(self, data_pipe, MAX_PROCESS=MAX_PROCESS):
        print("按下Ctrl+C即可中止爬取作业")
        self.max_process = MAX_PROCESS
        self.url = "https://space.bilibili.com/"

        self.pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT)
        self.r_con = redis.Redis(connection_pool=self.pool)
        self.process_list = []
        self.exit_flag = False
        self.data_pipe = data_pipe

        self.mins_pages_count = 0
        self.mins_items_count = 0
        self.total_pages_count = 0
        self.total_items_count = 0

    def run(self):
        self.exit_pipe, self.exit_ch_pipe = Pipe()
        self.count_pipe, self.count_ch_pipe = Pipe()
        for count in range(self.max_process):
            pro = Process(target=CrawlProcess, args=(self.exit_ch_pipe, self.count_ch_pipe, self.data_pipe))
            self.process_list.append(pro)
            pro.daemon = False
            pro.start()

        self.redis_clear()
        # 读取数据并存入缓存
        old_data = self.load(DATAFILE_PATH) or {}
        if old_data.get("save", None):
            print("数据长度：", len(old_data["save"]))
            for item in old_data['save']:
                self.r_con.rpush("Pending", item)
        # 计算uid
        self.uid = old_data.get("uid", 1)

        # 记录启动时间
        self.seconds = time.time()
        self.start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        self.run_damon_thread()  # 监控子进程运行状态。
        self.bind_exit_signal()  # 绑定退出信号。
        self.run_statistics_thread()  # 爬取速度计数。
        self.build_data()

    def run_damon_thread(self):
        def dameon():
            while True:
                if not self.exit_flag:
                    for index in range(self.max_process):
                        if not self.process_list[index].is_alive():
                            print("检测到爬取进程中止，已重启")
                            pro = Process(target=CrawlProcess,
                                          args=(self.exit_ch_pipe, self.count_ch_pipe, self.data_pipe))
                            self.process_list[index] = pro
                            pro.daemon = False
                            pro.start()
                time.sleep(1)
                print("剩余长度：", self.r_con.llen("Pending"))

        th = Thread(target=dameon)
        th.daemon = False
        th.start()

    def run_statistics_thread(self):
        # 接收计数
        def receive():
            while True:
                try:
                    page_count, item_count = self.count_pipe.recv()
                except EOFError:
                    return
                self.mins_pages_count += page_count
                self.mins_items_count += item_count

        # 统计汇总计数
        def statistics():
            print_template = "本次启动总计爬取了{total_pages}个页面，提取了{total_items}条数据。（{mins_pages} pages/mins）（{mins_items} items/mins）"
            while True:
                time.sleep(60)
                self.total_pages_count += self.mins_pages_count
                self.total_items_count += self.mins_items_count
                print(print_template.format(
                    total_pages=str(self.total_pages_count),
                    total_items=str(self.total_items_count),
                    mins_pages=str(self.mins_pages_count),
                    mins_items=str(self.mins_items_count),
                ))
                sys.stdout.flush()
                self.mins_pages_count, self.mins_items_count = 0, 0  # 0v0

        th = Thread(target=receive)
        th_2 = Thread(target=statistics)
        th.start()
        th_2.start()

    def bind_exit_signal(self):
        def monitor():
            if not self.exit_flag:
                self.exit_flag = True
                print("开始执行退出流程，请稍候。")
                print("等待子进程退出")
                self.exit_process()
                print("等待数据同步")
                self.data_pipe.send("exit")
                time.sleep(10)
                print("保存redis cache")
                self.save_redis_cache()
                print("所有进程已退出")
                end_s = time.time()
                end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                print("本次工作统计信息：")
                print("启动时间：", self.start_time)
                print("结束时间：", end_time)
                print("平均速度：",
                      round(((self.total_pages_count + self.mins_pages_count) / (end_s - self.seconds)) * 60, 2),
                      "pages/mins ",
                      round(((self.total_items_count + self.mins_items_count) / (end_s - self.seconds)) * 60, 2),
                      "items/mins")
                sys.stdout.flush()
                os._exit(0)

        def recv_exit():
            recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            recv_socket.bind(("127.0.0.1", 6655))
            while True:
                msg, addr = recv_socket.recvfrom(1024)
                if msg.decode('utf-8') == "exit":
                    # 执行清理工作。确保所有进程工作完成后结束自身。
                    monitor()

        th = Thread(target=recv_exit)
        th.start()

        # # 捕捉中断信号和软件终止信号
        # signal.signal(signal.SIGINT, monitor)
        # signal.signal(signal.SIGTERM, monitor)

    # 取出redis缓存并保存
    def save_redis_cache(self):
        save_data_list = {
            "save": [],
            "uid": self.uid,
        }
        print("剩余数据：", self.r_con.llen("Pending"))
        while self.r_con.llen("Pending") > 0:
            try:
                data = self.r_con.lpop("Pending").decode("utf8")
            except Exception:
                pass
            else:
                save_data_list["save"].append(data)
        print("剩余数据数量：", len(save_data_list["save"]))
        self.save(save_data_list, DATAFILE_PATH)
        print("redis cache保存完毕")

    # 保存数据
    def save(self, data, path):
        json_data = json.dumps(data)
        with open(path, "w+", encoding="utf8") as fp:
            fp.write(json_data)

    # 读取数据
    def load(self, path):
        try:
            with open(path, "r", encoding="utf8") as fp:
                old_data = json.loads(fp.read())
            return old_data
        except FileNotFoundError:
            return None

    # 停止所有进程
    def exit_process(self):
        # 发送停止信号
        for _ in range(len(self.process_list)):
            self.exit_pipe.send("41538636ec35")
        for process in self.process_list:
            process.join()

    # 以一定延迟生成待请求的url。
    # 最大请求速度将取决于生成url的速度。
    def build_data(self):
        def build():
            print("上次爬取到：", self.uid)
            while self.uid < END_UID:
                if not self.exit_flag:
                    llen = self.r_con.llen("Pending")
                    if llen < MAX_PENDING:
                        self.r_con.rpush("Pending", str(self.uid))
                        self.uid += 1
                    if llen > MAX_PENDING >> 1:
                        time.sleep(REQUEST_BUILD_DELAY)

        th = Thread(target=build)
        th.start()

    # 清空redis缓存
    def redis_clear(self):
        while self.r_con.llen("Pending") != 0:
            self.r_con.lpop("Pending")


if __name__ == "__main__":
    recv_pipe, insert_pipe = Pipe()
    mp = MainProcess(insert_pipe)
    mysql = mysql_api.MySQL(recv_pipe)
    mysql.run()
    mp.run()

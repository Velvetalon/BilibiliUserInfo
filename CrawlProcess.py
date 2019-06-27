import copy
import os
import time
from threading import Thread

import redis
from fake_useragent import UserAgent

import ProxyPool
from requests_api import *


class CrawlProcess(object):
    _defult_item = {"uid": -1,
                    "user_name": "None",  # 用户名陈
                    "user_sign": "",  # 用户签名
                    "level": 0,  # 用户等级
                    "video": 0,  # 投稿视频
                    "audio": 0,  # 音频
                    "article": 0,  # 专栏
                    "album": 0,  # 相册
                    "follow": 0,  # 关注
                    "coins": 0,  # 硬币
                    "vip": 0,  # 大会员状态
                    "fans": 0,  # 粉丝
                    "play_count": 0,  # 总播放量
                    "read_count": 0,
                    "live_title": "",
                    "favorite_list": 0,  # 收藏夹数量
                    "favorite_sum": 0,  # 总计收藏视频
                    "birthday": "None",  # 生日
                    "gender": "None",  # 性别
                    "time": "1970-01-01 00:00:00", }  # 数据获取时间
    _defult_template = [
        "https://api.bilibili.com/x/space/acc/info?mid={uid}",  # 生日，名称，签名，等级
        "https://api.bilibili.com/medialist/gateway/base/created?pn=1&ps=10&up_mid={uid}",  # 大会员状态，收藏夹状态
        "https://api.bilibili.com/x/relation/stat?vmid={uid}",  # 关注数，粉丝数
        "https://api.live.bilibili.com/room/v1/Room/getRoomInfoOld?mid={uid}",  # 直播间信息
        "https://api.bilibili.com/x/space/navnum?mid={uid}",  # 投稿信息
        "https://api.bilibili.com/x/space/upstat?mid={uid}",  # 总播放量
    ]

    def __init__(self, exit_pipe, count_pipe, data_pipe):
        self.pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT)
        self.r_con = redis.Redis(connection_pool=self.pool)
        self.ua = UserAgent()
        self.exit_flag = False  # 退出信号

        self.exit_pipe = exit_pipe
        self.count_pipe = count_pipe
        self.data_pipe = data_pipe

        self.crawl_threads = []  # 爬取线程

        self.data_format = ["uid", "user_name", "user_sign", "gender", "level", "birthday", "coins", "vip",
                            "favorite_list", "favorite_sum", "follow", 'fans',
                            "live_title", "audio", "video", "album", "article", "play_count", "read_count", "time"]

        self.pages_count = 0
        self.items_count = 0

        self.redis = ProxyPool.ProxyAPI()

        self.run()

    def run(self):
        self.listen_exit_signal()
        for i in range(MAX_THREAD):
            th = Thread(target=self.crawl)
            self.crawl_threads.append(th)
            # th.daemon = False
            th.start()
        self.run_report_thread()
        self.run_damon_thread()

    # 监控信号
    def listen_exit_signal(self):
        def listen_exit():
            while True:
                msg = self.exit_pipe.recv()
                if msg == "41538636ec35":
                    # 执行清理工作。确保所有线程工作完成后结束自身。
                    self.exit()
                    break
            os._exit(0)

        th = Thread(target=listen_exit)
        th.start()

    def exit(self):
        self.exit_flag = True
        for thread in self.crawl_threads:
            thread.join()

    def run_report_thread(self):
        def report():
            while True:
                time.sleep(1)
                self.count_pipe.send((self.pages_count, self.items_count))
                self.pages_count, self.items_count = 0, 0

        th = Thread(target=report)
        th.start()

    # 获取默认数据项
    def get_item(self):
        return copy.deepcopy(self._defult_item)

    def get_template(self):
        return copy.deepcopy(self._defult_template)

    def run_damon_thread(self):
        def dameon():
            while True:
                if not self.exit_flag:
                    for index in range(MAX_THREAD):
                        if not self.crawl_threads[index].is_alive():
                            print("检测到线程中止，已重启")
                            th = Thread(target=self.crawl)
                            th.daemon = False
                            self.crawl_threads[index] = th
                            th.start()
                time.sleep(1)

        th = Thread(target=dameon)
        th.daemon = False
        th.start()

    # 爬取数据
    def crawl(self):
        while True:
            while True:
                try:
                    if self.exit_flag:
                        return
                    uid = self.r_con.lpop("Pending").decode("utf8")
                except AttributeError:
                    pass
                else:
                    break

            response_list = []
            while True:
                url_template = self.get_template()
                for url in url_template:
                    while True:
                        try:
                            proxy = self.redis.get_proxy()
                            response = requests_get(url=url.format(uid=uid), proxy=proxy, user_agent=UserAgent().random)
                        except Exception:
                            continue
                        if response.text.startswith("<"):
                            print("解析失败，已被封禁")
                            self.redis.delete_proxy(proxy, 0)
                            continue
                        json_data = json.loads(response.text)
                        judge = json_data.get("code", None)
                        if judge and int(judge) == -404:
                            # print("请求拦截成功，用户不不存在：",json_data)
                            response.url = uid
                            url_template.clear()
                        response_list.append(response)
                        break
                break
            self.pages_count += 1
            # 处理数据
            self.process(response_list)

    # 处理返回值
    def process(self, response_):
        response_list = []
        for response in response_:
            response_list.append(json.loads(response.text))

        judge = response_list[0].get("code", None)
        if judge and int(judge) == -404:
            # 生成空数据并插入
            item = [-404 for _ in self.data_format]
            item[0] = response_[0].url
            item[-1] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            self.data_pipe.send(tuple(item))
            self.items_count += 1
            return

        item = self.get_item()
        try:
            item["uid"] = response_list[0]["data"]["mid"]
            item["user_name"] = response_list[0]["data"]["name"]
            item["user_sign"] = response_list[0]["data"]["sign"]
            item["gender"] = response_list[0]["data"]["sex"]
            item["level"] = response_list[0]["data"]["level"]
            item["birthday"] = response_list[0]["data"]["birthday"] or "隐藏"
            item["coins"] = response_list[0]["data"]["coins"]
            item["vip"] = response_list[0]['data']['vip']['type']

            try:
                item["favorite_list"] = response_list[1]['data']['count']
                item["favorite_sum"] = 0
                for data in response_list[1]['data']['list']:
                    item["favorite_sum"] += data["media_count"]
            except KeyError:
                item["favorite_list"] = 0
                item["favorite_sum"] = 0

            item["follow"] = response_list[2]['data']['following'] if response_list[2]['data']['following'] else 0
            item['fans'] = response_list[2]['data']['follower'] if response_list[2]['data']['follower'] else 0

            try:
                item["live_title"] = response_list[3]['data']['title']
            except KeyError:
                item["live_title"] = ""

            item["audio"] = response_list[4]['data']["audio"] if response_list[4]['data']["audio"] else 0
            item["video"] = response_list[4]['data']["video"] if response_list[4]['data']["video"] else 0
            item["album"] = response_list[4]['data']["album"] if response_list[4]['data']["album"] else 0
            item["article"] = response_list[4]["data"]["article"] if response_list[4]["data"]["article"] else 0

            item["play_count"] = response_list[5]['data']["archive"]["view"] if response_list[5]['data']["archive"][
                "view"] else 0
            item["read_count"] = response_list[5]['data']["article"]["view"] if response_list[5]['data']["article"][
                "view"] else 0

            item["time"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        except KeyError as e:
            print("以下是debug信息：")
            print(e)
            for i in response_list:
                print(i)
            print()
            print(item)
            print()
            print("debug信息结束")

        # 提交数据库插入
        self.data_pipe.send(tuple([item[mat] for mat in self.data_format]))
        self.items_count += 1

    # 检测用户是否存在。
    def is_none_user(self, response):
        return True if int(json.loads(response.text)['code']) == -404 else False

from fake_useragent import UserAgent
import redis
from settings import *
import time,random

class Proxy(object):
    #关闭ssl验证
    ua = UserAgent(verify_ssl=False)

    def __init__(self):
        self.pool = redis.ConnectionPool(host=REDIS_HOST,port=REDIS_PORT)
        self.r_con = redis.Redis(connection_pool=self.pool)
        self.proxy_time_dict = {}

    #随机获取一个代理
    def get_proxy(self, protocol:str = "http"):
        type_ = protocol.lower().startswith("https")
        proxy_list_name = PROXY_HTTPS_LIST_NAME if type_ else PROXY_HTTP_LIST_NAME
        while True :
            while True:
                try:
                    proxy = str(self.r_con.srandmember(proxy_list_name), encoding="utf8")
                except TypeError:
                    pass
                else:
                    break
            #检测是否达到单个代理访问速度上限。
            t = self.proxy_time_dict.get(proxy,None)
            if t :
                if time.time() - t > SINGLE_PROXY_DELAY :
                    self.proxy_time_dict[proxy] = time.time()
                    break
            else :
                self.proxy_time_dict[proxy] = time.time()
                break
        return proxy

    #移除一个代理。
    def delete_proxy(self,proxy,mode:int):
        if proxy.lower().startswith("https"):
            print("从https队列删除:",proxy)
            self.r_con.srem(PROXY_HTTPS_LIST_NAME, proxy)
        else:
            print("从http队列删除:", proxy)
            self.r_con.srem(PROXY_HTTP_LIST_NAME, proxy)

        if self.proxy_time_dict.get(proxy,False) :
            del self.proxy_time_dict[proxy]
import requests
from settings import *
import json
import random

def requests_get(url,timeout = 3,proxy = None,user_agent = "Python 3.6.3"):
    proxies = None
    if proxy :
        proxies = {"http": proxy, "https": proxy}
    response = requests.get(url, proxies=proxies, timeout=timeout,
                 headers={'User-Agent': user_agent})

    return response
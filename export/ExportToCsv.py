import csv
import os
import time
import codecs


class Csv(object):
    itorater = None
    file_name = "Result/BilibiliUserInfo_{0}.csv"
    file_count = 1
    current_file_name = None
    book = None
    cursor = None
    flag = False
    current_rows = 2

    head = "uid 用户名 签名 性别 等级 生日 硬币 大会员状态 收藏列表 收藏视频总计 关注 粉丝 直播间标题 投稿音频 投稿视频 投稿画集 投稿专栏 视频播放量 文章阅读数 数据时间".split()

    def __init__(self):
        pass

    # 设置迭代器
    def set_itorater(self, itorater):
        self.itorater = itorater

    # 从迭代器中读取数据并保存
    def start(self):
        self.creat_file()
        for data_list in self.itorater():
            print("开始写入：",data_list[0][0],"到",data_list[-1][0])
            for item in data_list:
                self.write(item)
            print("写入完成：",data_list[0][0],"到",data_list[-1][0])
            del data_list
        print("导出完成")
        self.close()
        exit(0)

    # 建立文件
    def creat_file(self):
        print("creat_file")
        if self.book :
            self.book.close()

        self.current_file_name = self.file_name.format(self.file_count)
        self.file_count += 1

        if not os.path.exists(os.path.split(self.file_name)[0]):
            os.mkdir(os.path.split(self.file_name)[0])

        self.book = open(self.current_file_name, "w+", newline="",encoding="utf-8-sig")
        self.write_fp = csv.writer(self.book, dialect='excel')
        self.write_fp.writerow(self.head)

    # 重新打开文件
    def reopen(self):
        self.book.close()
        self.book = open(self.current_file_name, "a+", newline="",encoding="utf-8-sig")
        self.write_fp = csv.writer(self.book, dialect='excel')

    # 写入数据
    def write(self, row):
        temp = []
        for item in row:
            item = str(item)
            if item == "":
                item = "无"
            item = self.clear_string(item)
            if "," in item:
                item = ("\"" + item + "\"").replace(",", "\,")
            temp.append(item)

        self.write_fp.writerow(row)

        self.current_rows += 1
        # 每10w行保存一次
        if self.current_rows % 100000:
            self.reopen()

        # 每50w行切换文件
        if self.current_rows >= 500000:
            self.current_rows = 2
            self.creat_file()

    # 清洗文件缓冲区
    def save(self):
        self.book.flush()

    #清除字符串中的控制字符。
    def clear_string(self, str):
        for i in range(0, 33):
            str = str.replace(chr(i), '')
        str = str.replace(chr(127), '')
        return str

    def close(self):
        self.save()
        self.book.close()


if __name__ == "__main__":
    excel = Csv()
    excel.close()

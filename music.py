# coding:utf-8
import re
import ssl
import sys
import time
import string
import random
import httplib
import urllib2
import pymongo
import MySQLdb
import warnings
from random import choice
from socket import timeout
from bs4 import BeautifulSoup

reload(sys)
sys.setdefaultencoding("utf-8")

class DB:
    '''
    使用前先确认MySQL关闭链接默认时间，默认8小时，可执行以下改为	
    set global wait_timeout=50;
	set global interactive_timeout=50;
    '''
    conn = None

    def connect(self):
        self.conn = MySQLdb.connect(host='****', port=3306, user='****', passwd='****', db='db2', charset="utf8")
        self.conn.autocommit(1)  # 设置事务自动提交，置0则显示地开启事务

    def query(self, sql):  # 超时则一直重连直到重新连上
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            return cursor
        except (AttributeError, MySQLdb.OperationalError):
            print "MySQL connect"
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(sql)
            return cursor


def sleep(time1, sec):
    ''' 功能：限制代码段的速度
    :param time1: 初始时间
    :param sec: 睡眠间隔
    :return: 
    '''
    time2 = time.time()
    if time2 - time1 < sec:  # 控制sec秒的爬取频率
        if sec > 5: # 休眠超过5秒才打印日志
            print "sleep", sec - time2 + time1
        time.sleep(sec - time2 + time1)


def get_machine_id(table, status1, status0):
    ''' 请求一个机器id,
    :param table: 待爬行的表格名字
    :param status0: 待爬行表格初始状态
    :param status1：待爬行表格中间状态
    :return: 机器id
    '''
    db = DB()
    while 1:  # 新建一个新的机器id
        machine_id = random.randint(1, 10000)
        try:  # 并发模式下，会和某些insert产生幻读，进而死锁
            sql = "UPDATE " + table + " SET status = " + str(status1) + ", machine_id = " + str(machine_id) + " WHERE status = " + str(status0) + " LIMIT 1;"
            db.query(sql)  # 并发分组，一台机器一个id
        except:  
            print "get_machine_id: deadclock"
            continue
        sql = "select COUNT(*) from " + table + " where machine_id = " + str(machine_id) + ";"
        cur = db.query(sql)
        count = cur.fetchall()[0][0]
        if count == 1:
            break
        if count > 1:
            continue
    return machine_id


def request(url, sec):
    ''' 请求一个链接，数据。此处对应豆瓣cookie规则
    :param url: 请求的链接
    :return: 404 count 链接不存在,在此遍历次数
             data count UTF-8编码的返回数据,在此遍历次数
             -1 count 302或者其他URLError,在此遍历次数
    '''
    ug_pool = ["Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.54 Safari/536.5",
                "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
                "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
                "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
                "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
                "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
                "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)",
                "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; GTB5; Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1) ; Maxthon; InfoPath.1; .NET CLR 3.5.30729; .NET CLR 3.0.30618)",
                "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; Acoo Browser; GTB6; Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1) ; InfoPath.1; .NET CLR 3.5.30729; .NET CLR 3.0.30618)",
                "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; Acoo Browser; GTB5; Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1) ; InfoPath.1; .NET CLR 3.5.30729; .NET CLR 3.0.30618)",
                "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; GTB6; Acoo Browser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
                "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Trident/4.0; Acoo Browser; GTB5; Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1) ; InfoPath.1; .NET CLR 3.5.30729; .NET CLR 3.0.30618)",
                "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
                "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; GTB5; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
                "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; GTB5; Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1) ; InfoPath.1; .NET CLR 3.5.30729; .NET CLR 3.0.30618)",
                "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Acoo Browser; InfoPath.2; .NET CLR 2.0.50727; Alexa Toolbar)",
                "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Acoo Browser; .NET CLR 2.0.50727; .NET CLR 1.1.4322)",
                "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Acoo Browser; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR 2.0.50727; FDM; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR 3.5.21022; InfoPath.2)",
                "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; Acoo Browser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
                "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; Acoo Browser; Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1) ; .NET CLR 2.0.50727)",
                ]  # 代理池
    count = 1
    while 1:
        time1 = time.time()
        request = urllib2.Request(url)
        request.headers["Cookie"] = 'bid=' + ''.join(choice(string.ascii_letters + string.digits) for x in range(11))  # 随机产生豆瓣网站的cookie
        request.headers["User-Agent"] = choice(ug_pool)
        request.headers["Accept-Language"] = "zh-CN,zh"
        try:
            data = urllib2.urlopen(request, timeout=4).read()
            data = data.decode('UTF-8')
            if len(data) == 0:
                print "empty data"
                sleep(time1, sec)
                count = count + 1
                continue
            return data, count
        except timeout:
            sleep(time1, sec)
            count = count + 1
            continue
        except ssl.SSLError:
            sleep(time1, sec)
            count = count + 1
            continue
        except urllib2.URLError, e:
            if hasattr(e, 'code') and e.code == 404:
                print e.code
                return 404, count
            if hasattr(e, 'code') and e.code == 302:
                print e.code
                return -1, count
            if hasattr(e, 'code') and e.code == 400:
                sleep(time1, 300)
                print e.code
                continue
            print e.reason
            sleep(time1, sec)
            count = count + 1
            continue
        except httplib.BadStatusLine:
            sleep(time1, sec)
            count = count + 1
            continue
        except httplib.IncompleteRead:
            sleep(time1, sec)
            count = count + 1
            continue

def main():
    client = pymongo.MongoClient('mongodb://s1.gongyan.me:27017/')
    mg = client.db2
    collection = mg.items
    db = DB()
    warnings.filterwarnings("ignore")  # 禁止警告信息输出
    machine_id = get_machine_id("items", 4, 1)
    count = 0
    time1 = time.time()
    while 1:
        ## 请求一条连接
        if count % 99 == 0:  # 豆瓣api每小时100次的限制
            count = 0
            time1 = time.time()
        sql = "UPDATE items SET status = 4, machine_id = " + str(machine_id) + " WHERE status = 1 LIMIT 1;"
        db.query(sql)  # 并发分组，一台机器一个id
        sql = "SELECT item_id FROM items WHERE machine_id = " + str(machine_id) + " LIMIT 1;"
        cur = db.query(sql)  # 从当前机器分组链接池中选一个链接，LIMIT 1会加快查询速度
        subject_id = cur.fetchall()
        if len(subject_id) == 0:  # 分组池无链接，则爬行完成
            print "Finish!!!"
            break
        subject_id = subject_id[0][0]
        # subject_id = 27030004 # 1420193  # 测试，退出后数据库更新 update items set status=1, machine_id = 0 where status = 4; update items set status=1, machine_id = 0 where status = 3;


        ## 获取专辑评论数目
        time2 = time.time()
        url = "https://music.douban.com/subject/" + str(subject_id) + "/"
        data, temp = request(url, 2.1)
        if data == 404:
            sql = "update items set status = -2, machine_id = 0 where item_id = " + str(subject_id) + ";"
            db.query(sql)
            print subject_id, "debug 1: link 404"
            sleep(time2, 2.1)
            continue
        if data == -1:
            print subject_id, "debug 2: Exit"
            return
        pattern = u"(?<=target=\"_self\">全部 )\d+(?= 条</a>)"
        comments_num = re.findall(pattern, data)
        sleep(time2, 2.1)


        ## 取得专辑信息
        time3 = time.time()
        if count == 98:
            print subject_id, "debug 3: sleep hourly"
            sleep(time1, 3600)
        url = "https://api.douban.com/v2/music/" + str(subject_id)
        data, temp = request(url, 2.1)
        count = count + temp
        if data == 404:
            sql = "update items set status = -2, machine_id = 0 where item_id = " + str(subject_id) + ";"
            db.query(sql)
            print subject_id, "debug 5: link 404"
            sleep(time3, 2)
            continue
        if data == -1:
            print subject_id, "debug 6: Exit"
            return

        # 若专辑目前无评论，则只爬取其基本信息
        item = eval(data)
        if len(comments_num) == 0:
            item["comments_num"] = 0
            sql = "update items set status = 3, machine_id = 0 where item_id = " + str(subject_id) + ";"
            db.query(sql)
            print subject_id, "debug 7: mysql status update"
            mg.items.insert(item)
            print subject_id, "debug 8: mongodb insert finish"
            sleep(time3, 2)
            continue
        comments_num = comments_num[0]
        item["comments_num"] = int(comments_num)
        sleep(time3, 2)


        ## 取得专辑评论信息
        subject_ids = [subject_id] * int(comments_num)  # 辅助列表
        user_titles = []  # 用户名成
        user_ids = []  # 用户字符id
        user_num_ids = []  # 用户数字id
        music_ratings = []  # 用户评分
        music_comments = []  # 用户评论
        music_comments_time = []  # 用户评论时间

        for page in range(1, (int(comments_num) - 1) / 20 + 2):  # 总页数，每页20条评论, -1防止页数刚好为20的倍数，另外豆瓣有的页面只有19评
            time4 = time.time()
            # 此辅助五元组用来存数据入mysql，可充分利用睡眠时间
            _subject_ids = [subject_id] * 20  # 辅助列表
            _user_ids = []  # 用户字符id
            _music_ratings = []  # 用户评分
            _music_comments = []  # 用户评论
            _music_comments_time = []  # 用户评论时间

            url = "https://music.douban.com/subject/" + str(subject_id) + "/comments/hot?p=" + str(page)
            data, temp = request(url, 2.1)
            if data == -1:
                print subject_id, "debug 10: Exit"
                return

            soup = BeautifulSoup(data, 'html.parser')
            avatars = soup.find_all("div", "avatar")
            comment_info = soup.find_all("span", "comment-info")
            comments = soup.find_all("div", "comment")

            for _item in avatars:
                user_title = _item.a['title']
                user_titles.append(user_title)
                user_id = _item.a['href'].split('/')[4]
                user_ids.append(user_id)
                _user_ids.append(user_id)
                # user_num_id为用户的数字id,截取头像链接获得https://img1.doubanio.com/icon/u138762062-9.jpg
                user_num_id = _item.img['src'].split('/')[4]
                pattern = r"(?<=u)\d+"
                user_num_id = re.findall(pattern, user_num_id)
                if(len(user_num_id) == 0):
                    user_num_id = -1
                    user_num_ids.append(-1)
                else:
                    user_num_id = user_num_id[0]
                    user_num_ids.append(user_num_id)
                try:    
                    sql = "insert into users values('" + user_id + "', " + str(user_num_id) + ", '" + user_title.replace('\'','`').replace('\\','\\\\') + "', 0, 0, 0);"
                    db.query(sql)  # 此处也可以不统计用户，加入ignore即可，items_users外键参考次数即可。有无数字id可判断用户有无头像，用户名称可判断用户是否注销
                except:
                    sql = "update users set comments_num = comments_num + 1 where user_id = '" + user_id + "';"
                    db.query(sql) 
            for _rating in comment_info:
                try:  # 此处异常可能为用户无评分
                    music_ratings.append(_rating.contents[3]['title'])
                    _music_ratings.append(_rating.contents[3]['title'])
                    music_comments_time.append(_rating.contents[5].text)
                    _music_comments_time.append(_rating.contents[5].text)
                except:
                    music_ratings.append("空")
                    _music_ratings.append("空")
                    music_comments_time.append(_rating.contents[3].text)
                    _music_comments_time.append(_rating.contents[3].text)
            for _comment in comments:
                music_comments.append(_comment.p.text)
                _music_comments.append(_comment.p.text)

            quintuple = (zip(_subject_ids, _user_ids, _music_ratings, _music_comments, _music_comments_time))
            if len(quintuple) == 0:  # 防止有的专辑一页超过20条评论，或者有人临时删评论，如1420193
                sleep(time4, 2.1)
                continue
            sql = "insert ignore into items_users values"
            for i in range(0, len(quintuple)):
                sql = sql + "(" + str(quintuple[i][0]) + ", '" + quintuple[i][1] + "', '" + quintuple[i][2] + "', '" + quintuple[i][3].replace('\'','`').replace('\\','\\\\') + "', '" + quintuple[i][4] + "'),"
            sql = sql[:-1] + ";"
            db.query(sql)
            sleep(time4, 2.1)
            print "debug 9", subject_id, "page", page, "spidering finish"


        ## 合并数据，存入mysql和mongodb
        short_comments = (zip(subject_ids, user_ids, music_ratings, music_comments, music_comments_time))
        item["comments"] = short_comments
        sql = "update items set status = 3, machine_id = 0 where item_id = " + str(subject_id) + ";"
        db.query(sql)
        print subject_id, "debug 11: mysql status update"
        mg.items.insert(item)
        print subject_id, "debug 12: mongodb insert finish"


if __name__ == '__main__':
    main()

# coding:utf-8
import re
import ssl
import time
import string
import random
import MySQLdb
import httplib
import urllib2
import warnings
from random import choice
from socket import timeout


class DB:
    '''
    使用前先确认MySQL关闭链接默认时间，默认8小时，可执行以下改为	
    set global wait_timeout=50;
	set global interactive_timeout=50;
    '''
    conn = None

    def connect(self):
        self.conn = MySQLdb.connect(host='s1.gongyan.me', port=3306, user='rec', passwd='gogo1357A*', db='url', charset="utf8")
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

db = DB()
warnings.filterwarnings("ignore")  # 禁止警告信息输出
machine_id = get_machine_id("items", 2, 0)

while 1:
    time1 = time.time()
    try:  # 并发模式下，此处会和下文的insert产生幻读，进而死锁。为保证效率，不应可串行
        sql = "UPDATE items SET status = 2, machine_id = " + str(machine_id) + " WHERE status = 0 LIMIT 1;"
        db.query(sql)  # 并发分组，一台机器一个id
    except:  
        print subject_id, "debug 1: deadclock when request a link and update"
        continue
    sql = "SELECT item_id FROM items WHERE machine_id = " + str(machine_id) + " LIMIT 1;"
    cur = db.query(sql)  # 从当前机器分组链接池中选一个链接，LIMIT 1会加快查询速度
    subject_id = cur.fetchall()
    if len(subject_id) == 0:  # 分组池无链接，则爬行完成
        print "Finish!!!"
        break
    subject_id = subject_id[0][0]
    # subject_id = 1479011 # 1420193  # 测试，退出后数据库更新 update items set status=1, machine_id = 0 where status = 2;


    url = "https://music.douban.com/subject/" + str(subject_id) + "/"
    data, temp = request(url, 2.1)
    if data == 404:
        sql = "update items set status = -2, machine_id = 0 where item_id = " + str(subject_id) + ";"
        db.query(sql)
        print subject_id, "debug 2: link 404"
        sleep(time1, 2.1)
        continue
    if data == -1:
        print subject_id, "debug 3: Exit"
        break

    pattern = r"(?<=<a href=\"https://music.douban.com/subject/)\d+(?=/\">)"
    subjects = re.findall(pattern, data)
    pattern = u"(?<=target=\"_self\">全部 )\d+(?= 条</a>)"
    comments_num = re.findall(pattern, data)
    if len(subjects) == 0 and len(comments_num) == 0:  # 无出度无评论专辑将其置为-1，等待被删除
        sql = "update items set status = -1, machine_id = 0 where item_id = " + str(subject_id) + ";"
        db.query(sql)
        print subject_id, "debug 4: the link have no comment and no out-degree"
        sleep(time1, 2.1)
        continue
    if len(comments_num) == 0:  # 后续无评论项目，防止溢出
        comments_num = 0
    else:
        comments_num = comments_num[0]
    if len(subjects) == 0:  # 无出度有评论项目, 不再进行遍历
        sql = "update items set status = 1, machine_id = 0, comments_num = " + str(comments_num) + " where item_id = " + str(subject_id) + ";"
        db.query(sql)
        print subject_id, "debug 5: the link have no out-degree"
        sleep(time1, 2.1)
        continue


    sql = "insert ignore into items values"
    for i in range(0, len(subjects)):
        sql = sql + "(" + subjects[i] + ",0,0,0),"
    sql = sql[:-1] + ";"  # 将插入语句合并为一条执行，ignore可以跳过重复，replace可以覆盖重复
    try:
        db.query(sql)  # 并发模式下，此处insert会和最开始的update产生幻读，进而死锁。为保证效率，不应可串行
    except:
        print subject_id, "debug 6: deadclock when insert"
        sleep(time1, 2.1)
        continue


    sql = "update items set status = 1, machine_id = 0, comments_num = " + str(comments_num) + " where item_id = " + str(subject_id) + ";"  # 结束，更新数据
    db.query(sql)
    sleep(time1, 2.1)
    print subject_id, "debug 7: link spidering finish"

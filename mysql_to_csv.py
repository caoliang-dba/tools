# encoding: utf-8
import os
import pymysql
import datetime
import sys
sys.path.append('../')
import time
import argparse
import csv
from progress.bar import Bar


class Etract(object):

    def get_query_result(self, sql, ip, db, passwd,port,csv_file):

        sql1 = u'''{0}'''.format(sql)
        conn = pymysql.connect(
            host=ip,
            user=user,
            passwd=passwd,
            port=port,
            database=db,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        cur = conn.cursor()  # 创建游标

        start_time = datetime.datetime.now()
        print(start_time.strftime('%Y-%m-%d %H:%M:%S'),"正在查询数据,请耐心等待...")
        cur.execute(sql1)  # 执行sql命令

        if os.path.exists(csv_file):
            os.remove(csv_file)  # 判断文件存在则删除

        result = cur.fetchall()  # 获取查询结果
        if not result:
            print("查询结果为空")
            return False

        print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),"查询结果成功，正在写入csv...")
        column = list(result[0])
        sum = len(result)
        with  open(csv_file, 'w', encoding='utf-8-sig') as csvFile:
            writer = csv.writer(csvFile)
            writer.writerow(column)
            i = 0
            with Bar('写入进度:', max=sum) as bar:
                for line in result:
                    value = self.make_time(line)

                    value_1 = self.change_int(value)
                    writer.writerow(value_1)
                    # print(value)
                    i += 1
                    bar.next()

        if not os.path.exists(csv_file):  # 判断文件是否存在
            print("生成csv失败")
            return False

        finished_time =  datetime.datetime.now()
        sum_time = (finished_time-start_time).seconds
        print(finished_time.strftime('%Y-%m-%d %H:%M:%S'),"导出至'%s'文件成功,用时%s秒" %(csv_file,sum_time))
        return True

    def change_int(self,value):
        for i in range(len(value)):
            if value[i] and len(str(value[i])) >= 14:
                try:
                    float(value[i])
                    value[i] = str(value[i]) + '\t'
                except:
                    pass
        return value

    def make_time(self, row):
        value = list(row.values())
        for i in range(len(value)):
            # 判断为日期时
            if isinstance(value[i], datetime.datetime):
                value[i] = value[i].strftime('%Y-%m-%d %H:%M:%S')
        return value

    def readfile(self, filename):
        file = open(filename, 'r')
        fileread = file.read()
        file.close()
        return fileread

    def parse_args(self):
        parser = argparse.ArgumentParser(
            usage='python3 extract_data.py -i <DB ip> -u <DB user> -p <DB password> -d <DB name> -f <sql filename> -P <DB port>',
            description='Dump data', )
        parser.add_argument('-i', '--ip', nargs='?', required=True, help='DB ip')
        parser.add_argument('-u', '--user', nargs='?', required=True, help='DB user')
        parser.add_argument('-p', '--password', nargs='?', required=True, help='DB password')
        parser.add_argument('-d', '--database', nargs='?', required=True, help='DB name')
        parser.add_argument('-f', '--filename', nargs='?', required=True, help='sql filename')
        parser.add_argument('-P', '--port', nargs='?', required=False, help='DB port', type=int, const=1, default=3306)

        return parser.parse_args()


if __name__ == '__main__':
    extract = Etract()
    csv_file = extract.parse_args().filename.split('.')[0]+'.csv'
    ip = extract.parse_args().ip
    port = extract.parse_args().port
    user = str(extract.parse_args().user)
    passwd = str(extract.parse_args().password)
    db = str(extract.parse_args().database)
    sql = extract.readfile(extract.parse_args().filename)
    result2 = extract.get_query_result(sql, ip, db, passwd,port,csv_file)
    # extract.generate_table(result2)


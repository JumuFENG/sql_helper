# Python 3
# -*- coding: utf-8 -*-

import pymysql
import os

class SqlHelper():
    """操作mysql数据库，基本方法 

        """
    def __init__(self, host="localhost", username="root", password="", port=3306, database=None, charset="utf8mb4"):
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.port = port
        self.con = None
        self.cur = None

        try:
            self.con = pymysql.connect(host=self.host, user=self.username, passwd=self.password, port=self.port, charset=charset)
            # 所有的查询，都在连接 con 的一个模块 cursor 上面运行的
            self.cur = self.con.cursor()
            if database is not None:
                self.select_db(database)
        except:
            raise "DataBase connect error,please check the db config."

    def select_db(self, database):
        if not self.is_exist_schema(database):
            sql = "CREATE DATABASE IF NOT EXISTS " + self.database
            self.cur.execute(sql)
        self.con.select_db(database)
        self.database = database

    def close(self):
        """关闭数据库连接

        """
        if not self.con:
            self.con.close()


    def get_version(self):
        """获取数据库的版本号

        """
        self.cur.execute("SELECT VERSION()")
        return self.getOneData()

    def get_one_data(self):
        # 取得上个查询的结果，是单个结果
        data = self.cur.fetchone()
        return data

    def fetch_one_data(self, tablename):
        sql = "select * from %s" % tablename
        self.execute_commit(sql)
        return self.cur.fetchone()

    def create_table(self, tablename, attrdict, constraint):
        """创建数据库表

            args：
                tablename  ：表名字
                attrdict   ：属性键值对,{'book_name':'varchar(200) NOT NULL'...}
                constraint ：主外键约束,PRIMARY KEY(`id`)
        """
        if self.is_exist_table(tablename):
            return
        sql = ''
        sql_mid = '`id` bigint(11) NOT NULL AUTO_INCREMENT,'
        for attr,value in attrdict.items():
            sql_mid = sql_mid + '`'+attr + '`'+' '+ value+','
        sql = sql + 'CREATE TABLE IF NOT EXISTS %s (' % tablename
        sql = sql + sql_mid
        sql = sql + constraint
        sql = sql + ') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
        #print('createTable:' + sql)
        self.execute_commit(sql)

    def execute_sql(self, sql=''):
        """执行sql语句，针对读操作返回结果集

            args：
                sql  ：sql语句
        """
        try:
            self.cur.execute(sql)
            records = self.cur.fetchall()
            return records
        except pymysql.Error as e:
            error = 'MySQL execute failed! ERROR (%s): %s' %(e.args[0],e.args[1])
            print(error)

    def execute_sql_file(self, sql):
        """ 执行 sql'文件中的sql

        """
        if not os.path.isfile(sql):
            raise 'invalid input file path: %s' % sql
        stmts = []
        stmt = ''
        delimiter = ';'
        commentsblock = False
        with open(sql, 'r', encoding='utf-8') as f:
            data = f.readlines()
            for lineno, line in enumerate(data):
                if not line.strip():
                    continue
                if line.startswith('--'):
                    continue
                if line.startswith('/*'):
                    commentsblock = True
                    continue
                if line.endswith('*/'):
                    commentsblock = False
                    continue
                if 'DELIMITER' in line:
                    delimiter = line.split()[1]
                    continue
                if delimiter not in line:
                    stmt += line.replace(delimiter, ';')
                    continue

                if stmt:
                    stmt += line
                    stmts.append(stmt.strip())
                    stmt = ''
                else:
                    stmts.append(line.strip())

        try:
            for s in stmts:
                self.cur.execute(s)
            self.con.commit()
        except pymysql.Error as e:
            self.con.rollback()
            error = 'MySQL execute failed! ERROR (%s): %s' %(e.args[0],e.args[1])
            raise error

    def execute_commit(self,sql=''):
        """执行数据库sql语句，针对更新,删除,事务等操作失败时回滚

        """
        try:
            self.cur.execute(sql)
            self.con.commit()
        except pymysql.Error as e:
            self.con.rollback()
            error = 'MySQL execute failed! ERROR (%s): %s' %(e.args[0],e.args[1])
            print("error:", error)
            return error

    def insert(self, tablename, params):
        """插入数据库

            args：
                tablename  ：表名字
                key        ：属性键
                value      ：属性值
        """
        key = []
        value = []
        for tmpkey, tmpvalue in params.items():
            key.append(tmpkey)
            if isinstance(tmpvalue, str):
                value.append("\'" + tmpvalue + "\'")
            else:
                value.append(tmpvalue)
        attrs_sql = '('+','.join(key)+')'
        values_sql = ' values('+','.join(value)+')'
        sql = 'insert into %s'%tablename
        sql = sql + attrs_sql + values_sql
        #print('_insert:'+sql)
        self.execute_commit(sql)

    def select(self, tablename, fields='*', conds='', order=''):
        """查询数据

            args：
                tablename  ：表名字
                conds      ：查询条件
                order      ：排序条件

            example：
                print mydb.select(table)
                print mydb.select(table, fields=["name"])
                print mydb.select(table, fields=["name", "age"])
                print mydb.select(table, fields=["age", "name"])
                print mydb.select(table, fields=["age", "name"], conds = ["name = 'usr_name'","age < 30"])
        """
        sql = ','.join(fields) if isinstance(fields, list) else fields
        sql = 'select %s from %s ' % (sql, tablename)

        consql = ''
        if conds != '':
            if isinstance(conds, list):
                conds = ' and '.join(conds)
                consql = 'where ' + conds
            else:
                consql = 'where ' + conds

        sql += consql + order
        #print('select:' + sql)
        records = self.execute_sql(sql)
        if records is None:
            return None
        if len(records[0]) == 1:
            return tuple([r for r, in records])

        columns = self.get_table_columns(tablename) if fields == '*' else fields
        return tuple([dict(zip(columns, row)) for row in records])

    def insert_many(self, table, attrs = None, values = None, datalist = None):
        """插入多条数据
            example：
                table='test_mysqldb'
                key = ["id" ,"name", "age"]
                value = [[101, "liuqiao", "25"], [102,"liuqiao1", "26"], [103 ,"liuqiao2", "27"], [104 ,"liuqiao3", "28"]]
                mydb.insert_many(table, key, value)
                data = [{"id":101,"name":"liuqiao","age":"25"},{"id":102,"name":"liuqiao1", "age":"26"}]
                nydb.insert_many(table, datalist=data)
        """
        if datalist is not None:
            if not isinstance(datalist, (list, tuple)):
                self.insert(table, datalist)
                return

            if len(datalist) == 1:
                self.insert(table, datalist[0])
                return

            keys = datalist[0].keys()
            re_data = []
            cand_data = []
            for d in datalist:
                if not len(d.keys()) == len(keys):
                    re_data.append(d)
                    continue
                keys_same = True
                for k in keys:
                    if not k in d.keys():
                        re_data.append(d)
                        keys_same = False
                        continue
                if keys_same:
                    cand_data.append(d)

            cand_list = []
            for d in cand_data:
                d_list = []
                for k in keys:
                    d_list.append(d[k])
                cand_list.append(d_list)

            self.insert_many(table, keys, cand_list)

            if len(re_data):
                self.insert_many(table, datalist = re_data)

        if attrs and values:
            values_sql = ['%s' for v in attrs]
            attrs_sql = '('+','.join(attrs)+')'
            values_sql = ' values('+','.join(values_sql)+')'
            sql = 'insert into %s'% table
            sql = sql + attrs_sql + values_sql
            #print('insert_many:'+sql)
            try:
                for i in range(0,len(values),20000):
                    self.cur.executemany(sql,values[i:i+20000])
                    self.con.commit()
            except pymysql.Error as e:
                self.con.rollback()
                error = 'insert_many executemany failed! ERROR (%s): %s' %(e.args[0],e.args[1])
                print(error)

    def delete(self, tablename, cond_dict):
        """删除数据

            args：
                tablename  ：表名字
                cond_dict  ：删除条件字典

            example：
                params = {"name" : "caixinglong", "age" : "38"}
                mydb.delete(table, params)

        """
        consql = ' '
        if cond_dict!='':
            for k, v in cond_dict.items():
                if isinstance(v, str):
                    v = "\'" + v + "\'"
                consql = consql + tablename + "." + k + '=' + v + ' and '
        consql = consql + ' 1=1 '
        sql = "DELETE FROM %s where%s" % (tablename, consql)
        #print (sql)
        return self.execute_commit(sql)

    def update(self, tablename, attrs_dict, cond_dict):
        """更新数据

            args：
                tablename  ：表名字
                attrs_dict  ：更新属性键值对字典
                cond_dict  ：更新条件字典

            example：
                params = {"name" : "caixinglong", "age" : "38"}
                cond_dict = {"name" : "liuqiao", "age" : "18"}
                mydb.update(table, params, cond_dict)

        """
        attrs_list = []
        consql = ' '
        for tmpkey, tmpvalue in attrs_dict.items():
            attrs_list.append("`" + tmpkey + "`" + "=" +"\'" + str(tmpvalue) + "\'")
        attrs_sql = ",".join(attrs_list)
        #print("attrs_sql:", attrs_sql)
        if cond_dict!='':
            for k, v in cond_dict.items():
                v = "\'" + str(v) + "\'"
                consql = consql + "`" + tablename +"`." + "`" + str(k) + "`" + '=' + v + ' and '
        consql = consql + ' 1=1 '
        sql = "UPDATE %s SET %s where%s" % (tablename, attrs_sql, consql)
        #print(sql)
        return self.execute_commit(sql)

    def update_many(self, table, attrs, conkeys, values):
        """更新多条数据, 有重复则
            args：
                tablename  ：表名字
                attrs      ：属性键
                conkeys      : 条件属性键
                values     ：所有属性值

            example：
                table='test_mysqldb'
                keys = ["name", "age"]
                conkeys = ["id"]
                values = [["liuqiao", "25", 101], ["liuqiao1", "26", 102], ["liuqiao2", "27", 103], ["liuqiao3", "28", 104]]
                mydb.update_many(table, conkeys, keys, values)
        """
        attrs_list = [a + '=(%s)' for a in attrs]
        attrs_sql = ','.join(attrs_list)
        cond_list = [c + '=(%s)' for c in conkeys]
        cond_sql = ' and '.join(cond_list)
        sql = "UPDATE %s SET %s where %s" % (table, attrs_sql, cond_sql)
        #print(sql)
        try:
            for i in range(0,len(values),20000):
                self.cur.executemany(sql, values[i:i+20000])
                self.con.commit()
        except pymysql.Error as e:
            self.con.rollback()
            error = 'insert_update_many executemany failed! ERROR (%s): %s' %(e.args[0],e.args[1])
            print(error)

    def insert_update_many(self, table, attrs, conkeys, values):
        """插入多条数据, 有重复则更新
        """
        values_new = []
        values_exist = []
        for v in values:
            cond_list = []
            for i in range(0, len(conkeys)):
                cond_list.append('%s = \'%s\'' % (conkeys[i], str(v[len(attrs) + i])))
            cond_sql = ' or '.join(cond_list)
            selectrows = self.select(table, conkeys, conds = cond_sql)
            if selectrows is None or len(selectrows) == 0:
                values_new.append(v)
            else:
                values_exist.append(v)

        if len(values_new) > 0:
            self.insert_many(table, attrs + conkeys, values_new)

        if len(values_exist) > 0:
            self.update_many(table, attrs, conkeys, values_exist)

    def drop_table(self, tablename):
        """删除数据库表

            args：
                tablename  ：表名字
        """
        sql = "DROP TABLE  %s" % tablename
        self.execute_commit(sql)

    def delete_table(self, tablename):
        """清空数据库表

            args：
                tablename  ：表名字
        """
        sql = "DELETE FROM %s" % tablename
        self.execute_commit(sql)

    def is_exist_schema(self, database):
        result, = self.select("information_schema.SCHEMATA", "count(*)",["schema_name = '%s'" % database]);
        return result > 0

    def is_exist_table(self, tablename):
        """判断数据表是否存在

            args：
                tablename  ：表名字

            Return:
                存在返回True，不存在返回False
        """
        result, = self.select("information_schema.tables","count(*)",["table_name = '%s'" % tablename, "table_schema = '%s'" % self.database])
        return result > 0

    def is_exist_table_column(self, tablename, column_name):
        result, = self.select("information_schema.columns","count(*)",["table_name = '%s'" % tablename, "column_name = '%s'" % column_name, "table_schema = '%s'" % self.database])
        return result > 0

    def get_table_columns(self, tablename):
        return self.select("information_schema.columns", "column_name", ["table_schema = '%s'" % self.database, "table_name = '%s'" % tablename])

    def add_column(self, tablename, col, tp):
        sql = "alter table %s add %s %s" % (tablename, col, tp)
        self.execute_commit(sql)

    def delete_column(self, tablename, col):
        sql = "alter table %s drop column %s" % (tablename, col)
        self.execute_commit(sql)

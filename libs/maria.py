########################################################################################################################
#    File: db-maria.py
# Purpose: Class to make working with MariaDB/MySQL a lot easier.
#  Author: Dan Huckson, https://github.com/unodan
########################################################################################################################
import os
import time
import logging as lg

from pymysql import connect

version = '0.1'


class MariaDB:
    def __init__(self, **kwargs):
        self.host = None
        self.port = None
        self.conn = None
        self.cursor = None
        self.db_name = None
        self.db_user = None
        self.db_password = None
        self.charset = None

        log_file = kwargs.get('log_file', 'maria.log')
        log_level = kwargs.get('log_level', lg.DEBUG)
        log_format = kwargs.get('log_format', '%(levelname)s:%(name)s:%(asctime)s:%(message)s')
        log_datefmt = kwargs.get('log_datefmt', '%Y/%m/%d %I:%M:%S')

        lg.basicConfig(filename=log_file, level=log_level, format=log_format, datefmt=log_datefmt)
        lg.info('__init__:Object created')

    def use(self, database, **kwargs):
        database = kwargs.get('database', database)

        sql = f'USE {database}'
        try:
            self.cursor.execute(sql)
            self.db_name = database
            self.set_autocommit(autocommit=kwargs.get('autocommit', True))
            lg.info(f'use:{sql}')
            return True
        except Exception as err:
            lg.error(f'use:{str(err)}:{sql}')

    def dump(self, database):
        try:
            t = time.strftime('%Y-%m-%d_%H:%M:%S')
            os.popen('mysqldump -u %s -p%s -h %s -e --opt -c %s | gzip -c > %s.gz' % (
                self.db_user, self.db_password, self.host, database, database + '_' + t))
            lg.info('dump:' + database + '_' + t + '.gz')
            return True
        except Exception as err:
            lg.error('dump:' + str(err))

    def connection_close(self):
        try:
            self.cursor.connection_close()
            self.conn.connection_close()
            self.conn = None
            self.cursor = None
            lg.info(f'close:Closed database ({self.db_name})')
            self.db_name = None
            return True
        except Exception as err:
            lg.error('close:' + str(err))

    def close(self):
        self.cursor.close()
        self.conn.close()
        self.conn = self.cursor = None

    def commit(self):
        try:
            self.conn.commit()
            lg.info('commit')
            return True
        except Exception as err:
            lg.error(f'commit:{str(err)}')

    def connect(self, database=None, **kwargs):
        info = kwargs.get('connection')
        if not info:
            return

        self.host = info['host']
        self.port = info['port']
        self.db_user = info['user']
        self.db_password = info['password']
        self.charset = info.get('charset', None)

        database = self.db_name = kwargs.get('database', database)

        try:
            self.conn = connect(
                host=self.host, port=self.port,
                user=self.db_user, passwd=self.db_password, charset=self.charset)

            self.cursor = self.conn.cursor()

            if database:
                if not self.database_exist(database):
                    self.create_database(database)
            self.use(database)

            lg.info(f'connect:Connection authenticated:('
                    f'host={self.host}, '
                    f'port={self.port}, '
                    f'user={self.db_user}, '
                    f'database={database} '
                    f'charset={self.charset})')
            return True
        except Exception as err:
            lg.error(f'connect:{str(err)}')

    def execute(self, sql, args=None):
        try:
            lg.info(f'execute:{sql}')
            return self.cursor.execute(sql, args)
        except Exception as err:
            lg.error(f'execute:{str(err)}:{sql}')

    def fetchone(self):
        try:
            return self.cursor.fetchone()
        finally:
            pass

    def fetchall(self):
        try:
            return self.cursor.fetchall()
        finally:
            pass

    def drop_table(self, table):
        sql = f'DROP TABLE {table}'
        try:
            lg.info(f'drop_table:{sql}')
            return self.cursor.execute(sql)
        except Exception as err:
            lg.error(f'drop_table:{str(err)}:{sql}')

    def drop_index(self, table, index):
        sql = f'DROP INDEX {index} ON {table};'
        try:
            lg.info(f'drop_index:{sql}')
            return self.cursor.execute(sql)
        except Exception as err:
            lg.error(f'drop_index:{str(err)}:{sql}')

    def drop_database(self, database, **kwargs):
        database = kwargs.get('database', database)
        sql = f'DROP DATABASE {database};'
        try:
            lg.info(f'drop_database:{sql}')
            return self.cursor.execute(sql)
        except Exception as err:
            lg.error(f'drop_database:{str(err)}:{sql}')

    def create_table(self, table, sql, **kwargs):
        database = kwargs.get('database', self.db_name)
        database_engine = kwargs.get('database_engine', 'InnoDB')

        sql = f'CREATE TABLE {table} ({sql}) ENGINE=%s'
        try:
            lg.info('create_table:' + sql)
            if database == self.db_name:
                return self.cursor.execute(sql, (database_engine,))
        except Exception as err:
            lg.error(f'create_table:{str(err)}:{sql}')

    def create_index(self, table, column, index):
        sql = f'CREATE INDEX {index} ON {table}({column});'
        try:
            lg.info(f'create_index:{sql}')
            return self.cursor.execute(sql)
        except Exception as err:
            lg.error(f'create_index:{str(err)}:{sql}')

    def create_database(self, database, **kwargs):
        database = kwargs.get('database', database)

        sql = f'CREATE DATABASE {database};'
        try:
            lg.info(f'create_database:{sql}')
            return self.cursor.execute(sql)
        except Exception as err:
            lg.error(f'create_database:{str(err)}:{sql}')

    def row_exist(self, table, _id):
        sql = f'SELECT id FROM {table} WHERE id=%s;'
        if self.execute(sql, (_id,)):
            return self.fetchone()

    def table_exist(self, table, **kwargs):
        database = kwargs.get('database', self.db_name)
        sql = 'SELECT table_name FROM information_schema.tables WHERE table_schema=%s AND table_name=%s;'

        try:
            self.cursor.execute(sql, (database, table))
            return self.cursor.fetchone()
        except Exception as err:
            lg.error(f'table_exist:{str(err)}')

    def index_exist(self, table, index, **kwargs):
        result = False
        database = kwargs.get('database', self.db_name)

        if database:
            sql = f'SELECT 1 FROM information_schema.statistics WHERE table_schema="{database}" AND ' \
                  f'table_name="{table}" AND index_name="{index}";'
            try:
                if database == self.db_name and self.cursor.execute(sql):
                    result = self.cursor.fetchone()
                else:
                    self.use(database)
                    if self.cursor.execute(sql):
                        result = self.cursor.fetchone()
                    self.use(database)
            except Exception as err:
                lg.error(f'index_exist:{str(err)}')

        return result

    def database_exist(self, database, **kwargs):
        database = kwargs.get('database', database)

        sql = 'SHOW DATABASES;'
        try:
            self.cursor.execute(sql)
            for row in self.cursor.fetchall():
                if database in row:
                    return True
        except Exception as err:
            lg.error(f'database_exist:{str(err)}')

    def insert_row(self, table, row):
        column_names = []

        for name in self.get_columns_metadata(table):
            column_names.append(name[3])

        sql = f"INSERT INTO {table} ({','.join(column_names[1:])}) VALUES ({('%s,' * len(row)).rstrip(',')});"
        try:
            lg.info(f'insert_row:{sql}')
            return self.cursor.execute(sql, row)
        except Exception as err:
            lg.error(f'insert_row:{str(err)}:{sql}:{row}')

    def update_row(self, table, _id, *args):
        column_names = []

        data = args[0]
        for name in self.get_columns_metadata(table):
            column_names.append(name[3])

        parts = ''
        sql = f'UPDATE {table} SET '
        for name in column_names[1:]:
            parts += (name + '=%s,')
        sql += (parts[:-1] + ' WHERE id=%s;')

        try:
            lg.info(f'update_row:{sql}')
            return self.cursor.execute(sql, list(data) + [_id])
        except Exception as err:
            lg.error(f'update_row:{str(err)}:{sql}')

    def update_columns(self, table, _id, columns, data):
        column_names = []

        if not isinstance(columns, list) or not isinstance(columns, tuple):
            columns = (columns, )

        if not isinstance(data, list) or not isinstance(data, tuple):
            data = (data, )

        for name in self.get_columns_metadata(table):
            column_names.append(name[3])

        sql = f'UPDATE {table} SET '
        for column, content in zip(columns, data):
            cname = column_names[column] if isinstance(column, int) else column
            sql += f'{cname}=%s,'

        sql = sql.rstrip(',') + ' WHERE id=%s;'

        try:
            lg.info(f'update_row:{sql}')
            return self.cursor.execute(sql, list(data) + [_id])
        except Exception as err:
            lg.error(f'update_row:{str(err)}:{sql}')

    def delete_row(self, table, _id):
        sql = f'DELETE FROM {table} WHERE id = %s;'
        try:
            lg.info(f'delete_row:{sql}')
            return self.cursor.execute(sql, (_id,))
        except Exception as err:
            lg.error(f'delete_row:{str(err)}:{sql}')

    def get_databases(self):
        sql = 'SHOW DATABASES;'
        try:
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
            if rows:
                return tuple([i[0] for i in rows])
        except Exception as err:
            lg.error(f'get_databases:{str(err)}:{sql}')

    def get_tables(self, **kwargs):
        database = kwargs.get('database', self.db_name)

        sql = "SHOW TABLES;"

        try:
            if database == self.db_name:
                self.cursor.execute(sql)
                rows = self.cursor.fetchall()
            else:
                db = self.db_name
                self.use(database)
                self.cursor.execute(sql)
                rows = self.cursor.fetchall()
                self.use(db)
            if rows:
                return tuple([i[0] for i in rows])
        except Exception as err:
            lg.error(f'get_tables:{str(err)}:{sql}')

    def get_column_metadata(self, table, column, **kwargs):
        database = kwargs.get('database', self.db_name)

        sql = f"SELECT * FROM information_schema.COLUMNS WHERE " \
              f"TABLE_SCHEMA='{database}' AND TABLE_NAME='{table}' AND COLUMN_NAME='{column}';"
        try:
            if database == self.db_name:
                self.cursor.execute(sql)
                rows = self.cursor.fetchall()
            else:
                db = self.db_name
                self.use(database)
                self.cursor.execute(sql)
                rows = self.cursor.fetchall()
                self.use(db)

            if rows:
                return tuple(rows)
        except Exception as err:
            lg.error(f'get_databases:{str(err)}:{sql}')

    def get_columns_metadata(self, table, **kwargs):
        database = kwargs.get('database', self.db_name)

        sql = f"SELECT * FROM information_schema.COLUMNS WHERE " \
              f"TABLE_SCHEMA='{database}' AND " \
              f"TABLE_NAME = '{table}';"
        try:
            if database == self.db_name:
                self.cursor.execute(sql)
                rows = self.cursor.fetchall()
            else:
                db = self.db_name
                self.use(database)
                self.cursor.execute(sql)
                rows = self.cursor.fetchall()
                self.use(db)
            if rows:
                return tuple(rows)
        except Exception as err:
            lg.error(f'get_columns_metadata:{str(err)}:{sql}')

    #############################################

    def set_autocommit(self, **kwargs):
        sql = f'SET AUTOCOMMIT = {kwargs.get("autocommit", True)};'

        try:
            lg.info(f'set_autocommit:{sql}')
            return self.cursor.execute(sql)
        except Exception as err:
            lg.error(f'set_autocommit:{str(err)}:{sql}')

    def get_table_status(self, table=None, **kwargs):
        database = kwargs.get('database', self.db_name)

        sql = "SHOW TABLE STATUS"
        if table:
            sql += f" WHERE Name='{table}'"

        if database or table:
            try:
                if database == self.db_name:
                    self.cursor.execute(sql)
                    rows = self.cursor.fetchall()
                else:
                    db = self.db_name
                    self.use(database)
                    self.cursor.execute(sql)
                    rows = self.cursor.fetchall()
                    self.use(db)
                if rows:
                    return rows
            except Exception as err:
                lg.error(f'get_table_status:{str(err)}:{sql}')

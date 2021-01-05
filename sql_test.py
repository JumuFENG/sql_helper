# Python 3
# -*- coding: utf-8 -*-

from sql_helper import SqlHelper
import json

def load_db_config():
    with open('db.config', 'r', encoding='utf-8') as fp:
        db_config = json.load(fp=fp)
        return db_config

if __name__ == '__main__':
    db_config = load_db_config()
    db = SqlHelper(db_config['Host'], db_config['User'], db_config['Password'], database=db_config['DB'])
    t = db.is_exist_table('ssqq')
    print(t)

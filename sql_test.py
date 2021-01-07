# Python 3
# -*- coding: utf-8 -*-

from creator import create_sqlhelper

if __name__ == '__main__':
    db = create_sqlhelper('db.config')
    t = db.is_exist_table('ssqq')
    print(t)

# Python 3
# -*- coding: utf-8 -*-

import os
import json
from getpass import getpass
from base64 import b64encode,b64decode
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

from .sql_helper import SqlHelper

def _encrypt_pwd(config):
    key = get_random_bytes(16)
    cipher = AES.new(key, AES.MODE_CFB)
    ct_bytes = cipher.encrypt(config['Password'].encode('utf-8'))
    config_en = {k:v for k,v in config.items()}
    config_en['iv'] = b64encode(cipher.iv).decode('utf-8')
    config_en['Password'] = b64encode(ct_bytes).decode('utf-8')
    config_en['key'] = b64encode(key).decode('utf-8')
    config_en['encrypted'] = True
    return config_en

def _decrypt_pwd(config):
    cipher = AES.new(b64decode(config['key']), AES.MODE_CFB, iv=b64decode(config['iv']))
    config['Password'] = cipher.decrypt(b64decode(config['Password'])).decode('utf-8')
    config['encrypted'] = False
    return config

def _load_db_config(conf_file):
    if not os.path.isfile(conf_file):
        return
    with open(conf_file, 'r', encoding='utf-8') as fp:
        config = json.load(fp=fp)
        if config['encrypted']:
            config = _decrypt_pwd(config)
        else:
            _save_db_config(conf_file, config)
        return config

def _save_db_config(conf_file, config):
    if not config['encrypted']:
        config_en = _encrypt_pwd(config)
    with open(conf_file, "w") as fp:
        json.dump(obj=config_en, fp=fp, ensure_ascii=False, indent=4)

def _gather_db_config(conf_file):
    db_config_items = ['Host', 'Port', 'Schema', 'Charset', 'User']
    config = {}
    print('Input your MySQL configurations >>>\n')
    for item in db_config_items:
        _input = input('{:^10}: '.format(item)).strip()
        _input = int(_input) if item == 'Port' and _input else _input
        if item == 'Charset':
            config[item] = _input if _input else 'utf8mb4'
        elif item == 'Host':
            config[item] = _input if _input else 'localhost'
        elif item == 'Port':
            config[item] = _input if _input else 3306
        elif item == 'Schema':
            config[item] = _input if _input else 'world'
        elif item == 'User':
            config[item] = _input if _input else 'root'
    print('=============>')
    config['Password'] = getpass('Password for User %s: ' % (config['User']))
    config['encrypted'] = False
    _save_db_config(conf_file, config)
    return config

def create_sqlhelper(conf_file):
    config = _load_db_config(conf_file)
    if config is None:
        config = _gather_db_config(conf_file)

    return SqlHelper(config['Host'], config['User'], config['Password'], database=config['Schema'], charset=config['Charset'])

if __name__ == '__main__':
    db = create_sqlhelper('db.config')
    t = db.is_exist_table('ssqq')
    print(t)

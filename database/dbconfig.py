#!/usr/bin/python
import sys
sys.path.insert(1, 'C:\Projects\colombiadata\Simulations')

from configparser import ConfigParser
from collections import namedtuple
import os

import settings

print(__file__)

# Need to change absolute path
def dbconfig(filename=os.path.join(settings.BASE_DIR,'database','database.ini'), section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def namedtuple_fetchall(cursor):
    """Return all rows from a cursor as a namedtuple"""
    desc = cursor.description
    nt_result = namedtuple('Result', [col[0] for col in desc])

    return [nt_result(*row) for row in cursor.fetchall()]

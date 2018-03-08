#!/usr/bin/env python
#!/bin/env python

import os
import os.path
import M6.Common.Default as Default
from M6.Common.DB import Backend
import types
from M6.Common.Protocol.DTDClient import Client as DTDClient

CREATE_TABLE_QUERY = {}
SYSTEM_QUERY = {}
default_hash_mod = 5

PRAGMA_COMMAND = []
PRAGMA_COMMAND.append("PRAGMA page_size = %s;" % Default.PAGE_SIZE)
PRAGMA_COMMAND.append("PRAGMA auto_vacuum = 1;")
if Default.ZIP_MODE and Default.JOURNAL_MODE == 'WAL':
    PRAGMA_COMMAND.append("PRAGMA zipvfs_journal_mode = %s;" % Default.JOURNAL_MODE)
else:
    PRAGMA_COMMAND.append("PRAGMA journal_mode = %s;" % Default.JOURNAL_MODE)


CREATE_TABLE_QUERY['SYS_HASHMOD_INFO'] = """
CREATE TABLE SYS_HASHMOD_INFO (
    TABLE_NAME TEXT,
    HASH_MOD_VALUE NUMBER,
    LOCK TEXT,

    PRIMARY KEY (TABLE_NAME)
);
"""

SYSTEM_QUERY['INSERT_TABLE_INFO_STRING'] = """
INSERT INTO SYS_TABLE_INFO (
    TABLE_NAME,
    TABLE_REALNAME,
    DB_NAME,
    DB_REALNAME,
    SCOPE,
    SQL_SCRIPT,
    RAM_EXP_TIME,
    SSD_EXP_TIME,
    DSK_EXP_TIME,
    KEY_STRING,
    PARTITION_RANGE,
    ZIP_OPTION
)
VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d, '%s', %d, '%s');"""

SYSTEM_QUERY['INSERT_HASHMOD_INFO_STRING_LOCAL'] = """
    INSERT INTO SYS_HASHMOD_INFO (
        TABLE_NAME,
        HASH_MOD_VALUE,
        LOCK
    )
    VALUES ('%s', %d, '%s');"""


def _execute_query(table_name, query_set, is_select=False):
    try:
        file_path = "%s/%s.DAT" % (Default.M6_MASTER_DATA_DIR, table_name)
        bd = Backend([file_path])
        cur = bd.GetConnection().cursor()
        for item in query_set:
            if item == "PRAGMA":
                for cmd in PRAGMA_COMMAND:
                    cur.execute(cmd)
            else:
                if type(item) == types.StringType:
                    cur.execute(item)
                else:
                    for q in item:
                        cur.execute(q)
        if is_select:
            data = cur.fetchall()
            return data
    except Exception, e:
        print str(e)
        return
    finally:
        bd.Disconnect()
    return "+ OK\r\n"


def _create_global_sys(table_name):
    print _execute_query(table_name, ["PRAGMA", CREATE_TABLE_QUERY[table_name]])
    print _execute_query("SYS_TABLE_INFO", [SYSTEM_QUERY['INSERT_TABLE_INFO_STRING'] % ( table_name, table_name, 'SYS', 'SYS', 'SYSTEM', CREATE_TABLE_QUERY[table_name], 0, 0, 0, '', 0, 'ZIPFALSE' )])

def sync_table(table_name):
    try:
        dtd_param = "[%s,None,None]" % table_name
        dtd_conn = DTDClient()
        dtd_conn.Connect()
        ret = dtd_conn.COPY_ALL(dtd_param, dtd_param)
        dtd_conn.Close()
    except Exception, e:
        print str(e)
    return ret

if __name__ == "__main__":
    _create_global_sys("SYS_HASHMOD_INFO")
    local_table_list =  _execute_query("SYS_TABLE_INFO", ["SELECT TABLE_NAME FROM SYS_TABLE_INFO WHERE SCOPE = 'LOCAL'"], is_select = True)
    
    for item in local_table_list:
        table_name = item[0]
        print table_name
        print _execute_query("SYS_HASHMOD_INFO", [SYSTEM_QUERY['INSERT_HASHMOD_INFO_STRING_LOCAL'] % ( table_name, default_hash_mod, '0' )])
    
    sync_table("SYS_TABLE_INFO")
    sync_table("SYS_HASHMOD_INFO")


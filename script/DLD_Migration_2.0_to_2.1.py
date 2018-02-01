#!/bin/env python
# -*- coding: utf-8 -*-

import M6.Common.Default as Default
from M6.Common.DB import Backend
import M6.Bin.Admin.DatabaseInitQuery as DIQ
import hashlib
import os
import types
import time

def migration_sys_table_location(path):
    data = []
    for base, dirs, files in os.walk(path):
        for _file in files:
            is_fine = True
            if ".DAT" in _file.upper() and ("T" in _file.upper() or "SYS" in _file.upper()): 
                file_path = base + '/' + _file

                try:
                    bd = Backend([file_path])
                    conn = bd.GetConnection()
                    cursor = conn.cursor()
                    cursor.execute("select * from sys_table_location")
                    data = cursor.fetchall()
                except:
                    is_fine = False
                finally:
                    try:
                        conn.close()
                    except:
                        pass
                if is_fine:
                    new_dir = make_new_dir(base, _file)
                    if new_dir:
                        print new_dir
                        excute_hash_mod_split(new_dir, _file, data)

def make_new_dir(base, _file):
    table_id = _file.split(".")[0]
    if "T" in table_id.upper() or "SYS" in table_id.upper():
        new_dir = base + '/' + table_id
        if not os.path.exists(new_dir):
            try:
                os.mkdir(new_dir)
            except Exception, e:
                print "-ERR : ", str(e)
        return new_dir
    return None

def excute_hash_mod_split(new_dir, _file, data):
    hash_mod_num = Default.HASH_MOD_VALUE
    query_list = ["PRAGMA", DIQ.CREATE_TABLE_QUERY['SYS_TABLE_LOCATION'], DIQ.CREATE_INDEX_QUERY['SYS_TABLE_LOCATION']]
    for hash_mod_val in range(hash_mod_num):
        file_path = new_dir + "/%s.DAT" % hash_mod_val
        bd = Backend([file_path])
        conn = bd.GetConnection()
        cur = conn.cursor()
        for item in query_list:
            try:
                if item == "PRAGMA":
                    for cmd in DIQ.PRAGMA_COMMAND:
                        cur.execute(cmd)
                else:
                    if type(item) == types.StringType:
                        cur.execute(item)
                    else:
                        for q in item:
                            cur.execute(q)
            except Exception, e:
                print str(e)
                pass
        bd.Disconnect()
    _split(new_dir, _file, data)

def _split(new_dir, _file, data):
    table = _file.split(".")[0]
    for item in data:
        table_key = item[0]
        table_partition = item[1]
        node_id = item[2]
        status = item[3]
        hash_value = hash(table + table_key + table_partition)
        mod_value = hash_value % Default.HASH_MOD_VALUE
        
        INSERT_SQL = """
        insert into 
        sys_table_location 
        (table_key, table_partition, node_id, status, revision, size) 
        values 
        ('%s', '%s', %s, '%s', %s, %s);
        """

        file_path = new_dir + "/%s.DAT" % mod_value
        try:
            bd = Backend([file_path])
            conn = bd.GetConnection()
            cur = conn.cursor()
            cur.execute(INSERT_SQL % (table_key, table_partition, node_id, status, 1, 0))
            conn.commit()
        except Exception, e:
            print e
            pass
        finally:
            try:
                conn.close()
            except:
                pass


if __name__ == "__main__":
    s_time = time.time()
    sys_table_location_path = "./SYS_TABLE_LOCATION" 
    migration_sys_table_location(sys_table_location_path)
    e_time = time.time()
    print e_time - s_time

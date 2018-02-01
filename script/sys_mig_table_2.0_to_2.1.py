import M6.Common.Default as Default
import sqlite3
import os
import shutil


SYS_TABLE_INFO = '%s/SYS_TABLE_INFO.DAT' % Default.M6_MASTER_DATA_DIR
SYS_TABLE_REALNAME = '%s/SYS_TABLE_REALNAME.DAT' % Default.M6_MASTER_DATA_DIR
SYS_DB_INFO = '%s/SYS_DB_INFO.DAT' % Default.M6_MASTER_DATA_DIR
#SYS_TABLE_INFO = 'SYS_TABLE_INFO.DAT'
#SYS_TABLE_REALNAME = 'SYS_TABLE_REALNAME.DAT'
#SYS_DB_INFO = 'SYS_DB_INFO.DAT'

try:
    os.makedirs('./sys_backup')
except:
    pass

shutil.copy2(SYS_TABLE_INFO, './sys_backup/SYS_TABLE_INFO.DAT')
shutil.copy2(SYS_TABLE_REALNAME, './sys_backup/SYS_TABLE_REALNAME.DAT')
shutil.copy2(SYS_DB_INFO, './sys_backup/SYS_DB_INFO.DAT')


if 1:
    # SYS_TABLE_INFO add column 
    conn = sqlite3.connect(SYS_TABLE_INFO)
    cur = conn.cursor()

    cur.execute('ALTER TABLE SYS_TABLE_INFO ADD COLUMN TABLE_REALNAME TEXT')
    cur.execute('ALTER TABLE SYS_TABLE_INFO ADD COLUMN DB_NAME TEXT')
    cur.execute('ALTER TABLE SYS_TABLE_INFO ADD COLUMN DB_REALNAME TEXT')

    conn.commit()
    conn.close()


# SYS_TABLE_INFO update
conn = sqlite3.connect(SYS_TABLE_INFO)
cur = conn.cursor()

cur.execute("ATTACH DATABASE '%s' as SYS_DB_INFO" % SYS_DB_INFO)
cur.execute("ATTACH DATABASE '%s' as SYS_TABLE_REALNAME" % SYS_TABLE_REALNAME)

cur.execute("""
select
    TR.table_name,
    TR.table_realname,
    DI.db_name,
    DI.db_realname

from
    SYS_TABLE_INFO TI,
    SYS_TABLE_REALNAME TR,
    SYS_DB_INFO DI
where 1=1
    AND TI.TABLE_NAME = TR.TABLE_NAME
    AND TR.DB_NAME = DI.DB_NAME
;
""")

table_name_info = cur.fetchall()
conn.commit()
conn.close()

conn = sqlite3.connect(SYS_TABLE_INFO)
cur = conn.cursor()
for table_name, table_realname, db_name, db_realname in table_name_info:
    cur.execute("""
        update
            sys_table_info
        set
            table_realname = '%s',
            db_name = '%s',
            db_realname = '%s'
        where
            table_name = '%s'
        ;
    """ % (table_realname, db_name, db_realname, table_name))

cur.execute("""
        update
            sys_table_info
        set
            table_realname = table_name,
            db_name = 'SYS',
            db_realname = 'SYS'
        where
            table_name not like 'T%'
            and table_name != '$DEFAULT_TABLE_SETTING'
        ;
    """)
conn.commit()
conn.close()


# SYS_TABLE_INFO add index
if 1:
    conn = sqlite3.connect(SYS_TABLE_INFO)
    cur = conn.cursor()

    cur.execute('CREATE UNIQUE INDEX SYS_TABLE_INFO_IDX2 on SYS_TABLE_INFO (TABLE_REALNAME, DB_REALNAME);')

    conn.commit()
    conn.close()



# SYS_DB_INFO update
conn = sqlite3.connect(SYS_DB_INFO)
cur = conn.cursor()

cur.execute("""
    update
        sys_db_info
    set
        db_name = db_realname
    where
        db_realname in ('SYS', 'DEFAULT')
    ;
""")
conn.commit()
conn.close()


# SYS_TABLE_INFO update

conn = sqlite3.connect(SYS_TABLE_INFO)
cur = conn.cursor()

cur.execute("""
    update 
        sys_table_info
    set
        sql_script = ' 
create table SYS_TABLE_INFO (
    TABLE_ID         INTEGER PRIMARY KEY AUTOINCREMENT,
    TABLE_NAME       TEXT,
    SCOPE            TEXT,
    RAM_EXP_TIME     NUMBER,
    SSD_EXP_TIME     NUMBER,
    DSK_EXP_TIME     NUMBER,
    KEY_STRING       TEXT,
    PARTITION_STRING TEXT,
    PARTITION_RANGE  NUMBER,
    ZIP_OPTION       TEXT,
    RAM_OPTION       TEXT,
    SSD_OPTION       TEXT,
    DSK_OPTION       TEXT,
    SQL_SCRIPT       TEXT,
    TABLE_REALNAME   TEXT,
    DB_REALNAME      TEXT,
    DB_NAME          TEXT,
    DB_REALNAME      TEXT
);
'
    where table_name = 'SYS_TABLE_INFO'
""")
conn.commit()
conn.close()

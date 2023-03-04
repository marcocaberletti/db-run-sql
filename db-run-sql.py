#!/usr/bin/env python3

import os
import MySQLdb
import logging
from enum import Enum

class DML_STATUS(Enum):
    SUCCESS = 'SUCCESS'
    FAILURE = 'FAILURE'

dml_folder = 'raw/dml'
meta_table_name = 'RawScriptsMeta'

def run():
    log = logging.getLogger()
    log.setLevel(logging.DEBUG)
    format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(format)
    log.addHandler(ch)

    logging.info("Starting...")

    dbhost = get_env_variable("DBHOST")
    dbuser = get_env_variable("DBUSER")
    dbpassword = get_env_variable("DBPASSWORD")
    dbname = get_env_variable("DBNAME")

    logging.info(f"Try connection to {dbhost}")
    db = MySQLdb.connect(host=dbhost, user=dbuser, passwd=dbpassword, db=dbname)
    logging.info(f"Connected to host:[{dbhost}] user:[{dbuser}] db:[{dbname}]")
    if db.get_autocommit():
        db.autocommit(False)
        logging.info("Disable DB connection transaction autocommit.")

    create_meta_table(db)
    dml_executed = readMetaTable(db)

    dml_file_list = sorted(os.listdir(dml_folder))

    for dml in dml_file_list:
        logging.debug(f"DML name: {dml}")

        # check if already executed
        if dml in dml_executed:
            logging.debug(f"DML {dml} already executed. Skip.")
            continue

        # read file
        fd = open(os.path.join(dml_folder, dml), 'r')
        sqlFile = fd.read()
        fd.close()
        logging.debug(f"DML to execute:\n{sqlFile}")

        # execute
        cur = db.cursor()
        sqlCommands = sqlFile.split(';')
        for command in sqlCommands:
            if(command.strip()):
                try:
                    logging.debug(command)
                    cur.execute(command)
                except Exception as ex:
                    logging.error(f"Error executing command: {ex}")
                    db.rollback()
                    updateMetaTable(db, dml, DML_STATUS.FAILURE.name)
                    exit(1);

        logging.info(f"DML {dml} executed")
        updateMetaTable(db, dml, DML_STATUS.SUCCESS.name)


def get_env_variable(varname):
    value = os.getenv(varname)
    if not value:
        print(f"{varname} env variable not defined")
        exit(1)
    return value

def create_meta_table(db):
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{meta_table_name}` (
        `name` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
        `executedAt` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        `status` varchar(20) NOT NULL,
        PRIMARY KEY (`name`),
        UNIQUE KEY `name` (`name`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8_unicode_ci;
    """
    cur = db.cursor()
    cur.execute(sql)

def readMetaTable(db):
    query = f"SELECT name FROM {meta_table_name};"
    logging.debug(f"Query to execute: {query}")
    cur = db.cursor()
    cur.execute(query)
    data = cur.fetchall()
    values = []
    for elem in data:
        values.append(elem[0])
    logging.debug(f"List of DMLs in the Meta table: {values}")
    return values

def updateMetaTable(db, name, status):
    query = f"INSERT INTO {meta_table_name}(name,status) VALUES('{name}','{status}');"
    logging.debug(f"Update Meta Table: {query}")
    cur =  db.cursor()
    cur.execute(query)
    db.commit()

if __name__ == "__main__":
    run()


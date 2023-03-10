import os
import sys
global cursor
global conn
global hostname
global username
global password
global database

base_path = tmp_global_obj["basepath"]
cur_path = base_path + 'modules' + os.sep + \
    'PostgreSQL' + os.sep + 'libs' + os.sep


cur_path_x64 = os.path.join(cur_path, 'Windows' + os.sep +  'x64' + os.sep)
cur_path_x86 = os.path.join(cur_path, 'Windows' + os.sep +  'x86' + os.sep)

if sys.maxsize > 2**32 and cur_path_x64 not in sys.path:
    sys.path.append(cur_path_x64)

if sys.maxsize < 2**32 and cur_path_x86 not in sys.path:
    sys.path.append(cur_path_x86)

import platform

platform_ = platform.system()

module = GetParams('module')

if module == "connect":
    hostname = GetParams('hostname')
    username = GetParams('username')
    password = GetParams('password')
    database = GetParams('database')
    dbport = GetParams("dbport")
    if not dbport:
        dbport = 5432
    var_ = GetParams('var_')

    if "indows" in platform_:
        import psycopg2

        status = False

        try:
            conn = psycopg2.connect(host=hostname, user=username, password=password, dbname=database, port=dbport)
            cursor = conn.cursor()
            status = True
        except Exception as e:
            PrintException()
            SetVar(var_, status)
            raise e

        SetVar(var_, status)

    else:
        import pgdb

        params = {'host': hostname, 'database': database, 'user': username, 'password': password, 'port': dbport}

        status = False
        try:
            conn = pgdb.connect(**params)
            cursor = conn.cursor()
            status = True

        except Exception as e:
            PrintException()
            SetVar(var_, status)
            raise e

        SetVar(var_, status)


if module == "execute":
    query_ = GetParams('query_')
    result = []
    var_ = GetParams('var_')

    try:
        query = query_
        cursor.execute(query)

        if query.lower().startswith("select"):

            col = [d[0] for d in cursor.description]
            rows = cursor.fetchall()

            for row in rows:
                ob_ = {}
                t = 0
                for r in row:
                    ob_[col[t]] = str(r) + ""
                    t = t + 1
                result.append(ob_)

        elif query.lower().startswith("insert") and "returning" in query.lower():

            col = [d[0] for d in cursor.description]
            rows = cursor.fetchall()

            for row in rows:
                ob_ = {}
                t = 0
                for r in row:
                    ob_[col[t]] = str(r) + ""
                    t = t + 1
                result.append(ob_)

            conn.commit()
        else:
            conn.commit()
            result = "True"

        SetVar(var_, result)
    except Exception as e:
        PrintException()
        raise e


if module == "closeConn":
    conn.close()
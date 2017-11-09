import sqlite3


def get_single_row(db_path, query, parse_decl_types=False):
    """Get and return a single row from the db"""
    d = {}
    conn = None
    try:
        if parse_decl_types:
            conn = sqlite3.connect(
                db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        else:
            conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(query)
        r = cur.fetchone()
        if r and r.keys():
            d = {}
            for key in r.keys():
                d[key] = r[key]
    except Exception, e:
        return None, 'Error reading from single row from database : %s' % str(e)
    else:
        return d, None
    finally:
        if conn:
            conn.close()
    return d


def get_multiple_rows(db_path, query, parse_decl_types=False):
    """Get and return multiple rows from the db."""
    l = []
    conn = None
    try:
        if parse_decl_types:
            conn = sqlite3.connect(
                db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        else:
            conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        if rows:
            for row in rows:
                d = {}
                for key in row.keys():
                    d[key] = row[key]
                l.append(d)
    except Exception, e:
        return None, 'Error reading multiple rows from database : %s' % str(e)
    else:
        return l, None
    finally:
        if conn:
            conn.close()


def execute_iud(db_path, command_list, get_rowid=False):
    """Execute a set of insert/update/delete commands into the db.

    command_list is a list of commands to execute in a transaction. Each
    command can have just the command or command with parameters
    """
    conn = None
    rowid = -1
    try:
        # print command_list
        # print db_path
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute('PRAGMA journal_mode=TRUNCATE')
        cur.execute('PRAGMA foreign_keys=ON')
        for command in command_list:
            # print command
            if len(command) > 1:
                cur.execute(command[0], command[1])
            else:
                cur.execute(command[0])
        if get_rowid:
            rowid = cur.lastrowid
        cur.close()
        conn.commit()
    except Exception, e:
        return -1, 'Error inserting/updating database : %s' % str(e)
    else:
        return rowid, None
    finally:
        if conn:
            conn.close()

# vim: tabstop=8 softtabstop=0 expandtab ai shiftwidth=4 smarttab

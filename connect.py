from random import randint
import multiprocessing
# https://pymotw.com/2/multiprocessing/basics.html
import MySQLdb as mdb
import time

from utility import flush_print

# examples: http://zetcode.com/db/mysqlpython/

def conn(instance):
    if instance.passwd:
        return mdb.connect(host=instance.host,
                           port=instance.port,
                           user=instance.user,
                           passwd=instance.passwd
                          )
    return mdb.connect(host=instance.host,
                       port=instance.port,
                       user=instance.user
                      )

def execute(conn=None, stmt="", verbose=False):
    """
    Returns the rows from a stmt
    """
    cur = conn.cursor(mdb.cursors.DictCursor)
    cur.execute(stmt)

    return cur.fetchall()

def run_sql(instance, stmt="", verbose=False):
    # DEBUG remove after testing complete
    # time.sleep(randint(0,5))

    conn = instance.get_connection()

    rows = execute(conn=conn, stmt=stmt, verbose=verbose)
    for row in rows:
        print instance.name,row

def execute_in_parallel(instance_list, stmt, timeout=10):
    """
    Calls the run_sql function in parallel.
    todo:
         o need to be able to poll for completion rather than waiting
           until the timeout is reached. 
         o may need to be able to throttle parallelism.
    """

    jobs = dict()
    for instance in instance_list:
        p = multiprocessing.Process(target=run_sql, args=(instance,stmt,True))
        jobs[instance] = p
        p.start()
        flush_print("{instance_name}:stmt".format(instance_name=instance.name))

    time_waited = 0
    while [job for job in jobs.values() if job.is_alive()]:
        if time_waited >= timeout:
            print "timeout reached"
            break
        time.sleep(1)
        time_waited += 1

    for instance,job in jobs.iteritems():
        if job.is_alive():
            flush_print("Warning: terminated job for {0}".format(instance.name))
            job.terminate()


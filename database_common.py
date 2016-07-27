import socket
import time

def show_master_status(instance):
    """
    Return a dict of show master status info or None
    """
    stmt = "show master status"
    instance.get_connection()
    instance.execute_stmt(connection=instance.connection, stmt=stmt)

    if instance.rows:
        return instance.rows[0]


def show_slave_status(instance):
    """
    Return a dict of show slave status info or None
    """
    stmt = "show slave status"
    instance.get_connection()
    instance.execute_stmt(connection=instance.connection, stmt=stmt)

    if instance.rows:
        return instance.rows[0]


def show_global_variables(instance):
    """
    Returns a dict of the global variables for the instance.
    """
    stmt = "SELECT variable_name, variable_value FROM information_schema.global_variables"
    instance.get_connection()
    instance.execute_stmt(connection=instance.connection, stmt=stmt)

    variable_dict = dict()
    for row in instance.rows:
        variable_dict[row["variable_name"].lower()] = row["variable_value"]

    return variable_dict


def is_db_availble(instance):
    """
    Return 
    """
    stmt = "select 1 as result"
    try:
        instance.get_connection()
        instance.execute_stmt(connection=instance.connection, stmt=stmt)
        for row in instance.rows:
            result = row["result"]
    except Exception as e:
        print "Error: {0}".format(e)
        return
    return result


def is_read_only(instance):
    """
    Quick function for testing whether an instance is read only or not.
    """
    if show_global_variables(instance)["read_only"] == "ON":
        return True

def is_slave_running(instance):
    """
    Quick function for testing whether an instance is replicating or not. Note: if 
    either the IO or SQL thread is running this function returns True.
    """
    slave_status = show_slave_status(instance)
    if slave_status:
        if (slave_status["Slave_IO_Running"] == "Yes" or slave_status["Slave_SQL_Running"] == "Yes"):
            return True

def change_master(master_instance="", slave_instance="", force=False):
    """
    Perform the change master command.
    Todo:
         o Check that master is not actively slaving
         o Confirm no writes to the master binary logs for N seconds

    """
    
    if not is_read_only(master_instance) and not force:
        print "Error: master is writeable."
        return 1

    if not is_read_only(slave_instance):
        print "Error: slave is writeable."
        return 1

    master_status = show_master_status(master_instance)
    # {"Position": 402L, "Executed_Gtid_Set": "", "Binlog_Do_DB": "", "File": "mysql-bin.000001", "Binlog_Ignore_DB": ""}
    if master_status:
        if master_status.get("Excuted_Gtid_set"):
            print "Error: change_master does not currently support gtids."
            return 1
    else:
        print "Error: master_status returned no values. Check that binary logging is enabled."
        return 1

    slave_status = show_slave_status(slave_instance)
    if slave_status:
        if (slave_status["Slave_IO_Running"] == "Yes" or slave_status["Slave_SQL_Running"] == "Yes"):
            if force:
                stop_slave(slave_instance)
            else:
                print "Error: slave threads found running. Use force if you really want to change master for this instance."
                return 1
    # todo: need to pass user, password, etc 
    stmt = """CHANGE MASTER TO master_host = \"{master_host}\",
                               master_user = \"repl\",
                               master_password = \"r3pl\",
                               master_port = {master_port},
                               master_log_file = \"{master_log_file}\",
                               master_log_pos = {master_log_pos}
           """.format(master_host=master_instance.get_ip(),
                      master_port=master_instance.port,
                      master_log_file=master_status['File'],
                      master_log_pos=master_status['Position'])

    print "{stmt} for slave {slave_instance_name}".format(stmt=" ".join(stmt.split()), slave_instance_name=slave_instance.name)

    #==============================================================================
    # todo: fix the above so it doesnt output the following
    # CHANGE MASTER TO master_host = "dbtest01", master_user = "repl", master_password = "XXXX", master_port = 3306, master_log_file = "mysql-bin.000002", master_log_pos = 120 for slave dbtest02:3306
    # /home/dturner/dba/admin/dev/drelib/connect.py:28: Warning: Sending passwords in plain text without SSL/TLS is extremely insecure.
    #   cur.execute(stmt)
    # /home/dturner/dba/admin/dev/drelib/connect.py:28: Warning: Storing MySQL user name or password information in the master info repository is not secure and is therefore not recommended. Please consider using the USER and PASSWORD connection options for START SLAVE; see the 'START SLAVE Syntax' in the MySQL Manual for more information.
    #   cur.execute(stmt)
    #==============================================================================

    slave_instance.get_connection()
    slave_instance.execute_stmt(connection=slave_instance.connection, stmt=stmt)
    stmt = 'START SLAVE'
    slave_instance.execute_stmt(connection=slave_instance.connection, stmt=stmt)
    time.sleep(1)
    print is_slave_running(slave_instance)

    print master_status


def set_read_only(instance):
    """
    Set the instance to read only
    """
    stmt = "set global read_only=1"
    instance.get_connection()
    instance.execute_stmt(connection=instance.connection, stmt=stmt)
    if not is_read_only(instance):
        print "Error: unable to set the instance to read only."
        return 1


def set_writeable(instance):
    """
    Set the instance to writeable / read_only off
    """
    stmt = "set global read_only=0"
    instance.get_connection()
    instance.execute_stmt(connection=instance.connection, stmt=stmt)
    if is_read_only(instance):
        print "Error: unable set the instance to writeable."
        return 1

def reset_master(instance):
    """
    Reset binary logs on the master instance.
    todo: check on impact when lots of logs
          require force be set when slaves are actively replicating
          from the instance.
    """
    stmt = "reset master"
    instance.get_connection()
    try:
        instance.execute_stmt(connection=instance.connection, stmt=stmt)
    except Exception as e:
        print "Error: problem reseting master."
        print e
        return 1

def reset_slave_all(instance):
    """
    A slightly modified way of reseting slave all on a host to ensure that
    slave info is written to the .err file before wiping slave info. This
    ensures a slave can be remastered correctly if it was mistakenly reset.

    """
    instance.get_connection()
    try:
        stmt = "CHANGE MASTER TO master_host='NA'"
        instance.execute_stmt(connection=instance.connection, stmt=stmt)
        stmt = "RESET SLAVE ALL"
        instance.execute_stmt(connection=instance.connection, stmt=stmt)
    except Exception as e:
        print "Error: reseting slave all."
        print e
        return 1

def stop_slave(instance):
    """
    Stop replication on the given instance
    """
    stmt = "stop slave"
    instance.get_connection()
    instance.execute_stmt(connection=instance.connection, stmt=stmt)
    if is_slave_running(instance):
        print "Error: unable stop slave replication."
        return 1

def start_slave(instance):
    """
    Start replication on the given instance
    """
    stmt = "start slave"
    instance.get_connection()
    instance.execute_stmt(connection=instance.connection, stmt=stmt)
    if is_slave_running(instance):
        print "Error: unable start slave replication."
        return 1

def is_master_of_slave(master_instance, slave_instance):
    """
    Returns true if the master host and port matches that of the slave. 
    """
    slave_status = show_slave_status(slave_instance)
    # added this in case master has no slave status info
    if not slave_status:
        return False
    if ("{0}:{1}".format(master_instance.get_ip(), master_instance.port) ==
        "{0}:{1}".format(socket.gethostbyname(slave_status['Master_Host']), slave_status['Master_Port'])):
        return True


def is_replication_in_sync(master_instance=None, slave_instance=None, retries=5, sleep_time=30):
    """
    Returns true if the master and slave are in sync
    todo: make this stuff gtid capable.
    """
    if not is_master_of_slave(master_instance, slave_instance):
        print "Error: master is not the parent to this slave."
        return

    attempts = 0 

    while attempts < retries: 
        master_status = show_master_status(master_instance)
        slave_status = show_slave_status(slave_instance)
    
        print "master {0} - {1} {2}".format(master_instance.name,
                                                       master_status['File'],
                                                       master_status['Position'])
        print "slave  {0} - {1} {2}".format(slave_instance.name, 
                                                       slave_status['Relay_Master_Log_File'],
                                                       slave_status['Exec_Master_Log_Pos'])
    
        if (master_status['File'] == slave_status['Relay_Master_Log_File'] and
           master_status['Position'] == slave_status['Exec_Master_Log_Pos']):
            print "replication sync ok"
            return 1

        attempts += 1
        print "Replication not in sync. Retrying in {0}s".format(sleep_time)
        time.sleep(sleep_time)

    print "replication sync failed"



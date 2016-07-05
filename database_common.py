
def show_master_status(instance):
    """
    Return show master status info
    """
    stmt = "show master status"
    instance.get_connection()
    instance.execute_stmt(connection=instance.connection, stmt=stmt)

    return instance.rows 

def show_slave_status(instance):
    """
    Return show slave status info
    """
    stmt = "show slave status"
    instance.get_connection()
    instance.execute_stmt(connection=instance.connection, stmt=stmt)

    return instance.rows 

def change_master(master_instance="", slave_instance="", force=False):
    """
    Perform the change master command.
    Todo:
         o confirm that the new master is in read only mode. 
    """
    pass


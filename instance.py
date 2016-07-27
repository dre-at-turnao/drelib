import re

import utility

from connect import (
        conn,
        execute,
        )
from database_common import (
        change_master,
        reset_master,
        reset_slave_all,
        start_slave,
        stop_slave,
        show_global_variables,
        )

class Instance(object):
    
    def __init__(self, name):
        if re.search(":",name):
            self.host = name.split(":")[0]
            self.port = int(name.split(":")[1])
        else:
            self.host = name
            self.port = 3306
        self.name = "{host}:{port}".format(host=self.host, port=self.port)
        self.ip = None

        self.user = "dba"
        self.passwd = "C4USAL8788"
#         self.user = "root"
#         self.passwd = ""

    def get_connection(self):
        self.connection = conn(self)
        return self.connection

    def execute_stmt(self, connection, stmt):
        self.rows = execute(conn=connection, stmt=stmt)

    def show_global_variables(self):
        return show_global_variables(instance=self)

    def get_ip(self):
        """
        Return the ip for the given instance
        """
        if not self.ip:
            self.ip = utility.get_ip(self.host)
        return self.ip

    def change_master(self, master_instance, force=False):
        return change_master(master_instance=master_instance, slave_instance=self, force=force)

    def reset_master(self):
        return reset_master(instance=self)

    def reset_slave_all(self):
        return reset_slave_all(instance=self)

    def stop_slave(self):
        return stop_slave(instance=self)

    def start_slave(self):
        return start_slave(instance=self)



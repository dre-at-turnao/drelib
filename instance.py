import re
from connect import (
        conn,
        execute,
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

        self.user = "root"
        self.password = ""

    def get_connection(self):
        self.connection = conn(self)
        return self.connection

    def execute_stmt(self, connection, stmt):
        self.rows = execute(conn=connection, stmt=stmt)



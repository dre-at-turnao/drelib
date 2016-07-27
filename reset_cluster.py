#!/usr/bin/python

import argparse
import json
import logging
import sys

from instance import Instance

def get_instance_info():
    """
    Return master and slave info for promotions
    """
    # datadir is a symbolic link to the customer's instance info directory.
    with open('../datadir/instance_info.json', 'r') as f:
        return json.load(f)


if __name__ == "__main__":

   parser = argparse.ArgumentParser()
   parser.add_argument("--execute",
                        action="store_true",
                        help="Whether to actually run this against the hosts.")

   args = parser.parse_args()

   if not args.execute:
       sys.exit("Use the --execute flag if you really want to do this.")

   instance_info = get_instance_info()

   master = Instance(instance_info['masters'][0])
   slaves = [Instance(instance_info['masters'][1])] + [Instance(slave) for slave in instance_info['slaves']]


   for inst in [master] + slaves:
       print "Stopping slave for {0}".format(inst.name)
       inst.stop_slave()

   master.reset_slave_all()
   master.reset_master()

   for slave in slaves:
       print "Changing master to {0} for {1}".format(master.name, slave.name)
       slave.change_master(master_instance=master)





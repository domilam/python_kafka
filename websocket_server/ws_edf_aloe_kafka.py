#!/usr/bin/env python3.6
import os
from action_edf_aloe_kafka import WsActionEdfAloe
from wsserver import WsServer

port = int(os.environ.get('QUAD_PORT', 9006))
# port = int(os.environ.get('QUAD_PORT', 8080))
my_server = WsServer(WsActionEdfAloe, port, path='/var/log/pyServers/edf_aloe.log')
my_server.run_forever()

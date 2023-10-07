import xmlrpc.client
import xmlrpc.server
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
from concurrent.futures import ThreadPoolExecutor

from collections import defaultdict
import concurrent.futures
from threading import Lock

import time
import random
import threading

baseAddr = "http://localhost:"
baseServerPort = 9000

class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
        pass

class FrontendRPCServer:
    # TODO: You need to implement details for these functions.
    def __init__(self):
        self.locked_keys = defaultdict(Lock)
        self.alive_servers  = dict()
        self.dead_servers = dict()
        # self.backgroung_thread_for_heartbeat = threading.Thread(target=self.heartbeat)
        # self.backgroung_thread_for_heartbeat.setDaemon(True)
        # self.backgroung_thread_for_heartbeat.start()
                
    ## put: This function routes requests from clients to proper
    ## servers that are responsible for inserting a new key-value
    ## pair or updating an existing one.
    def put_util(self, func, key, value):
        count = 0
        while count < 5:
            try:
                func(key, value)
                return
            except Exception as e:
                count += 1
                # time.sleep(0.05 * count)

        return

    def put(self, key, value):
        if key not in self.locked_keys:
            self.locked_keys[key] = Lock()
        self.locked_keys[key].acquire()
        # with ThreadPoolExecutor(16) as executor:
        #     res = []
        #     keys = list(self.alive_servers.keys())
        #     for serverId in keys:
        #         res.append(executor.submit(self.put_util, func=self.alive_servers[serverId].put, key = key, value =value))
        # concurrent.futures.wait(res, return_when=concurrent.futures.ALL_COMPLETED)
        threads = []
        keys = list(self.alive_servers.keys())
        for serverId in keys:
            thread = threading.Thread(target=self.put_util, args=(self.alive_servers[serverId].put, key, value))
            threads.append(thread)
            thread.start()
        for thread in threads:
            thread.join()
        self.locked_keys[key].release()

        # res_result = []
        # for r in res:
        #     res_result.append(str(r.result()))
        # result = "\n".join(res_result)
        return "Success"

    ## get: This function routes requests from clients to proper
    ## servers that are responsible for getting the value
    ## associated with the given key.
    '''
    Print format - key:value
    For a “get” operation, if the server doesn’t have a value for the key, the value should be “ERR_KEY”
    '''
    def get(self, key):
        # check locked key
        if self.locked_keys.get(key, None) is not None:
            while self.locked_keys[key].locked():
                time.sleep(0.0001)

        while len(self.alive_servers.keys()) > 0:
            list_alive = list(self.alive_servers.keys())
            random_serverId = random.choice(list_alive)
            try:
                get_val = self.alive_servers[random_serverId].get(key)
                return get_val
            except Exception as e:
                print("In Exception of get")
                self.heartbeat_util()

        return "ERR_NOEXIST"

    ## printKVPairs: This function routes requests to servers
    ## matched with the given serverIds.
    '''
    Please make it printed like below (newline separated, newline after the very last key value pair as well).
    Key1:Val1
    Key2:Val2
    Key3:Val3

    In the case that the server that the client is trying to connect to does not exist, the value should instead be: “ERR_NOEXIST”.
    '''
    def printKVPairs(self, serverId):
        count = 0
        while count < 3:
            try:
                resp = self.alive_servers[serverId].printKVPairs()
                return resp
            except:
                # resp = "Server {} is dead after retrying 3 times.".format(serverId)
                resp = "ERR_NOEXIST"
                count += 1
                # time.sleep(0.05*count)
        return resp

    ## addServer: This function registers a new server with the
    ## serverId to the cluster membership.
    def addServer(self, serverId):
        new_server = xmlrpc.client.ServerProxy(baseAddr + str(baseServerPort + serverId))
        server_ids = list(self.alive_servers.keys())
        if len(server_ids) != 0:
            random_server_id = random.choice(server_ids)
        flag = False
        self.alive_servers[serverId] = new_server

        # More servers exists
        if len(self.alive_servers) > 1:
            #need to copy kvs from one server to another
            try:
                kv_store = self.printKVPairs(random_server_id)
                flag = True
            except:
                return "Get K,V pair from " + str(random_server_id) + "failed."
            # try:
            self.alive_servers[serverId].deep_copy(kv_store)
            # except:
            #     return "Deep Copy of K,V pair to " + str(serverId) + "from" + str(random_server_id) + "failed."
            return "Success in creating new server " + str(serverId) + "K,V copied." + str(kv_store) + str(flag)

        return "Success in creating new server " + str(serverId) + "K,V copied."

    ## listServer: This function prints out a list of servers that
    ## are currently active/alive inside the cluster.
    def listServer(self):
        self.heartbeat_util()
        serverList = []
        for serverId, _ in self.alive_servers.items():
            serverList.append(serverId)

        if(len(serverList) == 0):
            return "ERR_NOSERVERS"
        serverList.sort()
        resp = ', '.join([str(server) for server in serverList])
        return resp
        

    ## shutdownServer: This function routes the shutdown request to
    ## a server matched with the specified serverId to let the corresponding
    ## server terminate normally.
    def shutdownServer(self, serverId):
        result = self.alive_servers[serverId].shutdownServer()
        self.alive_servers.pop(serverId, None)
        return result
    
    def heartbeat_util(self):
        alive_servers = []
        for serverId in self.dead_servers.keys():
            count = 0
            alive = False
            while count < 10:
                try:
                    self.dead_servers[serverId].heartBeat()
                    alive = True
                    count = 10
                except:
                    count += 1
                    time.sleep(0.05*count)
                
            if alive:
                alive_servers.append(serverId)
        
        for alive_server_id in alive_servers:
            self.alive_servers[alive_server_id] = self.dead_servers.pop(alive_server_id, None)
            
        
        dead_servers = []
        for serverId in self.alive_servers.keys():
            count = 0
            alive = False
            while count < 10:
                try:
                    self.alive_servers[serverId].heartBeat()
                    alive = True
                    count = 10
                except:
                    count += 1
                    time.sleep(0.05*count)
                
            if not alive:
                dead_servers.append(serverId)
        
        for dead_server_id in dead_servers:
            self.dead_servers[dead_server_id] = self.alive_servers.pop(dead_server_id, None)


    def heartbeat(self):
        while True:
            time.sleep(2)
            self.heartbeat_util()
            
    

server = SimpleThreadedXMLRPCServer(("localhost", 8001))
server.register_instance(FrontendRPCServer())

server.serve_forever()

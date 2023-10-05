import xmlrpc.client
import xmlrpc.server
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCServer
import random
import concurrent.futures
import threading
import time

kvsServers = dict()
baseAddr = "http://localhost:"
baseServerPort = 9000
alive_servers = dict()
dead_servers = dict()

class SimpleThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
        pass

class FrontendRPCServer:
    # TODO: You need to implement details for these functions.

    ## put: This function routes requests from clients to proper
    ## servers that are responsible for inserting a new key-value
    ## pair or updating an existing one.
    # alive_servers = {}
    # dead_servers = {}

    def put(self, key, value):
        for serverId, rpcHandle in alive_servers.items():
            rpcHandle.put(key, value)
        #print('Done')
        return 'Doneeee!'
    
    def get_local(self, key):
    ## get: This function routes requests from clients to proper
    ## servers that are responsible for getting the value
    ## associated with the given key.
        random_server_id = random.choice(list(alive_servers.keys()))
        # print("Random server ID: " + str(random_server_id))
        # print("Random server id object: " + self.alive_servers[random_server_id])
        value = alive_servers[random_server_id].get(key)
        return value

    def get(self, key):
        with concurrent.futures.ThreadPoolExecutor(max_workers = 16) as executor:
            future = executor.submit(self.get_local, (key))
            res = []
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                r = "[Server " + str(random_server_id) + "] Receive a get request: " + "Key = " + str(key) + " Value = " + str(result)
                res.append(r)
            #     #yield r + "[DONE]"
            # return str(res)
        concurrent.futures.wait(responses, return_when=concurrent.futures.ALL_COMPLETED)
        return res


        
        # GRPC call to random server for read
        # serverId = key % len(kvsServers)
        

    ## printKVPairs: This function routes requests to servers
    ## matched with the given serverIds.
    def printKVPairs(self, serverId):
        '''
        Please make it printed like below (newline separated).
        Key1:Val1
        Key2:Val2

        Key3:Val3
        '''

        ans = alive_servers[serverId].printKVPairs()
        #print(ans)
        return ans
        #return kvsServers[serverId].printKVPairs()

    ## addServer: This function registers a new server with the
    ## serverId to the cluster membership.
    def addServer(self, serverId):
        alive_servers[serverId] = xmlrpc.client.ServerProxy(baseAddr + str(baseServerPort + serverId))
        return "Success"

    ## listServer: This function prints out a list of servers that
    ## are currently active/alive inside the cluster.
    def listServer(self):
        serverList = []
        for serverId, rpcHandle in alive_servers.items():
            serverList.append(serverId)
        return serverList

    ## shutdownServer: This function routes the shutdown request to
    ## a server matched with the specified serverId to let the corresponding
    ## server terminate normally.
    def shutdownServer(self, serverId):
        result = alive_servers[serverId].shutdownServer()
        alive_servers.pop(serverId)
        return result

    def heartBeat(self):
        print("Done")
        for serverId, rpcHandle in alive_servers.items():
            if alive_servers[serverId].heartBeat() != "OK":
                dead_servers.put(serverId, rpcHandle)
                alive_servers.pop(serverId)
        if len(dead_servers) != 0:
            print("There are " + str(len(dead_servers)) + "dead servers.")
        return "There are " + str(len(alive_servers)) + "alive servers."

server = SimpleThreadedXMLRPCServer(("localhost", 8001))
server.register_instance(FrontendRPCServer())

server.serve_forever()

# def heartBeat(self):
#     print("Done")
#     for serverId, rpcHandle in alive_servers.items():
#         if alive_servers[serverId].heartBeat() != "OK":
#             dead_servers.put(serverId, rpcHandle)
#             alive_servers.pop(serverId)
#     if len(self.dead_servers) != 0:
#         print("There are " + str(len(dead_servers)) + "dead servers.")
#     print("There are " + str(len(alive_servers)) + "alive servers.")
#     threading.Timer(0.1, heartBeat).start()
#     return ""

# Lets always make thread for heartbeat
# print("Done")
# threading.Timer(0.1, heartBeat).start()


# counter in frontend
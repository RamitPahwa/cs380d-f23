import argparse
import xmlrpc.client
import xmlrpc.server
import concurrent.futures
import json
import copy

serverId = 0
basePort = 9000

class KVSRPCServer:
    KVStore = dict()
    # TODO: You need to implement details for these functions.

    ## put: Insert a new-key-value pair or updates an existing
    ## one with new one if the same key already exists.
    def put(self, key, value):
        # print("[Server " + str(serverId) + "] Receive a put request: " + "Key = " + str(key) + ", Val = " + str(value))
        self.KVStore[key] = value
        return "[Server " + str(serverId) + "] Receive a put request: " + "Key = " + str(key) + ", Val = " + str(value) + "Doneee"

    def get_local(self, key):
        return self.KVStore[key]

    ## get: Get the value associated with the given key.
    '''
    Print format - key:value
    For a “get” operation, if the server doesn’t have a value for the key, the value should be “ERR_KEY”
    '''
    def get(self, key):
        # with concurrent.futures.ThreadPoolExecutor(max_workers = 16) as executor:
        #     future = executor.submit(self.get_local, (key))
        #     res = []
        #     for future in concurrent.futures.as_completed(futures):
        #         result = future.result()
        #         r = "[Server " + str(random_server_id) + "] Receive a get request: " + "Key = " + str(key) + " Value = " + str(result)
        #         # yield str(result)
        #         res.append(result)
        #     # return res
        # concurrent.futures.wait(res, return_when=concurrent.futures.ALL_COMPLETED)
        # return res
        # print("[Server " + str(serverId) + "] Receive a get request: " + "Key = " + str(key))
        if(key in self.KVStore):
            resp = "[Server " + str(serverId) + "] Receive a get request: " + "Key = " + str(key) + " Value = " + str(self.KVStore[key])
        else:
            resp = "ERR_KEY"
        return resp

    ## printKVPairs: Print all the key-value pairs at this server.
    def printKVPairs(self):
        print("[Server " + str(serverId) + "] Receive a request printing all KV pairs stored in this server")
        return str(self.KVStore)
        # return "[Server " + str(serverId) + "] Receive a request printing all KV pairs stored in this server"

    ## shutdownServer: Terminate the server itself normally.
    def shutdownServer(self):
        return "[Server " + str(serverId) + "] Receive a request for a normal shutdown"

    def heartBeat(self):
        print("[Server " + str(serverId) + "] is running ..")
        return "OK"

    def parse_key_value_string(self, kv_store):
        return json.loads(kv_store)

    def deep_copy(self, kv_store):
        #self.KVStore = copy.deep_copy(parse_key_value_string(kv_store))
        self.KVStore = {key: value for key, value in kv_store.items()}
        return "Deep Copy Success"



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = '''To be added.''')

    parser.add_argument('-i', '--id', nargs=1, type=int, metavar='I',
                        help='Server id (required)', dest='serverId', required=True)

    args = parser.parse_args()

    serverId = args.serverId[0]

    server = xmlrpc.server.SimpleXMLRPCServer(("localhost", basePort + serverId))
    server.register_instance(KVSRPCServer())

    server.serve_forever()
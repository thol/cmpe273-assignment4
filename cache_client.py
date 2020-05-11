import sys
import socket

from sample_data import USERS
from server_config import NODES
from pickle_hash import serialize_GET, serialize_PUT, serialize_DELETE, lru_cache
from node_ring import NodeRing
from bloom_filter import BloomFilter 

BUFFER_SIZE = 1024
NUM_KEYS = 20 
FALSE_POSITIVE_PROBABILITY = 0.05
bloomfilter = BloomFilter(NUM_KEYS, FALSE_POSITIVE_PROBABILITY) 

class UDPClient():
    def __init__(self, host, port):
        self.host = host
        self.port = int(port)

    def get_port(self):
        return self.port

    def send(self, request):
        print('Connecting to server at {}:{}'.format(self.host, self.port))
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(request, (self.host, self.port))
            response, ip = s.recvfrom(BUFFER_SIZE)
            return response
        except socket.error:
            print("Error! {}".format(socket.error))
            exit()


def process(udp_clients):
    client_ring = NodeRing(udp_clients)
    hash_codes = set()
    # PUT all users.
    for u in USERS:
        data_bytes, key = serialize_PUT(u)
        bloomfilter.add(key)
        response = client_ring.get_randezvous_hash_node(key).send(data_bytes)
        print(response)
        hash_codes.add(str(response.decode()))


    print(f"Number of Users={len(USERS)}\nNumber of Users Cached={len(hash_codes)}")
    
    # GET all users.
    for hc in hash_codes:
        print(hc)
        data_bytes, key = serialize_GET(hc)
        if bloomfilter.is_member(key):
            response = client_ring.get_randezvous_hash_node(key).send(data_bytes)
        else:
            return None
        print(response)

    # DELETE all users.
    for hc in hash_codes:
        print(hc)
        data_bytes, key = serialize_DELETE(hc)
        if bloomfilter.is_member(key):
            response = client_ring.get_randezvous_hash_node(key).send(data_bytes)
        else:
            return None
        print(response)

if __name__ == "__main__":
    clients = [
        UDPClient(server['host'], server['port'])
        for server in NODES
    ]
    process(clients)

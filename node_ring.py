import hashlib
from server_config import NODES
import mmh3

class NodeRing():

    def __init__(self, nodes):
        assert len(nodes) > 0
        self.nodes = nodes
    
    def get_node(self, key_hex):
        key = int(key_hex, 16)
        node_index = key % len(self.nodes)
        return self.nodes[node_index]

    def get_weight(self, long_node, key_hex):
        a = 1428471428
        b = 27181
        mmh_hash = mmh3.hash(key_hex)
        return (a * ((a * long_node + b) ^ mmh_hash) + b) % (2^31)

    def get_randezvous_hash_node(self, key_hex):
        """Return the node for the randezvous hash key"""
        weights = []
        node_index = 0
        for node in self.nodes:
            # print(node['port'])
            n = node.get_port()
            w = self.get_weight(n, key_hex)
            weights.append((w, node_index))
            node_index+=1

        _, node_index = max(weights)
        return self.nodes[node_index]

def test():
    ring = NodeRing(nodes=NODES)
    node = ring.get_node('9ad5794ec94345c4873c4e591788743a')
    print(node)
    node = ring.get_randezvous_hash_node('9ad5794ec94345c4873c4e591788743a')
    print(node)
    print(ring.get_node('ed9440c442632621b608521b3f2650b8'))


# Uncomment to run the above local test via: python3 node_ring.py
# test()

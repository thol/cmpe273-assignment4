import bisect
import hashlib
import pickle

class ConsistentHashRing(object):

    def __init__(self, replica_nodes=100):
        self.replica_nodes = replica_nodes
        self.keys         = []
        self.nodes        = {}

    def get_hash(self, key):
        object_bytes      = pickle.dumps(key)
        return hashlib.md5(object_bytes).hexdigest()

    def replica_iterator(self, nodename):
        return (self.get_hash("%s:%s" % (nodename, i)) for i in range(self.replica_nodes))

    def __setitem__(self, nodename, node):
        for hash_iter in self.replica_iterator(nodename):
            if hash_iter in self.nodes:
                raise ValueError("Node already available: %r" % nodename)
            self.nodes[hash_iter] = node
            bisect.insort(self.keys, hash_iter)

    def __delitem__(self, nodename):
        for hash_iter in self.replica_iterator(nodename):
            # will raise KeyError for nonexistent node name
            del self.nodes[hash_iter]
            index = bisect.bisect_left(self.keys, hash_iter)
            del self.keys[index]

    def __getitem__(self, key):
        hash_obj = self.get_hash(key)
        start = bisect.bisect(self.keys, hash_obj)
        if start == len(self.keys):
            start = 0
        return self.nodes[self.keys[start]]
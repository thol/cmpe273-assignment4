import mmh3
from bitarray import bitarray
import math

class BloomFilter(object):

    def __init__(self, total_items, prob):
        self.total_items = total_items
        self.prob = prob

        self.bitarray_size = int(-(total_items * math.log(prob))/(math.log(2)**2))
        self.hash_size = int((self.bitarray_size/total_items) * math.log(2) )
        self.bitarray = bitarray(self.bitarray_size)

        self.bitarray.setall(0)


    def add(self, key):

        hashes = []
        for cnt in range(self.hash_size):
            hash_key = mmh3.hash(key, cnt) % self.bitarray_size
            hashes.append(hash_key)
            self.bitarray[hash_key]= True

    def is_member(self, key):

        for cnt in range(self.hash_size):
            hash_key = mmh3.hash(key, cnt) % self.bitarray_size
            if self.bitarray[hash_key] == False:
                return False
        return True



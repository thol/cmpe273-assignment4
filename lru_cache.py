import collections
import functools
from random import randint

class lru_cache_obj(object):

    _instance = None
    cache_dict = collections.OrderedDict()

    def __new__(cls, size):
        if cls._instance is None:
            # print('Creating new cache')
            cls._instance = super(lru_cache_obj, cls).__new__(cls)
            cls._instance.set_size(size)
        return cls._instance

    def __init__(self, size):
        self.set_size(size)

    def set_size(self, cache_entries):
        if cache_entries > 0:
            self.cache_size = cache_entries

    def set(self, k, v):
        try:
            self.cache_dict.pop(k)
        except KeyError:
            if len(self.cache_dict) >= self.cache_size:
                self.cache_dict.popitem(last=False)
        self.cache_dict[k] = v
        # print(self.cache_dict[k])

    def get(self, k):
        try:
            value = self.cache_dict[k]
            self.cache_dict.pop(k)
            self.cache_dict[k] = value
            # print("dict values... of size {}".format(self.cache_size))
            # for key in self.cache_dict:
            #     print("dict {}".format(key)) 
            return value
        except KeyError:
            return -1

    def delete(self, k):
        try:
            self.cache_dict.pop(k)
            return k
        except KeyError:
            return -1

def lru_cache(cache_size):
    def lru_cache_decorator(func):
        @functools.wraps(func)
        def wrapper_lru_cache(*args, **kwargs):
            cache_obj = lru_cache_obj(cache_size)
            # cache_obj.set_size(cache_size)
            # print("********"+func.__name__+"********")
            if func.__name__ == "serialize_PUT":
                key, value = func(*args, **kwargs)
                cache_obj.set(key, value)
                return key, value
            elif func.__name__ == "serialize_DELETE":
                key = args
                value = cache_obj.delete(*args)
                if (value == -1):
                    key, value = func(*args, **kwargs)
                return key, value
            elif func.__name__ == "serialize_GET":
                key = args
                value = cache_obj.get(*args)
                if (value == -1):
                    key, value = func(*args, **kwargs)
                    cache_obj.set(key, value)
                return key, value
            else:
                value = cache_obj.get(*args)
                if (value == -1):
                    value = func(*args, **kwargs)
                    cache_obj.set(*args, value)
                return value
        return wrapper_lru_cache
    return lru_cache_decorator

def lru_cache_test():
    c = lru_cache(5)

    for i in range(9,0,-1):
        c.set(i,"str")

    for i in range(50):
        r = randint(0,49)
        print("{} {}".format(r,c.get(r)))

# lru_cache_test()







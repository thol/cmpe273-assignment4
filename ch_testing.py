import unittest
import collections
import random
import math
from consistent_hashing import ConsistentHashRing

class ConsistentHashRingTest(unittest.TestCase):
    def test_get_distribution(self):
        ring = ConsistentHashRing(100)

        numnodes = 500
        numhits = 50000
        numvalues = 500000

        for i in range(1, 1 + numnodes):
            ring["node_name%d" % i] = "node_name_str%d" % i

        # del ring["node_name1"]

        distributions = collections.defaultdict(int)
        for i in range(numhits):
            key = str(random.randint(1, numvalues))
            node = ring[key]
            distributions[node] += 1

        # for k in distributions.keys():
        #     print("{} : {}".format(k,distributions[k]))

        print("Sum {}".format(sum(distributions.values())))
        self.assertEqual(sum(distributions.values()), numhits)

        standard_dev = self._pop_std_dev(distributions.values())
        print("stddev {}".format(standard_dev))
        self.assertLessEqual(standard_dev, 20)

        print("total distributions {}".format(len(distributions)))
        self.assertEqual(len(distributions), numnodes)

        print(set(distributions.keys()))
        self.assertEqual(
                set(distributions.keys()),
                set("node_name_str%d" % i for i in range(1, 1 + numnodes))
            )

    def _pop_std_dev(self, population):
        mean = sum(population) / len(population)
        return math.sqrt(
                sum(pow(n - mean, 2) for n in population)
                / len(population)
            )

if __name__ == '__main__':
    unittest.main()


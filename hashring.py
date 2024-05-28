from sortedcontainers import SortedDict
import mmh3 


class HashRing():
    def __init__(self):
        self.cur_nodes = SortedDict()
        self.hash = lambda x : mmh3.hash(str(x))
        #self.hash = lambda x : xxhash.xxh32(str(x).encode()).intdigest()

    def __getitem__(self, key):
        hash_value = self.hash(key)
        # retrieve position on ring corresponding to hash digest
        # modulus to retrieve first node if insertion index was equal to the length of the list
        next_node_index = self.cur_nodes.bisect_left(hash_value) % len(self.cur_nodes)
        next_node_hash = self.cur_nodes.keys()[next_node_index]
        return self.cur_nodes[next_node_hash]

    def __setitem__(self, key, val):
        raise Exception("Can't set item, __setitem__ not implemented")

    def __contains__(self, node):
        hash_value = self.hash(node)
        return hash_value in self.cur_nodes

    def __len__(self):
        return len(self.cur_nodes)

    def __str__(self):
        return str(self.cur_nodes)

    def add_node(self, nodeID):
        # TODO: Handle Collisions
        hash_value = self.hash(nodeID)
        self.cur_nodes[hash_value] = nodeID

    def remove_node(self, nodeID):
        hash_value = self.hash(nodeID)
        if hash_value in self.cur_nodes:
            del self.cur_nodes[hash_value]
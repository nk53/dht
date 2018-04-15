class Table:
    """A simple table that uses integer keys to access values. It isn't
    truly a hash table, since no hashing function is used to access values,
    and collisions are not allowed."""

    def __init__(self, num_buckets):
        """Initialize hash table with None everywhere"""
        self.buckets = [None for i in range(num_buckets)]
        self.num_buckets = num_buckets

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return repr(self.buckets)

    def put(self, key, value):
        """Associates a key with a value. This process is irreversible.
        
        For the sake of simplicity, any put that would cause a collision
        will simply not occur."""
        #index, value_in_table = self.lookup(key)
        #if value_in_table != None:
        #    return False

        index = key % self.num_buckets
        self.buckets[index] = value
        return True

    def get(self, key):
        index, value_in_table = self.lookup(key)
        return value_in_table

    def lookup(self, key):
        """Returns the value associated with `key` and the index where it
        occurs, if it exists. Otherwise, None is returned"""
        index = key % self.num_buckets
        return index, self.buckets[index]


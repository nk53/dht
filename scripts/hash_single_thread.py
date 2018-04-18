from multiprocessing import Lock
class Table:
    """A simple table that uses integer keys to access values. It isn't
    truly a hash table, since no hashing function is used to access values,
    and collisions are not allowed."""

    def __init__(self, num_buckets):
        """Initialize hash table with None everywhere"""
        self.buckets = [None for i in range(num_buckets)]
        self.locks = [Lock() for i in range(num_buckets)]
        self.num_buckets = num_buckets

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return repr(self.buckets)

    def put(self, key, value):
        """Associates a key with a value. This process is irreversible.
        
        For the sake of simplicity, any put that would cause a collision
        will simply not occur.
        
        If the process cannot be completed because the bucket is locked,
        False is returned, otherwise True is returned"""
        index = key % self.num_buckets
        self.buckets[index] = value

    def get(self, key):
        """Attempts to read a value from the table. If the lock is busy,
        false is returned instead"""
        index = key % self.num_buckets
        lock = self.locks[index]
        if lock.acquire(block=False):
            value_in_table = self.buckets[index]
            lock.release()
            return value_in_table
        else:
            return False

    def acquire(self, key):
        """Attempts to acquire the lock associated with a bucket"""
        index = key % self.num_buckets
        return self.locks[index].acquire(block=False)

    def release(self, key):
        """Releases the lock associated with a bucket"""
        index = key % self.num_buckets
        self.locks[index].release()

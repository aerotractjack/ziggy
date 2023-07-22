from pathlib import Path
import os
import json
import time

class FQueue:

    def __init__(self, base, name=0):
        # Initializes a file-based queue with a specified base directory.
        self.base = Path(base).absolute()
        self.base.mkdir(parents=True, exist_ok=True)

    def write(self, contents, path):
        # Writes the contents to a file at the specified path.
        with open(path, "w") as fp:
            fp.write(json.dumps(contents))

    def read(self, path):
        # Reads the contents from a file at the specified path.
        with open(path, "r") as fp:
            return json.loads(fp.read())

    @property
    def contents(self):
        # Returns a list of file paths in the queue directory.
        return list(self.base.iterdir())

    @property
    def names(self):
        # Returns a list of integer names assigned to the files in the queue.
        contents = self.contents
        names = [int(f.name.split(".")[0]) for f in contents]
        return names

    @property
    def length(self):
        # Returns the number of files in the queue.
        return len(os.listdir(self.base))

    @property
    def push_n(self):
        # Returns the next available integer name for pushing an entry.
        paths = self.names
        if len(paths) == 0:
            return 0
        return max(paths) + 1

    @property
    def pop_n(self):
        # Returns the smallest integer name in the queue.
        paths = self.names
        if len(paths) == 0:
            return -1
        return min(paths)

    def make_path(self, n):
        # Creates a file path for the given name.
        return (self.base / str(n)).with_suffix(".q")

    def push(self, entry):
        # Pushes an entry to the queue and returns the length of the queue and the path of the pushed entry.
        entry_dst_path = self.make_path(self.push_n)
        self.write(entry, entry_dst_path)
        return self.length

    def pop(self):
        # Pops the entry from the queue with the smallest name and returns the path and contents of the popped entry.
        if self.pop_n == -1:
            raise ValueError("nothing in q to pop")
        pop_path = self.make_path(self.pop_n)
        contents = self.read(pop_path)
        Path(pop_path).unlink()
        return contents

class MultiQueue:

    def __init__(self, base, n):
        # Initializes multiple Fqueues with a base directory and a specified number of queues.
        self.base = Path(base)
        self.bases = [(self.base / str(i)) for i in range(n)]
        self.qs = [FQueue(b) for b in self.bases]

    def add_queue(self):
        base = self.base / str(len(self.qs))
        self.bases.append(base)
        self.qs.append(FQueue(base))

    @property
    def length_map(self):
        # Returns a dictionary with the lengths of all the queues.
        return {i: self.qs[i].length for i in range(len(self.qs))}

    @property
    def length(self):
        # Returns the total length of all the queues.
        return sum(self.length_map.values())

    def get_least_busy(self):
        # Returns the Fqueue with the least number of entries.
        minv = 9999999
        mini = -1
        for i in range(len(self.qs)):
            qn = self.qs[i].length
            if qn > minv:
                continue
            minv = qn 
            mini = i
        return (mini, self.qs[mini])

    def get_most_busy(self):
        # Returns the Fqueue with the most number of entries.
        maxv = -1
        maxi = -1
        for i in range(len(self.qs)):
            qn = self.qs[i].length
            if qn < maxv:
                continue
            maxv = qn 
            maxi = i
        return (maxi, self.qs[maxi])

    def push(self, entry):
        # Pushes an entry to the least busy queue and returns the index of the queue and the push result.
        qi, q = self.get_least_busy()
        return (qi, q.push(entry))

    def pop(self):
        # Pops an entry from the most busy queue and returns the index of the queue and the pop result.
        qi, q = self.get_most_busy()
        return (qi, q.pop())

class NamedMultiQueue(MultiQueue):

    def __init__(self, base, names):
        # Initializes multiple Fqueues with a base directory and a list of queue names.
        super().__init__(base, len(names))
        self.names = names

    def add_queue(self, name):
        super().add_queue()
        self.names.append(name)

    def get_queues(self):
        return {self.names[i]: self.qs[i] for i in range(len(self.names))}

    def push(self, entry):
        # Pushes an entry to the least busy queue and returns a dictionary with the queue name and the queue length.
        qi, res = super().push(entry)
        return {"q_name": self.names[qi], "q_length": res}

    def pop(self):
        # Pops an entry from the most busy queue and returns a dictionary with the queue name and the popped entry.
        qi, res = super().pop()
        return {"q_name": self.names[qi], "entry": res}

    def pop_all(self):
        while True:
            try:
                yield self.pop()
            except ValueError:
                break

class QueuePoller:
    
    def __init__(self, q, delta=1):
        # Initializes a queue poller with a queue and a time delta.
        self.q = q
        self.delta = int(delta)

    def poll_forever(self, callback=None):
        # Yields the result of popping an entry from the queue indefinitely with the specified time delta.
        while True:
            try:
                if callback is not None and not callback():
                    time.sleep(self.delta)
                    continue
                yield self.q.pop()
            except ValueError:
                yield None
            time.sleep(self.delta)

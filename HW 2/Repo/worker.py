import rpyc
from collections import Counter, defaultdict
import string
import os

class MapReduceService(rpyc.Service):
    def exposed_map(self, text_chunk):
        """Map step: count words in text chunk."""
        STOP_WORDS = set([
        'a', 'an', 'and', 'are', 'as', 'be', 'by', 'for', 'if', 'in',
        'is', 'it', 'of', 'or', 'py', 'rst', 'that', 'the', 'to', 'with',
        ])
        TR = "".maketrans(string.punctuation, ' ' * len(string.punctuation))
        output = defaultdict(list)
        line = text_chunk.translate(TR)
        # print(line)
        for word in line.split():
            word = word.lower()
            if word.isalpha() and word not in STOP_WORDS:
                output[word].append(1)

        # Partition data by key
        partitioned_data = defaultdict(list)
        for key, value in output.items():
            partitioned_data[key].extend(value)
        return partitioned_data.items()

    def exposed_reduce(self, grouped_items):
        """Reduce step: sum counts for a subset of words."""
        reduced_counts = {}
        for key, values in grouped_items:
            reduced_counts[key] = sum(values)
        return reduced_counts.items()

if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    port = int(os.environ.get("WORKER_PORT", "18861"))
    print(f"Starting MapReduce worker on port {port}...")
    t = ThreadedServer(MapReduceService, port=port, protocol_config={"allow_public_attrs": True})
    t.start()
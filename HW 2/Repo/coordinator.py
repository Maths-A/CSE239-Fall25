import rpyc
import collections
import time
import operator
import glob
import os
import sys
import urllib.request
import zipfile
import threading

WORKERS = []
NUM_MAP_WORKERS = 4
NUM_REDUCE_WORKERS = 4

map_results = {}
reduce_results = {}

def map_worker_thread(worker, chunk):
    """Thread function for map workers."""
    partitioned_lists = worker.root.exposed_map(chunk)
    map_results[worker] = partitioned_lists
    print(f"Map completed.")

def reduce_worker_thread(worker, grouped_items):
    """Thread function for reduce workers."""
    results = worker.root.exposed_reduce(grouped_items)
    reduce_results[worker] = results
    print(f"Reduce completed.")

def mapreduce_wordcount(input_files):
    num_map_workers = NUM_MAP_WORKERS
    text_chunk = read_text_files(input_files)
    # Split text into chunks
    chunks = split_text(text_chunk, num_map_workers)
    # Connect to workers
    for i in range(1, max(NUM_MAP_WORKERS, NUM_REDUCE_WORKERS) + 1):
        WORKERS.append((f"worker-{i}", 18861))
    worker_conns = [rpyc.connect(host, port) for host, port in WORKERS]
    print("Connected to worker.")
    # MAP PHASE: Send chunks and get intermediate pairs
    print(f"\nStarting MAP phase with {NUM_MAP_WORKERS} workers...")
    start_time = time.time()
    map_threads = []
    for i, worker_conn in enumerate(worker_conns):
        thread = threading.Thread(target=map_worker_thread, args=(worker_conn, chunks[i]))
        thread.start()
        map_threads.append(thread)

    for thread in map_threads:
        thread.join()
    print(f"MAP phase completed.")
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Elapsed Time after MAP phase: {} seconds".format(elapsed_time))

    # REDUCE PHASE: Send grouped data to reducers
    print(f"\nStarting REDUCE phase with {NUM_REDUCE_WORKERS} workers...")
    start_time = time.time()
    reduce_threads = []
    for i, worker_conn in enumerate(worker_conns):
        grouped_items = map_results[worker_conn]
        thread = threading.Thread(target=reduce_worker_thread, args=(worker_conn, grouped_items))
        thread.start()
        reduce_threads.append(thread)

    for thread in reduce_threads:
        thread.join()
    print(f"REDUCE phase completed.")
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("\nElapsed Time after REDUCE phase: {} seconds".format(elapsed_time))

    # FINAL AGGREGATION
    start_time = time.time()
    total_counts = collections.defaultdict(int)
    # Directly aggregate from reduce results without intermediate list
    for worker_conn in worker_conns:
        for key, value in reduce_results[worker_conn]:
            try:
                total_counts[key] += int(value)
            except Exception:
                continue

    # Disconnect to workers
    for worker_conn in worker_conns:
        worker_conn.close()

    print("Final aggregation completed.")
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("\nElapsed Time after final phase: {} seconds".format(elapsed_time))
    return total_counts


def read_text_files(input_files):
    text_chunk = ""
    for filename in input_files:
        with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
            text_chunk += f.read()
    return text_chunk

def split_text(text, n):
    # Function to split text into n chunks
    avg_len = len(text) // n
    chunks = []
    for i in range(n):
        start = i * avg_len
        if i == n - 1:
            end = len(text)
        else:
            end = (i + 1) * avg_len
        chunks.append(text[start:end])
    return chunks

def download(url='https://mattmahoney.net/dc/enwik8.zip'):
    """Downloads and unzips a wikipedia dataset in txt/."""
    filename = url.split('/')[-1]
    filepath = "txt/" + filename
    print(filepath)
    if os.path.exists(filepath):
        print(f"File {filepath} already exists, skipping download.")
    else:
        os.makedirs('txt', exist_ok=True)
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, filepath)
        print(f"Downloaded {filepath}")
        print(f"Extracting {filepath}...")
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            zip_ref.extractall('txt/')
        print("Extraction complete.")
        # remove zip file
        os.remove(filepath)

    return glob.glob('txt/*')


if __name__ == "__main__":
    # DOWNLOAD AND UNZIP DATASET
    text = download(sys.argv[1] if len(sys.argv) > 1 else 'https://mattmahoney.net/dc/enwik8.zip')
    NUM_MAP_WORKERS = int(sys.argv[2]) if len(sys.argv) > 2 else NUM_MAP_WORKERS
    NUM_REDUCE_WORKERS = int(sys.argv[3]) if len(sys.argv) > 3 else NUM_REDUCE_WORKERS
    start_time = time.time()
    input_files = glob.glob('txt/*')
    word_counts = mapreduce_wordcount(input_files)
    word_counts = sorted(word_counts.items(), key=operator.itemgetter(1))
    word_counts.reverse()
    print('\nTOP 20 WORDS BY FREQUENCY\n')
    top20 = word_counts[0:20]
    longest = max(len(word) for word, count in top20)
    i = 1
    for word, count in top20:
        print('%s.\t%-*s: %5s' % (i, longest+1, word, count))
        i = i + 1

    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Elapsed Time: {} seconds".format(elapsed_time))
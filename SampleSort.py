import ray
import qs
import numpy as np
# pseudocode
# function sampleSort(A[1..n], k, p)
#     // if average bucket size is below a threshold switch to e.g. quicksort
#     if n / k < threshold then smallSort(A) 
#     /* Step 1 */
#     select S = [S1, ..., S(p−1)k] randomly from // select samples
#     sort S // sort sample
#     [s0, s1, ..., sp−1, sp] <- [-∞, Sk, S2k, ..., S(p−1)k, ∞] // select splitters
#     /* Step 2 */
#     for each a in A
#         find j such that sj−1 < a <= sj
#         place a in bucket bj
#     /* Step 3 and concatenation */
#     return concatenate(sampleSort(b1), ..., sampleSort(bk))

def sampleSort(arr, total_buckets):
    if len(arr)/total_buckets < 2000:
        print("Just doing quicksort, bucket size is too small")
        return qs.quickSort(arr, 0, len(arr)-1)
    # unsorted_splitters = np.random.choice(arr, total_buckets - 1, replace=False)
    # splitters = np.sort(unsorted_splitters)
    # randomly selecting was far too inefficient, have to make this more efficient
    percentiles = [(i/ total_buckets) * 100 for i in range (1, total_buckets)]
    splitters = np.sort(np.percentile(arr, percentiles))
    # print(splitters)

    # Step 2: Distribute data into buckets
    chunk_size = len(arr) // total_buckets
    chunks = [arr[i:i+chunk_size] for i in range(0, len(arr), chunk_size)]
    buckets_unprocessed =  [process_chunk.remote(chunk, splitters, total_buckets) for chunk in chunks]
    # Combine buckets from all chunks
    combined_buckets = [[] for _ in range(total_buckets)]
    for chunk_buckets in ray.get(buckets_unprocessed):
        for i, bucket in enumerate(chunk_buckets):
            combined_buckets[i].extend(bucket)

    # Step 3: Sort each bucket in parallel
    sorted_buckets = ray.get([call_qs.remote(bucket) for bucket in combined_buckets])

    # Step 4: Concatenate the results
    return [element for bucket in sorted_buckets for element in bucket]

@ray.remote
def process_chunk(chunk, splitters, total_buckets):
    buckets = [[] for _ in range(total_buckets)]
    for element in chunk:
        placed = False
        for i, splitter in enumerate(splitters):
            if element < splitter:
                buckets[i].append(element)
                placed = True
                break
        if not placed:
            buckets[-1].append(element)
    return buckets


@ray.remote
def call_qs(bucket):
    return qs.quickSort(bucket, 0, len(bucket)-1)
    # return np.sort(bucket)

    

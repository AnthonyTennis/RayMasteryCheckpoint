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
    unsorted_splitters = np.random.choice(arr, total_buckets - 1, replace=False)
    splitters = qs.quickSort(unsorted_splitters,0,len(unsorted_splitters) -1)
    print(splitters)

    # Step 2: Distribute data into buckets
    buckets = [[] for _ in range(total_buckets)]
    for element in arr:
        in_a_bucket = False
        for i, splitter in enumerate(splitters):
            if element < splitter:
                buckets[i].append(element)
                in_a_bucket=True
                break
        if in_a_bucket == False:
            buckets[-1].append(element)

    for bucket in buckets:
        print(len(bucket))

    # Step 3: Sort each bucket in parallel
    sorted_buckets = ray.get([call_qs.remote(bucket) for bucket in buckets])

    # Step 4: Concatenate the results
    return [element for bucket in sorted_buckets for element in bucket]

@ray.remote
def call_qs(bucket):
    return qs.quickSort(bucket, 0, len(bucket)-1)

    

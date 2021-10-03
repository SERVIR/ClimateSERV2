import multiprocessing
import random
import time

def start_processing(iterable_item):
    print("in the method")
    print(iterable_item['id'])
    print("past")
    time.sleep(3)
    return {
        'new_id': iterable_item['id'] * 2,
        'new_something': iterable_item['something'] + " work",
    }
    #results.append(altered)

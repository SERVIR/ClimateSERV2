import multiprocessing
import random
import time


def start_processing(full_job):
    # here calculate the years and create a list of jobs
    # instead i am sending fake data
    # jobs = get_filelist(dataset,datatype,start_date,end_date):

    jobs = [{'id': 1, 'something': "bob"},
            {'id': 2, 'something': "tim"},
            {'id': 3, 'something': "sam"},
            {'id': 4, 'something': "todd"},
            {'id': 5, 'something': "bill"}
            ]

    print("in head processor")
    print(full_job)

    with multiprocessing.Manager() as manager:
        L = manager.list()  # <-- can be shared between processes.
        processes = []
        # here we are creating processes for each job (year)
        for job in jobs:
            p = multiprocessing.Process(target=start_worker_process, args=(L, job))
            p.start()
            processes.append(p)
        for p in processes:
            p.join()

        # this is the final list that would be returned by the jobs
        # you likely have to merge them, i'm guessing you had to do
        # similar with the results of zmq
        print(L)


def start_worker_process(result, job_item):
    # here is where you would open each netcdf
    # and do the processing and create the data
    # to return to the parent for said year.
    # I am using fake data so i'm just changing
    # it so we can see it is being "processed"
    print("in worker process")
    print(job_item['id'])
    print("past")
    time.sleep(3)
    result.append({
        'new_id': job_item['id'] * 2,
        'new_something': job_item['something'] + " work",
    })

import os
import datetime


def remove_from(where):
    today = datetime.datetime.today()
    os.chdir(where)

    for root, directories, files in os.walk(where, topdown=False):
        for name in files:
            # this is the last modified time
            t = os.stat(os.path.join(root, name))[8]
            filetime = today - datetime.datetime.fromtimestamp(t)

            print(datetime.datetime.fromtimestamp(t))
            print(today)

            hours_old = filetime.total_seconds() / 3600
            if hours_old >= 24:
                os.remove(os.path.join(root, name))


remove_from('''/mnt/cs-temp/request_out/''')

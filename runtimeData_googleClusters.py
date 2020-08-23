import pandas as pd
import matplotlib
from numpy import arange
matplotlib.use('agg')
import matplotlib.pyplot as plt
import csv
import sys
'''
data_events fields:
0: timestamp
2: jobID
3: task index
5: event type

job_events field:
7: job logical name
2: job event

'''

def read_data(log_name,part_nums):
    job_IDs = list(get_job_IDs(log_name,part_nums))
    print('Job_ids are:',job_IDs)
    schedule_dict = {}
    runtime_dict = {}
    for part_num in part_nums:
        data = pd.read_csv(get_part_add_task(part_num), compression='gzip', usecols=[0,2,3,5], header=None)
        for index, row in data.iterrows():
            if row[5] == 1: ### a schedule event
                schedule_dict['%d_%d'%(row[2],row[3])] = row[0]
            elif row[5] == 4 and '%d_%d'%(row[2],row[3]) in schedule_dict and row[2] in job_IDs:
                runtime_dict['%d_%d'%(row[2],row[3])] = (row[0] - schedule_dict['%d_%d'%(row[2],row[3])])/(1e6)
        print("part %d is done"%part_num)
        sys.stdout.flush()
    return runtime_dict

def get_part_add_task(num):
    if num < 10:
        return '/home/ab1598/google_traces/clusterdata-2011-2/task_events/part-0000%d-of-00500.csv.gz'%num
    elif num < 100:
        return '/home/ab1598/google_traces/clusterdata-2011-2/task_events/part-000%d-of-00500.csv.gz'%num
    else:
        return '/home/ab1598/google_traces/clusterdata-2011-2/task_events/part-00%d-of-00500.csv.gz'%num

def get_part_add_job(num):
    if num < 10:
        return '/home/ab1598/google_traces/clusterdata-2011-2/job_events/part-0000%d-of-00500.csv.gz'%num
    elif num < 100:
        return '/home/ab1598/google_traces/clusterdata-2011-2/job_events/part-000%d-of-00500.csv.gz'%num
    else:
        return '/home/ab1598/google_traces/clusterdata-2011-2/job_events/part-00%d-of-00500.csv.gz'%num

def get_max_logical_name(part_nums):
    job_dict = {}
    for part_num in part_nums:
        data = pd.read_csv(get_part_add_job(part_num), compression='gzip', usecols=[7], header=None)
        for index, row in data.iterrows():
            if '%s'%row[7] not in job_dict:
                job_dict['%s'%row[7]] = 1
            else:
                job_dict['%s'%row[7]] += 1
        print("part %d is done"%part_num)
        sys.stdout.flush()
    #print(job_dict[max(job_dict, key=job_dict.get)])
    print(job_dict)
    return max(job_dict, key=job_dict.get)
    

def get_job_IDs(log_name,part_nums):
    job_IDs = []
    for part_num in part_nums:
        data = pd.read_csv(get_part_add_job(part_num), compression='gzip', usecols=[2,7], header=None)
        for index, row in data.iterrows():
            if '%s'%row[7] == log_name:
                job_IDs.append(row[2])
        print("part %d is done"%part_num)
        sys.stdout.flush()
    job_IDs = set(job_IDs)
    return job_IDs

    
                
def plot_data(log_name,part_nums):
    data = list(read_data(log_name,part_nums).values())
    plt.hist(data, 200, facecolor='g', log=True, alpha=0.75)
    plt.xlabel('task run time')
    plt.ylabel('Number')
    plt.gca().set_xscale("log")
    plt.title('Histogram of task run times in Google traces')
    plt.savefig('result.pdf')
    plt.show()

def plot_data_ccdf(log_name,part_nums):
    data = list(read_data(log_name,part_nums).values())
    p = 1.*arange(len(data))/(len(data)-1)
    p = [1-x for x in p]
    data.sort()
    plt.plot(data,p)
    plt.xlabel("runtimes")
    plt.ylabel("probability")
    plt.xscale('log')
    plt.yscale('log')
    plt.savefig('ccdf.pdf')
    plt.show()

def write_data(log_name,part_nums):
    data = list(read_data(log_name,part_nums).values())
    df = pd.DataFrame(data,columns=["runtimes"])
    df.to_csv('runtime_data.csv', index=False)

def get_job_logical_name(part_nums):
    job_dict = {}
    jobs_with_more_than_20_runs = []
    for part_num in part_nums:
        data = pd.read_csv(get_part_add_job(part_num), compression='gzip', usecols=[7], header=None)
        for index, row in data.iterrows():
            if '%s'%row[7] not in job_dict:
                job_dict['%s'%row[7]] = 1
            else:
                job_dict['%s'%row[7]] += 1
    jobs_with_more_than_20_runs = [key for key in job_dict.keys() if job_dict[key] > 20]
    return jobs_with_more_than_20_runs

def get_job_of_interest(jobs,part_nums):
    temp_job = ""
    temp_ratio = 1
    for job_name in jobs:
        task_runtimes = list(read_data(job_name,part_nums).values())
        task_runtimes.sort()
        print("this is sorted runtimes",task_runtimes)
        sys.stdout.flush()
        if task_runtimes:
            with open('tail_log.txt', 'a') as logFile:
                logFile.write('job %s has the ratio of %.2f \n'%(job_name, task_runtimes[-1]/task_runtimes[len(task_runtimes)//2]))
    return job_name

if __name__ == "__main__":
    job_of_interest = "MW8tcTvQrf5pq7+4iGzi8Y64+fAPCFnOjEhFtpAIolU="
    write_data(job_of_interest,range(1,20))
    


import json
import pandas as pd
import subprocess

# collect job data
queues = ['secondary', 'wagner', 'qmchamm', 'physics', 'test']
result = subprocess.run(['squeue', '--json'], check=True, capture_output=True).stdout.decode('utf-8')
jobs = json.loads(result)
l = []
for job in jobs['jobs']:
    if job['partition'] in queues:
        l.append({
            'queue': job['partition'],
            'user_name': job['user_name'],
            'job_state': job['job_state']
            })
d = pd.DataFrame(l)

# count job states for each queue and user
counts = []
for (q, u), g in d.groupby(['queue', 'user_name']):
    count = g['job_state'].value_counts().to_dict()
    count['TOTAL'] = sum(count.values())
    count['queue'] = q
    count['user'] = u
    counts.append(count)
c = pd.DataFrame(counts)
c = c.fillna(0)
for col in c.columns:
    if col not in ['user', 'queue']:
        c[col] = c[col].astype(int)
show_columns = ['queue', 'user', 'TOTAL', 'PENDING', 'RUNNING']
right_cols = list(set(c.columns.tolist()) - set(show_columns))
s = c[show_columns + right_cols].sort_values(by=['queue', 'TOTAL'], ascending=False)
print(s)

# print(d.groupby('user_name')['job_state'].value_counts())
    # for job in d:
    #     print(job)
# for queue in queues:
#     print(f'Queue: {queue}')
#     result = subprocess.run(['squeue', '-l', '-p', queue], check=True, capture_output=True).stdout.decode('utf-8')
#     trim = re.sub('\((.+)\)', '-', result)

#     lines = trim.split('\n')
#     ll = [line.split() for line in lines[1:] if line]
#     if len(ll) < 2:
#         continue
#     print(ll)
#     d = pd.DataFrame(ll[1:], columns=ll[0])
#     l = []
#     for user, group in d.groupby(['USER']):
#         d = {'user': user[0]}
#         total = 0
#         for state, g in group.groupby(['STATE']):
#             count = g.shape[0]
#             d[state[0].lower()] = count
#             total += count
#         d['total'] = total
#         l.append(d)
#     c = pd.DataFrame(l).fillna(0)
#     c[c.columns.difference(['user'])] = c[c.columns.difference(['user'])].astype(int)
#     c = c.sort_values(by=['total'], ascending=False)
#     left = c.loc[:, ['user', 'total']]
#     right = c[c.columns.difference(['user', 'total'])]
#     out = left.join(right).reset_index(0, drop=True)
#     print(out)
#     print('-'*50)

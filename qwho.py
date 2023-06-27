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

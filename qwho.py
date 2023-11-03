import json
import pandas as pd
import subprocess

# collect job data
queues = ['secondary', 'wagner', 'qmchamm', 'physics', 'test']
result = subprocess.run(['squeue', '--json'], check=True, stdout=subprocess.PIPE).stdout.decode('utf-8')
result = '\n'.join([line for line in result.split('\n') if 'error' not in line])
lines = result.split('\n')
result_l = []
for j, line in enumerate(lines):
    if j == 15 and ']' in line:
        continue
    result_l.append(line)
output = '\n'.join(result_l)
jobs = json.loads(output)
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

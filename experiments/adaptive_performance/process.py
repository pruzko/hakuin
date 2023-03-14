import os

files = os.listdir()
files.remove('measurements')
files.remove('process.py')
files.remove('results.csv')

results = {}

for fn in files:
    data = open(fn).read().split('\n')
    data.pop()
    data = [float(x) for x in data]
    results[fn.split('.')[0]] = [round(sum(data[i*20:(i+1)*20])/20, 2) for i in range(int(1000/20))]

results['baseline'] = [7.00 for x in results['users_username']]

with open('results.csv', 'w') as f:
    f.write(','.join(['i'] + list(results.keys())))
    f.write('\n')
    for i, vals in enumerate(zip(*results.values())):
        f.write(','.join([str(i * 20)] + [str(v) for v in vals]))
        f.write('\n')
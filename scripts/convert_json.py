import json

import os

with open('Task1.json') as f:
    s = json.load(f)
with open('file_lst.json') as f:
    lst = json.load(f)

s_dct = dict(s)
lst = list(lst)

lst = [os.path.basename(l) for l in lst]
dct_name = {name: i for i, name in enumerate(lst)}

output = list(list(zip(*sorted([(v, dct_name[v[0]])
                                          for v in list(s_dct.items())], key=lambda x: x[1])))[0])

output = [ds for name, ds in output]

with open('task1_sorted.json', 'w') as f:
    json.dump(output, f)
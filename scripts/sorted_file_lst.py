from ds_reader import get_ds_file_names

import json

with open('/user/hm74/NYCOpenData') as f:
    json.dump(get_ds_file_names('/user/hm74/NYCOpenData'), f)

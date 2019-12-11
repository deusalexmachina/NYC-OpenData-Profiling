# theBiggestData

## Running

### Timing

Use timing module to automate timing of a function that operates on a series of dfs.
`timing.timed` accepts a closure. Refer to `sample.py` for a good example of how to use timing.timed with a closure.

### From CLI

typical usage: `./env.sh "<file.py> <ds dir> <number of ds (limit)> <random order>"`
Limit and Rand are optional. Use `-` to not pass arg.
example: `./env.sh "sample.py datasets - True"`

refer to `cli.py` for an explanation of how to grab arguments from the cli

Run as ./env.sh "basic_metadata.py /user/hm74/NYCOpenData 30 print"

Make sure you remove files named "average_time_record", "master_dct_0.json","master_itemset.json" and "run_time_record" from your running directory if running the code for the second time.

---


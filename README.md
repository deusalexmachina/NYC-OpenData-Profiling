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
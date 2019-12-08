# loads env and run command
module load python/gnu/3.6.5
module load spark/2.2.0
export PYTHON_PATH='/share/apps/python/3.6.5/bin/python'

spark-submit --conf spark.pyspark.python=$PYTHON_PATH $1 

# execute like `[vn500@login-1-1 theBiggestData]$ ./env.sh "ds_reader.py datasets"`
# loads env and run command
module load python/gnu/3.6.5
module load spark/2.4.0

# PYTHON_PATH='/share/apps/python/3.6.5/bin/python'

PYTHON_PATH="$HOME/.conda/envs/big_data/bin/python"
spark-submit \
--conf spark.pyspark.python=$PYTHON_PATH \
--conf spark.executor.memoryOverhead=6G \
--executor-memory 6G \
--py-files similarity.py \
$1

# # TODO: below doesn't work
# PYTHON_PATH="./CONDA/big_data/bin/python"
# spark-submit \
# --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=$PYTHON_PATH \
# --archives hdfs://user/vn500/big_data.zip#CONDA \
# --master yarn \
# --deploy-mode cluster \
# --conf spark.executor.memoryOverhead=6G \
# --executor-memory 6G \
# $1

# execute like `[vn500@login-1-1 theBiggestData]$ ./env.sh "ds_reader.py datasets"`
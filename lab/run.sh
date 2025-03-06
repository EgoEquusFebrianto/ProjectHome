spark-submit --master yarn --deploy-mode cluster \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/home/kudadiri/anaconda3/envs/Home/bin/python \
  --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=/home/kudadiri/anaconda3/envs/Home/bin/python \
  --conf spark.yarn.executorEnv.PYSPARK_PYTHON=/home/kudadiri/anaconda3/envs/Home/bin/python \
    main.py local 2025-02-19
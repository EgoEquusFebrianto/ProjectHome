spark-submit --master yarn --deploy-mode cluster \
--files spark_conf/spark.conf,spark_conf/project.conf \
--py-files capstone_lib.zip \
main.py qa 2025-02-18
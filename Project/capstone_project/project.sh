spark-submit --master yarn --deploy-mode cluster \
--py-file capstone_lib.zip \
--files conf/spark.conf, conf/project.conf,log4j2.properties \
main.py qa 2025-02-15
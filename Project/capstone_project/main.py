import sys
import uuid
import pyspark.sql.functions as f
from lib.logging import Log4j
from lib import ConfigLoader, utils, DataLoader, Transformations

# mode = {1: "local", 2: "qa", 3: "prod"}
# load_date = "2025-02-17"

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("Usage: main.py <mode> <date>")
        sys.exit(1)

    mode = sys.argv[1]
    load_date = sys.argv[2]

    print("Initialize Job Id..")
    job_run_id = f"Capstone-{uuid.uuid4()}"

    print("Initialize Spark Environment..")
    conf = ConfigLoader.get_project_configs(mode)
    enable_hive = True if conf.get("enable.hive") == "true" else False
    hive_db = conf.get("hive.database")
    print("Initialize Successful..")

    print("Creating Spark Session")
    spark = utils.get_spark_session(mode)
    logger = Log4j(spark)
    logger.info(f"Spark App with ID {job_run_id}, Capstone Project is Running..")
    logger.info(f"Spark Initialize Data and Variables for Application")

    if enable_hive and not hive_db:
        logger.warn("Hive is enabled, but no Hive database is specified. This may cause failures.")

    try:
        logger.info("Spark Reading Account Data..")
        account_df = DataLoader.read_accounts(spark, mode, enable_hive, hive_db)

        if account_df is None or account_df.isEmpty():
            logger.warn("Account data is empty. Check the data source.")

        contract_df = Transformations.get_contract(account_df)
    except Exception as e:
        logger.error(f"Failed to read or process account data: {str(e)}")
        sys.exit(1)

    try:
        logger.info("Spark Reading Party Data..")
        parties_df = DataLoader.read_parties(spark, mode, enable_hive, hive_db)

        if parties_df is None or parties_df.isEmpty():
            logger.warn("Party data is empty. Check the data source.")

        relations_df = Transformations.get_relations(parties_df)
    except Exception as e:
        logger.error(f"Failed to read or process party data: {str(e)}")
        sys.exit(1)

    try:
        logger.info("Spark Reading Address Data..")
        address_df = DataLoader.read_address(spark, mode, enable_hive, hive_db)

        if address_df is None or address_df.isEmpty():
            logger.warn("Address data is empty. Check the data source.")

        relation_address_df = Transformations.get_address(address_df)
    except Exception as e:
        logger.error(f"Failed to read or process address data: \n {str(e)}")
        sys.exit(1)

    try:
        logger.info("Join Party Relations and Address")
        party_address_df = Transformations.join_party_address(relations_df, relation_address_df)

        logger.info("Join Account and Parties")
        data_df = Transformations.join_contract_party(contract_df, party_address_df)

        logger.info("Apply Header and create Event")
        final_df = Transformations.apply_header(spark, data_df)

    except Exception as e:
        logger.error(f"Failed to join or process transformations: \n {str(e)}")
        sys.exit(1)

    try:
        logger.info("Preparing to send data to Kafka")
        kafka_kv_df = final_df.select(
            f.col("payload.contractIdentifier.newValue").alias("key"),
            f.to_json(f.struct("*")).alias("value")
        )

        # kafka_kv_df.show(5)
        # input("Press Any Key")

        api_key = conf["kafka.api_key"]
        api_secret = conf["kafka.api_secret"]

        kafka_kv_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", conf["kafka.bootstrap.servers"]) \
            .option("topic", conf["kafka.topic"]) \
            .option("kafka.security.protocol", conf["kafka.security.protocol"]) \
            .option("kafka.sasl.jaas.config", conf["kafka.sasl.jaas.config"].format(api_key, api_secret)) \
            .option("kafka.sasl.mechanism", conf["kafka.sasl.mechanism"]) \
            .option("kafka.client.dns.lookup", conf["kafka.client.dns.lookup"]) \
            .save()

        logger.info("Finished sending data to Kafka")
    except Exception as e:
        logger.error(f"Failed to send data to Kafka: \n {str(e)}")
        sys.exit(1)

    logger.info("Spark Terminate")
    spark.stop()
import sys
import uuid
from random import choice, choices
from faker import Faker
from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession, functions as f
import os

os.environ["SPARK_LOCAL_IP"] = "172.25.5.7:"
os.environ["PYSPARK_PYTHON"] = "/home/kudadiri/anaconda3/envs/Home/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/kudadiri/anaconda3/envs/Home/bin/python"
os.environ["SPARK_HOME"] = "/home/kudadiri/anaconda3/envs/Home/lib/python3.10/site-packages/pyspark"

account_id_offset = 6982391059
part_id_offset = 9823462809
fake = Faker()
Faker.seed(0)
source_sys = ["COR", "COH", "BDL", "ADS", "CML"]
tax_id_type = ["EIN", "SSP", "CPR"]
country = ["United States", "Canada", "Mexico"]


def get_accounts_df(spark: SparkSession, load_date, num_records) -> DataFrame:
    branch = [(fake.swift8(), choice(country)) for r in range(1, 20)]
    data_list = [(
                     load_date,
                     choices([0, 1], cum_weights=[10, 90], k=1)[0],
                     account_id_offset + i,
                     choice(source_sys),
                     fake.date_time_between(start_date='-5y', end_date='-3y'),
                     choice([fake.company(), fake.name()]),
                     choice([fake.name(), None]),
                     choice(tax_id_type),
                     fake.bban()
                 ) + (choice(branch)) for i in range(1, num_records)]

    return spark.createDataFrame(data_list).toDF(
        "load_date",
        "active_ind",
        "account_id",
        "source_sys",
        "account_start_date",
        "legal_title_1",
        "legal_title_2",
        "tax_id_type",
        "tax_id",
        "branch_code",
        "country"
    )


def get_account_party(spark: SparkSession, load_date, num_records) -> DataFrame:
    data_list_f = [(
        load_date,
        account_id_offset + i, part_id_offset + i,
        "F-N",
        fake.date_time_between(start_date='-5y', end_date='-3y')
    ) for i in range(1, int(num_records))]

    data_list_s = [(
        load_date,
        account_id_offset + fake.pyint(1, num_records),
        part_id_offset + num_records + i,
        "F-S",
        fake.date_time_between(start_date='-5y', end_date='-3y')
    ) for i in range(1, int(num_records / 3))]

    return spark.createDataFrame(data_list_f + data_list_s).toDF(
        "load_date",
        "account_id",
        "party_id",
        "relation_type",
        "relation_start_date"
    )


def get_party_address(spark: SparkSession, load_date, num_records) -> DataFrame:
    data_list = [(
        load_date,
        part_id_offset + i,
        f"{fake.building_number()} {fake.street_name()}",
        fake.street_address(),
        fake.city(),
        fake.postcode(),
        choice(country),
        fake.date_time_between(start_date='-5y', end_date='-3y')
    ) for i in range(1, num_records)]

    return spark.createDataFrame(data_list).toDF(
        "load_date",
        "party_id",
        "address_line_1",
        "address_line_2",
        "city",
        "postal_code",
        "country_of_address",
        "address_start_date"
    )


def create_data_files(spark, load_date, num_records):
    account_df = get_accounts_df(spark, load_date, num_records)
    party_df = get_account_party(spark, load_date, num_records)
    address_df = get_party_address(spark, load_date, num_records)

    account_df.coalesce(1).write \
        .format("csv") \
        .option("header", True) \
        .mode("overwrite") \
        .save("data/accounts")

    party_df.coalesce(1).write \
        .format("csv") \
        .option("header", True) \
        .mode("overwrite") \
        .save("data/parties")

    address_df.coalesce(1).write \
        .format("csv") \
        .option("header", True) \
        .mode("overwrite") \
        .save("data/party_address")


def get_account_schema():
    schema = """
        load_date date,
        active_ind int,
        account_id string,
        source_sys string,
        account_start_date timestamp,
        legal_title_1 string,
        legal_title_2 string,
        tax_id_type string,
        tax_id string,
        branch_code string,
        country string"""
    return schema


def get_party_schema():
    schema = """
        load_date date,
        account_id string,
        party_id string,
        relation_type string,
        relation_start_date timestamp"""
    return schema


def get_address_schema():
    schema = """
        load_date date,
        party_id string,
        address_line_1 string,
        address_line_2 string,
        city string,
        postal_code string,
        country_of_address string,
        address_start_date date"""
    return schema


def read_accounts(spark, env, enable_hive, hive_db):
    runtime_filter = get_data_filter(env, "account.filter")
    if enable_hive:
        return spark.sql(f"select * from {hive_db}.accounts").where(runtime_filter)
    else:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(get_account_schema()) \
            .load("data/accounts/") \
            .where(runtime_filter)


def read_parties(spark, env, enable_hive, hive_db):
    runtime_filter = get_data_filter(env, "party.filter")
    if enable_hive:
        return spark.sql(f"select * from {hive_db}.parties").where(runtime_filter)
    else:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(get_party_schema()) \
            .load("data/parties/") \
            .where(runtime_filter)


def read_address(spark, env, enable_hive, hive_db):
    runtime_filter = get_data_filter(env, "address.filter")
    if enable_hive:
        return spark.sql(f"select * from {hive_db}.party_address").where(runtime_filter)
    else:
        return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(get_address_schema()) \
            .load("data/party_address/") \
            .where(runtime_filter)


def get_insert_operation(column, alias):
    return f.struct(
        f.lit("INSERT").alias("operation"),
        column.alias("newValue"),
        f.lit(None).alias("oldValue")
    ).alias(alias)


def get_contract(df):
    contract_title = f.array(
        f.when(
            ~f.isnull("legal_title_1"),
            f.struct(
                f.lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
                f.col("legal_title_1").alias("contractTitleLine")
            ).alias("contractTitle")
        ),
        f.when(
            ~f.isnull("legal_title_2"),
            f.struct(
                f.lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
                f.col("legal_title_2").alias("contractTitleLine")
            ).alias("contractTitle")
        )
    )

    contract_title_n1 = f.filter(contract_title, lambda x: ~f.isnull(x))
    tax_identifier = f.struct(
        f.col("tax_id_type").alias("taxIdType"),
        f.col("tax_id").alias("taxId")
    ).alias("taxIdentifier")

    return df.select(
        "account_id",
        get_insert_operation(f.col("account_id"), "contractIdentifier"),
        get_insert_operation(f.col("source_sys"), "sourceSystemIdentifier"),
        get_insert_operation(f.col("account_start_date"), "contactStartDateTime"),
        get_insert_operation(contract_title_n1, "contractTitle"),
        get_insert_operation(tax_identifier, "taxIdentifier"),
        get_insert_operation(f.col("branch_code"), "contractBranchCode"),
        get_insert_operation(f.col("country"), "contractCountry")
    )


def get_relations(df):
    return df.select(
        "account_id",
        "party_id",
        get_insert_operation(f.col("party_id"), "partyIdentifier"),
        get_insert_operation(f.col("relation_type"), "partyRelationshipType"),
        get_insert_operation(f.col("relation_start_date"), "partyRelationStartDateTime")
    )


def get_address(df):
    address = f.struct(
        f.col("address_line_1").alias("addressLine1"),
        f.col("address_line_2").alias("addressLine2"),
        f.col("city").alias("addressCity"),
        f.col("postal_code").alias("addressPostalCode"),
        f.col("country_of_address").alias("addressCountry"),
        f.col("address_start_date").alias("addressStartDate"),
    )

    return df.select("party_id", get_insert_operation(address, "partyAddress"))


def join_party_address(p_df: DataFrame, a_df: DataFrame):
    return p_df.join(a_df, "party_id", "left") \
        .groupby("account_id") \
        .agg(
        f.collect_list(
            f.struct(
                "partyIdentifier",
                "partyRelationshipType",
                "partyRelationStartDateTime",
                "partyAddress"
            ).alias("partyDetails")
        ).alias("partyRelations")
    )


def join_contract_party(c_df: DataFrame, p_df: DataFrame):
    return c_df.join(p_df, "account_id", "left")


def apply_header(spark: SparkSession, df):
    header_info = [("Capstone_Project", 1, 0), ]
    header_df = spark.createDataFrame(header_info).toDF(
        "eventType",
        "majorSchemaVersion",
        "minorSchemaVersion"
    )

    event_df = header_df.hint("broadcast").crossJoin(df).select(
        f.struct(
            f.expr("uuid()").alias("eventIdentifier"),
            f.col("eventType"),
            f.col("majorSchemaVersion"),
            f.col("minorSchemaVersion"),
            f.lit(f.date_format(f.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssZ")).alias("eventDateTime")
        ).alias("eventHeader"),
        f.array(
            f.struct(
                f.lit("contractIdentifier").alias("keyField"),
                f.col("account_id").alias("keyValue")
            ).alias("keys")
        ),
        f.struct(
            f.col("contractIdentifier"),
            f.col("sourceSystemIdentifier"),
            f.col("contactStartDateTime"),
            f.col("contractTitle"),
            f.col("taxIdentifier"),
            f.col("contractBranchCode"),
            f.col("contractCountry"),
            f.col("partyRelations")
        ).alias("payload")
    )

    return event_df


class Log4j:
    def __init__(self, spark: SparkSession):
        log4j = spark._jvm.org.apache.logging.log4j
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        namespace = "engineer.capstoneproject.spark.application"

        self.logger = log4j.LogManager.getLogger(f"{namespace}.{app_name}")

    def info(self, message):
        self.logger.info(message)

    def debug(self, message):
        self.logger.debug(message)

    def warn(self, message):
        self.logger.warn(message)

    def error(self, message):
        self.logger.error(message)


def get_spark_configs(environment: str):
    spark_conf = SparkConf()
    if environment == "local":
        spark_conf.set("spark.app.name", "CapstoneProjectApp-LOCAL")
    else:
        spark_conf.set("spark.app.name", "CapstoneProjectApp-QA")
        spark_conf.set("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        spark_conf.set("spark.executor.cores", "1")
        spark_conf.set("spark.driver.memory", "512m")
        spark_conf.set("spark.executor.memory", "1g")
        spark_conf.set("spark.executor.memoryOverhead", "256m")
        spark_conf.set("spark.executor.instances", "1")
        spark_conf.set("spark.sql.shuffle.partitions", "100")
    return spark_conf


def get_project_configs(environment):
    if environment == 'local':
        conf = {'enable.hive': 'false', 'hive.database': 'null', 'account.filter': 'active_ind = 1', 'party.filter': '',
                'address.filter': '', 'kafka.topic': 'capstone-cloud',
                'kafka.bootstrap.servers': 'pkc-4j8dq.southeastasia.azure.confluent.cloud:9092',
                'kafka.security.protocol': 'SASL_SSL', 'kafka.sasl.mechanism': 'PLAIN',
                'kafka.client.dns.lookup': 'use_all_dns_ips',
                'kafka.sasl.jaas.config': "org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';",
                'kafka.api_key': 'V7XQGD6BF2GXJBUZ',
                'kafka.api_secret': 'vKK9V8b9Pz/7PzKAzZOJcvZ76Q9whAS7ODM8iU76v4doPmtu/Gvw3cW/RqYPyNyE'}
    else:
        conf = {'enable.hive': 'true', 'hive.database': 'capstone_db_qa', 'account.filter': 'active_ind = 1',
                'party.filter': '', 'address.filter': '', 'kafka.topic': 'capstone-cloud',
                'kafka.bootstrap.servers': 'pkc-4j8dq.southeastasia.azure.confluent.cloud:9092',
                'kafka.security.protocol': 'SASL_SSL', 'kafka.sasl.mechanism': 'PLAIN',
                'kafka.client.dns.lookup': 'use_all_dns_ips',
                'kafka.sasl.jaas.config': "org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';",
                'kafka.api_key': 'V7XQGD6BF2GXJBUZ',
                'kafka.api_secret': 'vKK9V8b9Pz/7PzKAzZOJcvZ76Q9whAS7ODM8iU76v4doPmtu/Gvw3cW/RqYPyNyE'}

    return conf


def get_data_filter(environment, data_filter: str):
    conf = get_project_configs(environment)
    return "true" if conf.get(data_filter) == "" else conf.get(data_filter)


def get_spark_session(env):
    location = "-Dlog4j.configurationFile=file:log4j2.properties"
    folder_log = "-Dspark.yarn.app.container.log.dir=app-logs"
    file_log = "-Dlogfile.name=CapstoneProject-app"

    if env == "local":
        spark = SparkSession.builder \
            .config(conf=get_spark_configs(env)) \
            .config("spark.sql.autoBroadcastJoinThreshold", -1) \
            .config("spark.sql.adaptive.enabled", False) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
            .enableHiveSupport() \
            .getOrCreate()

    else:
        spark = SparkSession.builder \
            .config(conf=get_spark_configs(env)) \
            .enableHiveSupport() \
            .getOrCreate()

    return spark


if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("Usage: main.py <mode> <date>")
        sys.exit(1)

    mode = sys.argv[1]
    load_date = sys.argv[2]

    print("Initialize Job Id..")
    job_run_id = f"Capstone-{uuid.uuid4()}"

    print("Initialize Spark Environment..")
    conf = get_project_configs(mode)
    enable_hive = True if conf.get("enable.hive") == "true" else False
    hive_db = conf.get("hive.database")
    print("Initialize Successful..")

    print("Creating Spark Session")
    spark = get_spark_session(mode)
    logger = Log4j(spark)
    logger.info(f"Spark App with ID {job_run_id}, Capstone Project is Running..")
    logger.info(f"Spark Initialize Data and Variables for Application")

    if enable_hive and not hive_db:
        logger.warn("Hive is enabled, but no Hive database is specified. This may cause failures.")

    try:
        logger.info("Spark Reading Account Data..")
        account_df = read_accounts(spark, mode, enable_hive, hive_db)

        if account_df is None or account_df.isEmpty():
            logger.warn("Account data is empty. Check the data source.")

        contract_df = get_contract(account_df)
    except Exception as e:
        logger.error(f"Failed to read or process account data: {str(e)}")
        sys.exit(1)

    try:
        logger.info("Spark Reading Party Data..")
        parties_df = read_parties(spark, mode, enable_hive, hive_db)

        if parties_df is None or parties_df.isEmpty():
            logger.warn("Party data is empty. Check the data source.")

        relations_df = get_relations(parties_df)
    except Exception as e:
        logger.error(f"Failed to read or process party data: {str(e)}")
        sys.exit(1)

    try:
        logger.info("Spark Reading Address Data..")
        address_df = read_address(spark, mode, enable_hive, hive_db)

        if address_df is None or address_df.isEmpty():
            logger.warn("Address data is empty. Check the data source.")

        relation_address_df = get_address(address_df)
    except Exception as e:
        logger.error(f"Failed to read or process address data: \n {str(e)}")
        sys.exit(1)

    try:
        logger.info("Join Party Relations and Address")
        party_address_df = join_party_address(relations_df, relation_address_df)

        logger.info("Join Account and Parties")
        data_df = join_contract_party(contract_df, party_address_df)

        logger.info("Apply Header and create Event")
        final_df = apply_header(spark, data_df)

    except Exception as e:
        logger.error(f"Failed to join or process transformations: \n {str(e)}")
        sys.exit(1)

    try:
        logger.info("Preparing to send data to Kafka")
        kafka_kv_df = final_df.select(
            f.col("payload.contractIdentifier.newValue").alias("key"),
            f.to_json(f.struct("*")).alias("value")
        )

        kafka_kv_df.show(5)
        # input("Press Any Key")

        # api_key = conf["kafka.api_key"]
        # api_secret = conf["kafka.api_secret"]
        #
        # kafka_kv_df.write \
        #     .format("kafka") \
        #     .option("kafka.bootstrap.servers", conf["kafka.bootstrap.servers"]) \
        #     .option("topic", conf["kafka.topic"]) \
        #     .option("kafka.security.protocol", conf["kafka.security.protocol"]) \
        #     .option("kafka.sasl.jaas.config", conf["kafka.sasl.jaas.config"].format(api_key, api_secret)) \
        #     .option("kafka.sasl.mechanism", conf["kafka.sasl.mechanism"]) \
        #     .option("kafka.client.dns.lookup", conf["kafka.client.dns.lookup"]) \
        #     .save()

        logger.info("Finished sending data to Kafka")
    except Exception as e:
        logger.error(f"Failed to send data to Kafka: \n {str(e)}")
        sys.exit(1)

    logger.info("Spark Terminate")
    spark.stop()

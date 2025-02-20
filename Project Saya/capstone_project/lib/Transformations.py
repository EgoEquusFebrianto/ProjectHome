from pyspark.sql import DataFrame, SparkSession, functions as f

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
    header_info = [("Capstone_Project", 1, 0),]
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
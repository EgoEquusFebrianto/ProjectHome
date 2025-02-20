from pyspark.sql.types import StructType, StructField, StringType, NullType, TimestampType, ArrayType, DateType, Row
from chispa import assert_df_equality
from datetime import datetime, date
import pytest

from lib import DataLoader, Transformations
from lib.ConfigLoader import get_project_configs
from lib.DataLoader import get_party_schema
from lib.utils import get_spark_session


@pytest.fixture(scope='session')
def spark():
    return get_spark_session("local")


@pytest.fixture(scope='session')
def expected_party_rows():
    return [
        Row(
            load_date=date(2025, 2, 18),
            account_id='6982391060',
            party_id='9823462810',
            relation_type='F-N',
            relation_start_date=datetime(2021, 2, 5, 15, 8, 5, 612000)
        ),
        Row(
            load_date=date(2025, 2, 18),
            account_id='6982391061',
            party_id='9823462811',
            relation_type='F-N',
            relation_start_date=datetime(2021, 7, 16, 16, 59, 19, 819000)
        ),
        Row(
            load_date=date(2025, 2, 18),
            account_id='6982391062',
            party_id='9823462812',
            relation_type='F-N',
            relation_start_date=datetime(2020, 12, 29, 5, 49, 57, 199000)
        ),
        Row(
            load_date=date(2025, 2, 18),
            account_id='6982391063',
            party_id='9823462813',
            relation_type='F-N',
            relation_start_date=datetime(2020, 9, 29, 4, 50, 25, 872000)
        ),
        Row(
            load_date=date(2025, 2, 18),
            account_id='6982391064',
            party_id='9823462814',
            relation_type='F-N',
            relation_start_date=datetime(2021, 8, 11, 9, 19, 18, 111000)
        ),
        Row(
            load_date=date(2025, 2, 18),
            account_id='6982391065',
            party_id='9823462815',
            relation_type='F-N',
            relation_start_date=datetime(2021, 12, 6, 0, 12, 27, 359000)
        ),
        Row(
            load_date=date(2025, 2, 18),
            account_id='6982391066',
            party_id='9823462816',
            relation_type='F-N',
            relation_start_date=datetime(2021, 12, 24, 11, 33, 48, 131000)
        ),
        Row(
            load_date=date(2025, 2, 18),
            account_id='6982391067',
            party_id='9823462817',
            relation_type='F-N',
            relation_start_date=datetime(2021, 5, 24, 11, 45, 38, 622000)
        ),
        Row(
            load_date=date(2025, 2, 18),
            account_id='6982391068',
            party_id='9823462818',
            relation_type='F-N',
            relation_start_date=datetime(2020, 11, 22, 0, 21, 23, 650000)
        ),
        Row(
            load_date=date(2025, 2, 18),
            account_id='6982391061',
            party_id='9823462820',
            relation_type='F-S',
            relation_start_date=datetime(2020, 4, 9, 18, 12, 7, 60000)
        ),
        Row(
            load_date=date(2025, 2, 18),
            account_id='6982391061',
            party_id='9823462821',
            relation_type='F-S',
            relation_start_date=datetime(2020, 7, 15, 7, 58, 1, 815000)
        )
    ]

@pytest.fixture(scope='session')
def parties_list():
    return [
        (date(2025, 2, 18), '6982391060', '9823462810', 'F-N', datetime.fromisoformat('2021-02-05T15:08:05.612+07:00')),
        (date(2025, 2, 18), '6982391061', '9823462811', 'F-N', datetime.fromisoformat('2021-07-16T16:59:19.819+07:00')),
        (date(2025, 2, 18), '6982391062', '9823462812', 'F-N', datetime.fromisoformat('2020-12-29T05:49:57.199+07:00')),
        (date(2025, 2, 18), '6982391063', '9823462813', 'F-N', datetime.fromisoformat('2020-09-29T04:50:25.872+07:00')),
        (date(2025, 2, 18), '6982391064', '9823462814', 'F-N', datetime.fromisoformat('2021-08-11T09:19:18.111+07:00')),
        (date(2025, 2, 18), '6982391065', '9823462815', 'F-N', datetime.fromisoformat('2021-12-06T00:12:27.359+07:00')),
        (date(2025, 2, 18), '6982391066', '9823462816', 'F-N', datetime.fromisoformat('2021-12-24T11:33:48.131+07:00')),
        (date(2025, 2, 18), '6982391067', '9823462817', 'F-N', datetime.fromisoformat('2021-05-24T11:45:38.622+07:00')),
        (date(2025, 2, 18), '6982391068', '9823462818', 'F-N', datetime.fromisoformat('2020-11-22T00:21:23.650+07:00')),
        (date(2025, 2, 18), '6982391061', '9823462820', 'F-S', datetime.fromisoformat('2020-04-09T18:12:07.060+07:00')),
        (date(2025, 2, 18), '6982391061', '9823462821', 'F-S', datetime.fromisoformat('2020-07-15T07:58:01.815+07:00'))
    ]


@pytest.fixture(scope='session')
def expected_contract_df(spark):
    schema = StructType([
        StructField(
            'account_id',
            StringType()
        ),
        StructField(
            'contractIdentifier',
            StructType([
                StructField('operation', StringType()),
                StructField('newValue', StringType()),
                StructField('oldValue', NullType())
            ])
        ),
        StructField(
            'sourceSystemIdentifier',
            StructType([
                StructField('operation', StringType()),
                StructField('newValue', StringType()),
                StructField('oldValue', NullType())
            ])
        ),
        StructField(
            'contactStartDateTime',
            StructType([
                StructField('operation', StringType()),
                StructField('newValue', TimestampType()),
                StructField('oldValue', NullType())
            ])
        ),
        StructField(
            'contractTitle',
            StructType([
                StructField('operation', StringType()),
                StructField('newValue', ArrayType(StructType([
                    StructField('contractTitleLineType', StringType()),
                    StructField('contractTitleLine', StringType())
                ]))),
                StructField('oldValue', NullType())
            ])
        ),
        StructField(
            'taxIdentifier',
            StructType([
                StructField('operation', StringType()),
                StructField('newValue', StructType([
                    StructField('taxIdType', StringType()),
                    StructField('taxId', StringType())
                ])),
                StructField('oldValue', NullType())
            ])
        ),
        StructField(
            'contractBranchCode',
            StructType([
                StructField('operation', StringType()),
                StructField('newValue', StringType()),
                StructField('oldValue', NullType())
            ])
        ),
        StructField(
            'contractCountry',
            StructType([
                StructField('operation', StringType()),
                StructField('newValue', StringType()),
                StructField('oldValue', NullType())
            ])
        )
    ])
    return spark.read.format("json") \
        .schema(schema) \
        .load("data/results/contract_df.json")


@pytest.fixture(scope='session')
def expected_final_df(spark):
    schema = StructType([
        StructField(
            'keys',
            ArrayType(StructType([
                StructField('keyField', StringType()),
                StructField('keyValue', StringType())
            ]))
        ),
        StructField(
            'payload',
            StructType([
                StructField('contractIdentifier', StructType([
                    StructField('operation', StringType()),
                    StructField('newValue', StringType()),
                    StructField('oldValue', NullType())
                ])),
                StructField('sourceSystemIdentifier',StructType([
                    StructField('operation', StringType()),
                    StructField('newValue', StringType()),
                    StructField('oldValue', NullType())
                ])),
                StructField('contactStartDateTime',StructType([
                    StructField('operation', StringType()),
                    StructField('newValue', TimestampType()),
                    StructField('oldValue', NullType())
                ])),
                StructField('contractTitle', StructType([
                    StructField('operation', StringType()),
                    StructField('newValue', ArrayType( StructType([
                        StructField('contractTitleLineType', StringType()),
                        StructField('contractTitleLine', StringType())
                    ]))),
                    StructField('oldValue', NullType())
                ])),
                StructField('taxIdentifier', StructType([
                    StructField('operation', StringType()),
                    StructField('newValue', StructType([
                        StructField('taxIdType', StringType()),
                        StructField('taxId', StringType())
                    ])),
                    StructField('oldValue', NullType())
                ])),
                StructField('contractBranchCode', StructType([
                    StructField('operation', StringType()),
                    StructField('newValue', StringType()),
                    StructField('oldValue', NullType())
                ])),
                StructField('contractCountry', StructType([
                    StructField('operation', StringType()),
                    StructField('newValue', StringType()),
                    StructField('oldValue', NullType())
                ])),
                StructField('partyRelations', ArrayType(StructType([
                    StructField('partyIdentifier', StructType([
                        StructField('operation', StringType()),
                        StructField('newValue', StringType()),
                        StructField('oldValue', NullType())
                    ])),
                    StructField('partyRelationshipType', StructType([
                        StructField('operation', StringType()),
                        StructField('newValue', StringType()),
                        StructField('oldValue', NullType())
                    ])),
                    StructField('partyRelationStartDateTime', StructType([
                        StructField('operation', StringType()),
                        StructField('newValue', TimestampType()),
                        StructField('oldValue', NullType())
                    ])),
                    StructField('partyAddress', StructType([
                        StructField('operation', StringType()),
                        StructField('newValue', StructType([
                            StructField('addressLine1', StringType()),
                            StructField('addressLine2', StringType()),
                            StructField('addressCity', StringType()),
                            StructField('addressPostalCode',StringType()),
                            StructField('addressCountry', StringType()),
                            StructField('addressStartDate', DateType())
                        ])),
                        StructField('oldValue', NullType())
                    ]))
                ])))
            ])
        )
    ])
    return spark.read.format("json") \
        .schema(schema) \
        .load("data/results/final_df.json") \
        .select("keys", "payload")


def test_blank_test(spark):
    print(spark.version)
    assert spark.version == "3.5.3"


def test_get_project_configs():
    conf_local = get_project_configs("local")
    conf_qa = get_project_configs("QA")
    assert conf_local["kafka.topic"] == "capstone-cloud"
    assert conf_qa["hive.database"] == "capstone_db_qa"


def test_read_accounts(spark):
    accounts_df = DataLoader.read_accounts(spark, "local", False, None)
    assert accounts_df.count() == 8


def test_read_parties_row(spark, expected_party_rows):
    actual_party_rows = DataLoader.read_parties(spark, "local", False, None).collect()
    assert expected_party_rows == actual_party_rows


def test_read_parties(spark, parties_list):
    expected_df = spark.createDataFrame(parties_list)
    actual_df = DataLoader.read_parties(spark, "local", False, None)
    assert_df_equality(expected_df, actual_df, ignore_schema=True)


def test_read_party_schema(spark, parties_list):
    expected_df = spark.createDataFrame(parties_list, get_party_schema())
    actual_df = DataLoader.read_parties(spark, "local", False, None)
    assert_df_equality(expected_df, actual_df)


def test_get_contract(spark, expected_contract_df):
    accounts_df = DataLoader.read_accounts(spark, "local", False, None)
    actual_contract_df = Transformations.get_contract(accounts_df)
    assert expected_contract_df.collect() == actual_contract_df.collect()
    assert_df_equality(expected_contract_df, actual_contract_df, ignore_schema=True)


def test_kafka_kv_df(spark, expected_final_df):
    accounts_df = DataLoader.read_accounts(spark, "local", False, None)
    contract_df = Transformations.get_contract(accounts_df)
    parties_df = DataLoader.read_parties(spark, "local", False, None)
    relations_df = Transformations.get_relations(parties_df)
    address_df = DataLoader.read_address(spark, "local", False, None)
    relation_address_df = Transformations.get_address(address_df)
    party_address_df = Transformations.join_party_address(relations_df, relation_address_df)
    data_df = Transformations.join_contract_party(contract_df, party_address_df)
    actual_final_df = Transformations.apply_header(spark, data_df) \
        .select("keys", "payload")
    assert_df_equality(actual_final_df, expected_final_df, ignore_schema=True)

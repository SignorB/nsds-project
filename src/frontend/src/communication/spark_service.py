import json

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from .config import KAFKA_BOOTSTRAP

_spark: SparkSession | None = None


def _get_spark() -> SparkSession:
    global _spark
    if _spark is None:
        _spark = (
            SparkSession.builder
            .appName("nsds-frontend")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",
            )
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def _read_topic_batch(topic: str) -> list[dict]:
    spark = _get_spark()

    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    rows = (
        df.select(
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp"),
            F.col("value").cast(StringType()).alias("raw"),
        )
        .orderBy("partition", "offset")
        .collect()
    )

    messages = []
    for row in rows:
        try:
            messages.append(json.loads(row["raw"]))
        except Exception:
            pass
    return messages


def _apply_user_events(events: list[dict]) -> pd.DataFrame:
    users: dict[str, dict] = {}
    for ev in events:
        try:
            et = ev.get("eventType", "").lower()
            payload = ev.get("payload", {})
            uid = payload.get("userId")
            if not uid:
                continue
            if et in ("userregistered", "userupdated"):
                users[uid] = {
                    "userId":   uid,
                    "email":    payload.get("email", ""),
                    "fullName": payload.get("fullName", ""),
                }
            elif et == "userremoved":
                users.pop(uid, None)
        except Exception:
            print(f"Error processing event: {ev}")

    if not users:
        return pd.DataFrame(columns=["userId", "email", "fullName"])
    return pd.DataFrame(list(users.values()))


def _apply_node_events(events: list[dict]) -> pd.DataFrame:
    nodes: dict[str, dict] = {}
    for ev in events:
        et = ev.get("eventType", "").lower()
        payload = ev.get("payload", {})
        nid = payload.get("nodeId")
        if not nid:
            continue
        if et in ("nodecreated", "nodeupdated"):
            nodes[nid] = {
                "nodeId":     nid,
                "userId":     payload.get("userId", ""),
                "districtId": payload.get("districtId", ""),
                "nodeType":   payload.get("nodeType", ""),
            }
        elif et == "nodedeleted":
            nodes.pop(nid, None)

    if not nodes:
        return pd.DataFrame(columns=["nodeId", "userId", "districtId", "nodeType"])
    return pd.DataFrame(list(nodes.values()))


def _flatten_monitoring_events(events: list[dict]) -> pd.DataFrame:
    rows = []
    for ev in events:
        row = {k: v for k, v in ev.items() if k != "payload"}
        payload = ev.get("payload", {})
        if isinstance(payload, dict):
            for pk, pv in payload.items():
                row[f"payload.{pk}"] = pv
        rows.append(row)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_users_df() -> pd.DataFrame:
    from .client import TOPIC_USER_LIFECYCLE
    events = _read_topic_batch(TOPIC_USER_LIFECYCLE)
    return _apply_user_events(events)


def get_nodes_df() -> pd.DataFrame:
    from .client import TOPIC_NODE_LIFECYCLE
    events = _read_topic_batch(TOPIC_NODE_LIFECYCLE)
    return _apply_node_events(events)


def get_measurements_df(max_messages: int = 500) -> pd.DataFrame:
    from .client import TOPIC_MEASUREMENT_EVENTS
    events = _read_topic_batch(TOPIC_MEASUREMENT_EVENTS)
    return _flatten_monitoring_events(events[:max_messages])


def get_usage_records_df(max_messages: int = 500) -> pd.DataFrame:
    from .client import TOPIC_USAGE_RECORDS
    events = _read_topic_batch(TOPIC_USAGE_RECORDS)
    return _flatten_monitoring_events(events[:max_messages])


_MEASUREMENT_SCHEMA = StructType([
    StructField("nodeId",     StringType(), True),
    StructField("nodeType",   StringType(), True),
    StructField("districtId", StringType(), True),
    StructField("value",      DoubleType(), True),
    StructField("measuredAt", LongType(),   True),
])


def get_district_energy_state_df(max_messages: int = 2000) -> pd.DataFrame:
    from .client import TOPIC_MEASUREMENT_EVENTS

    spark = _get_spark()
    events = _read_topic_batch(TOPIC_MEASUREMENT_EVENTS)
    if not events:
        return pd.DataFrame(
            columns=["districtId", "total_produced", "total_consumed",
                     "state_of_charge", "net_balance"]
        )

    rows: list[tuple] = []
    for ev in events[:max_messages]:
        payload = ev.get("payload", {})
        rows.append((
            str(payload.get("nodeId",     "")),
            str(payload.get("nodeType",   "")),
            str(payload.get("districtId", "")),
            float(payload.get("value",     0.0)),
            int(payload.get("measuredAt",  0)),
        ))

    if not rows:
        return pd.DataFrame(
            columns=["districtId", "total_produced", "total_consumed",
                     "state_of_charge", "net_balance"]
        )

    sdf = spark.createDataFrame(rows, schema=_MEASUREMENT_SCHEMA)

    prod_df = (
        sdf.filter(F.col("nodeType") == "producer")
        .groupBy("districtId")
        .agg(F.sum("value").alias("total_produced"))
    )

    cons_df = (
        sdf.filter(F.col("nodeType") == "consumer")
        .groupBy("districtId")
        .agg(F.sum(F.abs(F.col("value"))).alias("total_consumed"))
    )

    acc_window = Window.partitionBy("nodeId").orderBy(F.col("measuredAt").desc())
    acc_df = (
        sdf.filter(F.col("nodeType") == "accumulator")
        .withColumn("_rn", F.row_number().over(acc_window))
        .filter(F.col("_rn") == 1)
        .groupBy("districtId")
        .agg(F.sum("value").alias("state_of_charge"))
    )

    all_districts = sdf.select("districtId").distinct()
    result_sdf = (
        all_districts
        .join(prod_df, "districtId", "left")
        .join(cons_df, "districtId", "left")
        .join(acc_df,  "districtId", "left")
        .fillna(0.0)
        .withColumn("net_balance",
                    F.col("total_produced") - F.col("total_consumed"))
        .orderBy("districtId")
    )

    return result_sdf.toPandas()


_USAGE_SCHEMA = StructType([
    StructField("districtId",  StringType(), True),
    StructField("windowStart", LongType(),   True),
    StructField("windowEnd",   LongType(),   True),
    StructField("produced",    DoubleType(), True),
    StructField("consumed",    DoubleType(), True),
    StructField("netBalance",  DoubleType(), True),
])


def get_sliding_window_balance_df(window_minutes: int = 10) -> pd.DataFrame:
    from .client import TOPIC_USAGE_RECORDS

    spark = _get_spark()
    events = _read_topic_batch(TOPIC_USAGE_RECORDS)
    if not events:
        return pd.DataFrame(
            columns=["districtId", "window_start", "avg_net_balance"]
        )

    rows: list[tuple] = []
    for ev in events:
        payload = ev.get("payload", {})
        rows.append((
            str(payload.get("districtId", "")),
            int(payload.get("windowStart", 0)),
            int(payload.get("windowEnd",   0)),
            float(payload.get("produced",   0.0)),
            float(payload.get("consumed",   0.0)),
            float(payload.get("netBalance", 0.0)),
        ))

    if not rows:
        return pd.DataFrame(
            columns=["districtId", "window_start", "avg_net_balance"]
        )

    sdf = spark.createDataFrame(rows, schema=_USAGE_SCHEMA)

    sdf = sdf.withColumn(
        "event_time",
        (F.col("windowStart") / 1000).cast("timestamp"),
    )

    slide_minutes = max(1, window_minutes // 2)
    result_sdf = (
        sdf.groupBy(
            "districtId",
            F.window(
                "event_time",
                f"{window_minutes} minutes",
                f"{slide_minutes} minutes",
            ),
        )
        .agg(F.avg("netBalance").alias("avg_net_balance"))
        .withColumn("window_start", F.col("window.start"))
        .select("districtId", "window_start", "avg_net_balance")
        .orderBy("districtId", "window_start")
    )

    return result_sdf.toPandas()

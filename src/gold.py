import dlt
from pyspark.sql import functions as F


# ===========================================================================
# GOLD — KPIs DIARIOS POR COMERCIO Y CANAL
# ===========================================================================

@dlt.table(
    name="gold.daily_transaction_kpis",
    comment="KPIs diarios de transacciones por comercio, canal y moneda.",
    table_properties={"quality": "gold"},
    partition_cols=["transaction_date"]
)
def gold_daily_transaction_kpis():

    df_transactions = dlt.read("silver.transactions")
    df_kpis = (
        df_transactions
        .groupBy(
            "transaction_date",
            "merchant_id",
            "channel",
            "currency"
        )
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum(
                F.when(
                    F.col("transaction_type") == "pago",
                    1
                ).otherwise(0)
            ).alias("total_payments"),
            F.sum(
                F.when(
                    F.col("transaction_type") == "reversa",
                    1
                ).otherwise(0)
            ).alias("total_reversals"),
            F.sum(
                F.when(
                    F.col("transaction_type") == "retiro",
                    1
                ).otherwise(0)
            ).alias("total_withdrawals"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("average_amount"),
            F.sum(
                F.when(
                    F.col("transaction_type") == "reversa",
                    F.col("amount")
                ).otherwise(0)
            ).alias("reversed_amount"),
            F.sum(
                F.when(
                    F.col("status") == "rechazado",
                    1
                ).otherwise(0)
            ).alias("total_rejected_transactions")
        )

        # ===============================================================
        # TASA DE REVERSA
        # ===============================================================

        .withColumn(
            "reversal_rate",
            F.when(
                F.col("total_payments") > 0,
                F.col("total_reversals") /
                F.col("total_payments")
            ).otherwise(F.lit(0.0))
        )

        # ===============================================================
        # SCORE DE RIESGO
        # ===============================================================

        .withColumn(
            "risk_score",
            F.least(
                F.lit(100.0),
                (
                    F.col("reversal_rate") * 60
                )
                +
                (
                    F.when(
                        F.col("reversed_amount") > 50000,
                        40
                    )
                    .when(
                        F.col("reversed_amount") > 10000,
                        20
                    )
                    .otherwise(0)
                )
            )
        )

        .withColumn(
            "_processed_at",
            F.current_timestamp()
        )
    )
    return df_kpis


# ===========================================================================
# GOLD — RESUMEN HISTÓRICO DE COMERCIOS
# ===========================================================================

@dlt.table(
    name="gold.merchant_risk_summary",
    comment="Resumen histórico de actividad y riesgo por comercio.",
    table_properties={"quality": "gold"}
)
def gold_merchant_risk_summary():
    df_daily_kpis = dlt.read("gold.daily_transaction_kpis")
    df_merchants = dlt.read("silver.merchants")
    df_summary = (
        df_daily_kpis
        .groupBy("merchant_id")
        .agg(
            F.sum("total_transactions")
            .alias("historical_transactions"),
            F.sum("total_reversals")
            .alias("historical_reversals"),
            F.sum("total_amount")
            .alias("historical_amount"),
            F.avg("reversal_rate")
            .alias("average_reversal_rate"),
            F.max("risk_score")
            .alias("maximum_risk_score"),
            F.countDistinct("transaction_date")
            .alias("active_days")
        )
    )

    df_merchant_summary = (
        df_merchants
        .join(
            df_summary,
            on="merchant_id",
            how="left"
        )
        .withColumn(
            "_processed_at",
            F.current_timestamp()
        )
    )

    return df_merchant_summary


# ===========================================================================
# GOLD — ALERTAS DE FRAUDE
# ===========================================================================

@dlt.table(
    name="gold.fraud_detection_alerts",
    comment="Alertas de posible fraude basadas en indicadores de riesgo.",
    table_properties={"quality": "gold"}
)
def gold_fraud_detection_alerts():
    df_kpis = dlt.read("gold.daily_transaction_kpis")
    df_alerts = (
        df_kpis
        .filter(
            (F.col("reversal_rate") > 0.30) |
            (F.col("risk_score") >= 60)
        )
        .withColumn(
            "alert_reason",
            F.concat_ws(
                ",",
                F.when(
                    F.col("reversal_rate") > 0.30,
                    F.lit("high_reversal_rate")
                ),
                F.when(
                    F.col("risk_score") >= 60,
                    F.lit("high_risk_score")
                )
            )
        )
        .withColumn(
            "_processed_at",
            F.current_timestamp()
        )
    )

    return df_alerts
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, DateType


# ===========================================================================
# TABLA DE CUARENTENA
# ===========================================================================

dlt.create_streaming_table(
    name="silver.quarantine",
    comment="Registros rechazados por reglas de calidad en Silver.",
    table_properties={"quality": "silver"},
    schema="""
        source_name       STRING,
        rejection_reason  STRING,
        original_record   STRING,
        processed_at      TIMESTAMP
    """
)


def _quarantine(df, source_name: str, reason_col: str):
    """
    Devuelve un dataframe con el esquema estándar de cuarentena.
    """
    return df.select(
        F.lit(source_name).alias("source_name"),
        F.col(reason_col).alias("rejection_reason"),
        F.to_json(F.struct("*")).alias("original_record"),
        F.current_timestamp().alias("processed_at")
    )

# ===========================================================================
# SILVER — TRANSACTIONS
# ===========================================================================

@dlt.table(
    name="silver.transactions_validated",
    comment="Transacciones validadas temporalmente en Silver",
    table_properties={"pipelines.reset.allowed": "true"}
)
def transactions_validated():
    df = dlt.read_stream("bronze.transactions")
    if "_ingestion_timestamp" in df.columns:
        df = df.drop("_ingestion_timestamp")
        
    # ==========================================================
    # NORMALIZACIÓN MONTO
    # ==========================================================

    df = df.withColumn(
        "amount",
        F.when(
            F.col("amount").contains(",") &
            F.col("amount").contains("."),
            F.regexp_replace(
                F.col("amount").cast("string"),
                r"\.",
                ""
            )
        ).otherwise(
            F.col("amount").cast("string")
        )
    )

    df = df.withColumn(
        "amount",
        F.regexp_replace(F.col("amount"), ",", ".")
    )

    df = df.withColumn(
        "amount",
        F.col("amount").cast(DoubleType())
    )

    # ==========================================================
    # NORMALIZACIÓN TEXTO
    # ==========================================================

    for col in ("transaction_type", "channel", "status"):
        df = df.withColumn(
            col,
            F.lower(F.trim(F.col(col)))
        )

    df = df.withColumn(
        "transaction_type",
        F.when(
            F.col("transaction_type") == "payment",
            "pago"
        ).otherwise(F.col("transaction_type"))
    )

    # ==========================================================
    # NORMALIZACIÓN MONEDA
    # ==========================================================

    currency_map = {
        "sol": "PEN",
        "soles": "PEN",
        "us$": "USD"
    }

    df = df.withColumn(
        "currency",
        F.upper(F.trim(F.col("currency")))
    )

    for alias, iso in currency_map.items():

        df = df.withColumn(
            "currency",
            F.when(
                F.col("currency") == alias.upper(),
                iso
            ).otherwise(F.col("currency"))
        )

    # ==========================================================
    # FECHA
    # ==========================================================

    df = df.withColumn(
        "transaction_date",
        F.coalesce(
            F.to_date(
                F.col("transaction_date"),
                "yyyy-MM-dd"
            ),
            F.to_date(
                F.col("transaction_date"),
                "dd/MM/yyyy"
            )
        ).cast(DateType())
    )

    # ==========================================================
    # DEDUPLICACIÓN
    # ==========================================================

    df = df.dropDuplicates(["transaction_id"])

    # ==========================================================
    # VALIDACIONES + MOTIVO DE RECHAZO
    # ==========================================================

    df = df.withColumn(
        "rejection_reason",

        F.when(
            F.col("transaction_id").isNull(),
            "transaction_id es nulo"
        )

        .when(
            ~F.col("transaction_id").rlike(
                r"^TXN-\d{8}-\d{5}$"
            ),
            "formato transaction_id invalido"
        )

        .when(
            F.col("user_id").isNull(),
            "user_id es nulo"
        )

        .when(
            ~F.col("user_id").rlike(
                r"^USR-\d{6}$"
            ),
            "formato user_id invalido"
        )

        .when(
            F.col("merchant_id").isNull(),
            "merchant_id es nulo"
        )

        .when(
            ~F.col("merchant_id").rlike(
                r"^MCH-\d{5}$"
            ),
            "formato merchant_id invalido"
        )

        .when(
            F.col("amount").isNull(),
            "amount es nulo"
        )

        .when(
            F.col("amount") <= 0,
            "amount debe ser mayor a 0"
        )

        .when(
            ~F.col("transaction_type").isin(
                "pago",
                "reversa",
                "retiro"
            ),
            "tipo_transaccion invalido"
        )

        .when(
            ~F.col("channel").isin(
                "web",
                "app",
                "pos"
            ),
            "canal invalido"
        )

        .when(
            ~F.col("status").isin(
                "aprobado",
                "rechazado",
                "pendiente"
            ),
            "estado invalido"
        )

        .when(
            ~F.col("currency").isin(
                "PEN",
                "USD",
                "COP",
                "MXN",
                "CLP",
                "ARS"
            ),
            "moneda invalida"
        )

        .when(
            F.col("transaction_date").isNull(),
            "transaction_date invalida"
        )

        .when(
            (F.col("transaction_type") == "reversa") &
            F.col("reference_id").isNull(),
            "reversa requiere reference_id"
        )

        .when(
            (F.col("transaction_type") != "reversa") &
            F.col("reference_id").isNotNull(),
            "reference_id debe ser nulo"
        )

        .otherwise(F.lit(None))
    )

    return df


@dlt.table(
    name="silver.transactions",
    comment="Transacciones limpias y validadas.",
    table_properties={"quality": "silver"}
)
def silver_transactions():

    return (
        dlt.read_stream("silver.transactions_validated")
        .filter("rejection_reason IS NULL")
        .drop("rejection_reason")
    )


@dlt.append_flow(target="silver.quarantine")
def quarantine_transactions():

    bad = (
        dlt.read_stream("silver.transactions_validated")
        .filter("rejection_reason IS NOT NULL")
    )

    return _quarantine(
        bad,
        "transactions",
        "rejection_reason"
    )


# ===========================================================================
# SILVER — MERCHANTS
# ===========================================================================

@dlt.table(
    name="silver.merchants_validated",
    comment="Comercios validados temporalmente en Silver",
    table_properties={"pipelines.reset.allowed": "true"}
)
def merchants_validated():

    df = dlt.read_stream("bronze.merchants")

    if "_ingestion_timestamp" in df.columns:
        df = df.drop("_ingestion_timestamp")

    # ==========================================================
    # NORMALIZACIÓN TEXTO
    # ==========================================================

    for col in ("category", "status", "risk_level"):
        df = df.withColumn(
            col,
            F.lower(F.trim(F.col(col)))
        )

    df = df.withColumn(
        "status",
        F.when(
            F.col("status") == "suspended",
            "suspendido"
        ).otherwise(F.col("status"))
    )

    # ==========================================================
    # NORMALIZACIÓN COUNTRY
    # ==========================================================

    country_map = {
        "peru": "PE",
        "per": "PE",
        "colombia": "CO",
        "col": "CO",
        "mexico": "MX",
        "mex": "MX",
        "chile": "CL",
        "chi": "CL",
        "argentina": "AR",
        "arg": "AR",
    }

    df = df.withColumn(
        "country",
        F.lower(F.trim(F.col("country")))
    )

    for alias, iso in country_map.items():

        df = df.withColumn(
            "country",
            F.when(
                F.col("country") == alias,
                iso
            ).otherwise(F.col("country"))
        )

    df = df.withColumn(
        "country",
        F.upper(F.col("country"))
    )

    # ==========================================================
    # FECHA
    # ==========================================================

    df = df.withColumn(
        "affiliation_date",
        F.coalesce(
            F.to_date(
                F.col("affiliation_date"),
                "yyyy-MM-dd"
            ),
            F.to_date(
                F.col("affiliation_date"),
                "dd/MM/yyyy"
            )
        ).cast(DateType())
    )

    # ==========================================================
    # DEDUPLICACIÓN
    # ==========================================================

    df = df.dropDuplicates(["merchant_id"])

    # ==========================================================
    # VALIDACIONES
    # ==========================================================

    df = df.withColumn(
        "rejection_reason",

        F.when(
            F.col("merchant_id").isNull(),
            "merchant_id es nulo"
        )

        .when(
            ~F.col("merchant_id").rlike(
                r"^MCH-\d{5}$"
            ),
            "formato merchant_id invalido"
        )

        .when(
            F.col("merchant_name").isNull() |
            (F.trim(F.col("merchant_name")) == ""),
            "merchant_name vacio"
        )

        .when(
            ~F.col("category").isin(
                "retail",
                "restaurante",
                "farmacia",
                "supermercado",
                "tecnologia",
                "transporte",
                "educacion",
                "salud",
                "entretenimiento",
                "moda"
            ),
            "categoria invalida"
        )

        .when(
            ~F.col("country").isin(
                "PE",
                "CO",
                "MX",
                "CL",
                "AR"
            ),
            "pais invalido"
        )

        .when(
            F.col("affiliation_date").isNull(),
            "affiliation_date invalida"
        )

        .when(
            ~F.col("status").isin(
                "activo",
                "inactivo",
                "suspendido"
            ),
            "estado invalido"
        )

        .when(
            F.col("risk_level").isNotNull() &
            ~F.col("risk_level").isin(
                "bajo",
                "medio",
                "alto"
            ),
            "risk_level invalido"
        )

        .otherwise(F.lit(None))
    )

    return df


@dlt.table(
    name="silver.merchants",
    comment="Comercios limpios.",
    table_properties={"quality": "silver"}
)
def silver_merchants():

    return (
        dlt.read_stream("silver.merchants_validated")
        .filter("rejection_reason IS NULL")
        .drop("rejection_reason")
    )


@dlt.append_flow(target="silver.quarantine")
def quarantine_merchants():

    bad = (
        dlt.read_stream("silver.merchants_validated")
        .filter("rejection_reason IS NOT NULL")
    )

    return _quarantine(
        bad,
        "merchants",
        "rejection_reason"
    )

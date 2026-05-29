# ============================================
# bronze.py
# Proyecto: FinPay Lakehouse
# ============================================

import json,dlt
from pyspark.sql.functions import current_timestamp, col

# ============================================
# CONFIGURACION BASE
# ============================================

#CATALOG_NAME = "adbanalitycs"
INGESTION_CONFIG_PATH = ("/Volumes/adbanalitycs/default/vol_landing/metadata/ingestion_archetypes_editado.json")


# ============================================
# LEER ARCHIVO JSON DE CONFIGURACION
# ============================================

config_content = dbutils.fs.head(INGESTION_CONFIG_PATH, 100000)
sources = json.loads(config_content)

# ============================================
# CREAR TABLAS DINÁMICAMENTE
# ============================================

for source in sources:
    # ========================================
    # VALIDAR SI ESTA ACTIVA
    # ========================================
    if not source["active"]:
        continue

    source_name      = source["source_name"]
    source_path      = source["source_path"]
    file_format      = source["file_format"]
    #delimiter        = source["delimiter"]
    #header           = source["header"]
    #multiline        = source["multiline"]
    schema_location  = source["schema_location"]
    #checkpoint_path  = source["checkpoint_path"]
    #target_table     = source["target_table"]
    #partition_by     = source["partition_by"]

    #effective_format = "csv" if  file_format == "text" else file_format

    def create_table(
        source_name=source_name,
        source_path=source_path,
        #effective_format=effective_format,
        #delimiter=delimiter,
        #header=header,
        #multiline=multiline,
        file_format=file_format,
        schema_location=schema_location
    ):

        @dlt.table(
            name=source_name,
            comment=f"Bronze table for {source_name}"
        )
        def bronze_table():
            reader = (
                spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", file_format)
                .option("cloudFiles.schemaLocation", schema_location)
                .option("cloudFiles.inferColumnTypes", "true")
                #.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            )

            #if effective_format == "csv":
            #    reader = (
            #        reader
            #        .option("header", header)
            #        .option("delimiter", delimiter)
            #    )

            if file_format == "json":
                reader = (
                    reader
                    .option("multiline", True)
                    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                )

            elif file_format == "csv":
                reader = (
                    reader
                    .option("header",True)
                    #.option("inferSchema",False)
                    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                )

            elif file_format == "text":
                pass
            
            df = reader.load(source_path)

            return (
                df
                .withColumn("_ingestion_timestamp", current_timestamp())
                .withColumn("_source_file", col("_metadata.file_name"))
            )

    create_table()

"""
Ingestao da bronze: full-load e CDC (Auto Loader + MERGE).

A deduplicacao de cada micro-batch usa window function em vez de global
temp view, que nao e suportada no compute serverless da Free Edition.
"""

import delta
import pyspark.sql.functions as F
from pyspark.sql import Window

import utils


class Ingestor:
    """Carga full: le tudo e sobrescreve a tabela destino."""

    def __init__(self, spark, catalog, schemaname, tablename, data_format="parquet"):
        self.spark = spark
        self.catalog = catalog
        self.schemaname = schemaname
        self.tablename = tablename
        self.format = data_format
        self.data_schema = utils.import_schema(tablename)

    @property
    def full_table_name(self) -> str:
        return f"{self.catalog}.{self.schemaname}.{self.tablename}"

    def load(self, path):
        return (
            self.spark.read
            .format(self.format)
            .schema(self.data_schema)
            .load(path)
        )

    def save(self, df):
        (
            df.coalesce(1)
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(self.full_table_name)
        )
        return True

    def execute(self, path):
        return self.save(self.load(path))


class IngestorCDC(Ingestor):
    """Le so os arquivos novos de CDC e aplica I / U / D na bronze via MERGE."""

    def __init__(self, spark, catalog, schemaname, tablename,
                 id_field, timestamp_field, checkpoint_location,
                 data_format="parquet"):
        super().__init__(spark, catalog, schemaname, tablename, data_format)
        self.id_field = id_field
        self.timestamp_field = timestamp_field
        self.checkpoint_location = checkpoint_location
        self.deltatable = delta.DeltaTable.forName(spark, self.full_table_name)

    def dedup(self, df):
        """Mantem so a ultima versao de cada chave dentro do lote."""
        janela = Window.partitionBy(self.id_field).orderBy(
            F.col(self.timestamp_field).desc()
        )
        return (
            df.withColumn("_rn", F.row_number().over(janela))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

    def upsert(self, df):
        df_cdc = self.dedup(df)

        (
            self.deltatable.alias("b")
            .merge(df_cdc.alias("d"), f"b.{self.id_field} = d.{self.id_field}")
            .whenMatchedDelete(condition="d.OP = 'D'")
            .whenMatchedUpdateAll(condition="d.OP = 'U'")
            .whenNotMatchedInsertAll(condition="d.OP = 'I' OR d.OP = 'U'")
            .execute()
        )

    def load(self, path):
        return (
            self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", self.format)
            .schema(self.data_schema)
            .load(path)
        )

    def save(self, df):
        stream = (
            df.writeStream
            .option("checkpointLocation", self.checkpoint_location)
            .foreachBatch(lambda batch_df, batch_id: self.upsert(batch_df))
            .trigger(availableNow=True)
        )
        return stream.start()

    def execute(self, path):
        query = self.save(self.load(path))
        query.awaitTermination()  # no Job, a tarefa so termina quando a stream acaba
        return query

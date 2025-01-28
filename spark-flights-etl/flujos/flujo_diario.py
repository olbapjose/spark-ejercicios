
class FlujoDiario:
    """
    ETL diaria
    """

    def __init__(self, spark, properties):
        self.properties = properties
        self.spark = spark

    def run(self):
        flights_df = self.spark.read\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .csv(self.properties["raw_input_file"])
        
        flights_df.printSchema()
        print(flights_df.limit(10).toPandas())

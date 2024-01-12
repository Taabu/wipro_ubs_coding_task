import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, BooleanType
from db.database_handler import DatabaseHandler
from configparser import ConfigParser


class InstrumentCalculator:
    def __init__(self, spark, config):
        self.spark = spark
        self.database_handler = None
        self.config = config
        self.logger = logging.getLogger(__name__)

    def connect_db(self):
        self.connect_to_database()
        self.initialize_multipliers()
        self.logger.info("Database connection and initialization completed successfully.")

    def connect_to_database(self):
        database_path = self.config.get('Database', 'database_path')
        self.database_handler = DatabaseHandler(database_path)
        self.logger.info("Connecting to the database...")
        self.database_handler.connect()
        self.database_handler.create_database()

    def initialize_multipliers(self):
        self.database_handler.add_or_update_multiplier("INSTRUMENT1", 1.6)
        self.database_handler.add_or_update_multiplier("INSTRUMENT3", 0.8)

    def read_data(self):
        input_path = self.config.get('General', 'input_path')
        self.logger.info(f"Reading data from: {input_path}")
        schema = StructType([
            StructField("instrument", StringType(), True),
            StructField("date", DateType(), True),
            StructField("value", DoubleType(), True),
        ])

        # Read the data and repartition based on the instrument column
        data = (self.spark.read
                .csv(input_path, header=False, schema=schema, dateFormat="dd-MMM-yyyy")
                .repartition("instrument"))

        return data

    def calculate_metrics(self, data):
        self.logger.info("Calculating metrics...")
        # Filter non-business dates
        data = data.filter(self.is_business_date(data.date))

        # Apply modifiers from the database
        data = self.apply_modifiers(data)

        # Calculate metrics
        instrument1_mean = self.calculate_mean(data, "INSTRUMENT1")
        instrument2_mean_november = self.calculate_mean_for_month(data, "INSTRUMENT2", 11, 2014)
        instrument3_std_dev = self.calculate_std_dev(data, "INSTRUMENT3")
        newest_10_sum = self.calculate_newest_sum(data, "instrument", 10)

        self.display_results(instrument1_mean, "Mean for INSTRUMENT1:")
        self.display_results(instrument2_mean_november, "Mean for INSTRUMENT2 in November 2014:")
        self.display_results(instrument3_std_dev, "Standard Deviation for INSTRUMENT3:")
        self.display_results(newest_10_sum, "Sum of the newest 10 elements for other instruments:")

        return data

    def calculate_mean(self, data, instrument):
        return data.filter(data["instrument"] == instrument).agg(
            F.mean("value").alias(f"mean_value_{instrument}"),
            F.mean("multiplied_value").alias(f"mean_multiplied_value_{instrument}")
        )

    def calculate_mean_for_month(self, data, instrument, month, year):
        return data.filter(
            (data["instrument"] == instrument) & (F.month(data["date"]) == month) & (F.year(data["date"]) == year)).agg(
            F.mean("value").alias(f"mean_value_{instrument}_november"),
            F.mean("multiplied_value").alias(f"mean_multiplied_value_{instrument}_november")
        )

    def calculate_std_dev(self, data, instrument):
        return data.filter(data["instrument"] == instrument).agg(
            F.stddev("value").alias(f"std_dev_value_{instrument}"),
            F.stddev("multiplied_value").alias(f"std_dev_multiplied_value_{instrument}")
        )

    def calculate_newest_sum(self, data, partition_column, n):
        window_spec = Window.partitionBy(partition_column).orderBy(F.col("date").desc())
        data = data.withColumn("row_number", F.row_number().over(window_spec))
        return data.filter(F.col("row_number") <= n).groupBy(partition_column).agg(
            F.sum("value").alias(f"sum_value_newest_{n}"),
            F.sum("multiplied_value").alias(f"sum_multiplied_value_newest_{n}")
        )

    def display_results(self, result, message):
        self.logger.info(message)
        result.show()

    @staticmethod
    def is_business_date(date_col):
        return F.dayofweek(date_col).isin([2, 3, 4, 5, 6]).cast(BooleanType())

    def apply_modifiers(self, data):
        modifiers_data = self.database_handler.fetch_instrument_modifiers()

        schema = StructType([
            StructField("instrument", StringType(), True),
            StructField("multiplier", DoubleType(), True),
        ])
        modifiers_df = self.spark.createDataFrame(modifiers_data, schema)

        data = data.join(F.broadcast(modifiers_df), "instrument", "left_outer").withColumn(
            "multiplied_value", F.when(F.col("multiplier").isNotNull(), F.col("value") * F.col("multiplier")).otherwise(F.col("value"))
        )

        return data
    
def load_configuration():
    config = ConfigParser()
    config.read('config/config.ini')
    return config

def main():
    try:
        logging.basicConfig(level=logging.INFO)
        config = load_configuration()

        spark = SparkSession.builder\
            .appName("InstrumentCalculator")\
            .master("local[*]")\
            .config("spark.executor.memory", "8g")\
            .config("spark.executor.cores", "2")\
            .config("spark.driver.memory", "2g")\
            .getOrCreate()

        calculator = InstrumentCalculator(spark, config)
        calculator.connect_db()
        data = calculator.read_data()
        calculator.calculate_metrics(data)

    except Exception as e:
        logging.exception(f"An error occurred in the main process: {str(e)}")
        raise

    finally:
        if calculator:
            calculator.database_handler.close_connection()
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()

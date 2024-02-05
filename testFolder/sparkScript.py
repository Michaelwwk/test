# !pip install --ignore-install -q pyarrow
# !pip install --ignore-install -q pyspark
# !pip install --ignore-install -q findspark
# !pip install --ignore-install -q yfinance
# try:
#   !git clone https://github.com/Michaelwwk/test.git
# except:
#   pass
import yfinance as yf
import pyarrow.csv as pv
import pyarrow.parquet as pq
import findspark
findspark.init()
# import collections

from pyspark.sql import SparkSession
# from google.colab import drive
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType
from datetime import datetime
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def sparkScript():

    # on Local

    spark = SparkSession.builder.master("local").appName("Stock Ticker").config('spark.ui.port', '4050').getOrCreate()
    # drive.mount('/content/drive')

    someFile = "wordcount.txt"
    stock_folder = "StockData"
    stocks = spark.read.csv(stock_folder, header=True)
    peopleDF = spark.read.json("people.json")

    df_csv = spark.read.format('csv') \
                    .option("inferSchema","true") \
                    .option("header","true") \
                    .option("sep",";") \
                    .load("people.csv")

    df_json = spark.read.format('json') \
                    .option("inferSchema","true") \
                    .option("header","true") \
                    .option("sep",";") \
                    .load("people.json")

    # read and convert hdb resale price
    hdb_table = pv.read_csv("resale-flat-prices-based-on-registration-date-from-mar-2012-to-dec-2014.csv")
    iris_data = spark.read.csv("iris-data.csv", header=True, inferSchema=True)
    pq.write_table(hdb_table,'resale-flat-prices-based-on-registration-date-from-mar-2012-to-dec-2014.parquet')
    hdb_parquet = pq.ParquetFile('resale-flat-prices-based-on-registration-date-from-mar-2012-to-dec-2014.parquet')

    # # on Colab

    # spark = SparkSession.builder.master("local").appName("Stock Ticker").config('spark.ui.port', '4050').getOrCreate()
    # # drive.mount('/content/drive')

    # someFile = "/content/test/wordcount.txt"
    # stock_folder = "/content/test/StockData"
    # stocks = spark.read.csv(stock_folder, header=True)
    # peopleDF = spark.read.json("/content/test/people.json")

    # df_csv = spark.read.format('csv') \
    #                 .option("inferSchema","true") \
    #                 .option("header","true") \
    #                 .option("sep",";") \
    #                 .load("/content/test/people.csv")

    # df_json = spark.read.format('json') \
    #                 .option("inferSchema","true") \
    #                 .option("header","true") \
    #                 .option("sep",";") \
    #                 .load("/content/test/people.json")

    # # read and convert hdb resale price
    # hdb_table = pv.read_csv("/content/test/resale-flat-prices-based-on-registration-date-from-mar-2012-to-dec-2014.csv")
    # iris_data = spark.read.csv("/content/drive/MyDrive/iris-data.csv", header=True, inferSchema=True)
    # pq.write_table(hdb_table,'/content/test/resale-flat-prices-based-on-registration-date-from-mar-2012-to-dec-2014.parquet')
    # hdb_parquet = pq.ParquetFile('/content/test/resale-flat-prices-based-on-registration-date-from-mar-2012-to-dec-2014.parquet')

    # import collections
    # spark = SparkSession.builder.master("local").appName("My App ").getOrCreate()
    spark.sparkContext
    # the above file is under your pythonProject folder
    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    print(spark.read.text(someFile).count())

    # to read in data from a text file, first upload the data file into your google drive and then mount your google drive onto colab
    # to attempt to forcibly remount, call drive.mount("/content/drive", force_remount=True)
    # drive.mount('/content/drive', force_remount=True)
    rdd = spark.sparkContext.parallelize((1, 2, 3, 4, 5))
    # Traditional Python map(function, collection) (few MBs - GB fails)
    # Scalabale map (Peta - support)
    # iterablecollection.map(function) -> Object
    # collect() Object to collection
    print(tuple(rdd.map(lambda x: x * x).collect()))
    ####Filter
    print(rdd.filter(lambda x: x % 2 == 0).collect())  # Keeps even numbers

    words = spark.sparkContext.parallelize(["hello world", "hi", "hello mars", "hello jupiter", "hello saturn"])
    print(words.flatMap(lambda x: x.split(" ")).collect())
    print(words.flatMap(lambda x: x.split(" ")).distinct().collect())
    tuples = spark.sparkContext.parallelize((1, 1, 2, 3, 3, 4))
    print(tuples.distinct().collect())

    print(tuples.distinct().collect())
    print(tuples.count())
    first_three = tuples.take(3)
    print(first_three)

    sum = tuples.reduce(lambda a, b: a + b)
    print(sum)

    # pprint()
    rdd = spark.sparkContext.parallelize([(3, 6),(1, 2),(3, 4)])
    grouped = rdd.groupByKey()
    for key, values in grouped.collect():
        print(f"{key}: {tuple(values)}")
    reduced = rdd.reduceByKey(lambda a, b: a + b)
    print(reduced.collect())
    sorted_rdd = rdd.sortByKey()
    print(sorted_rdd.collect())
    # Create two RDDs
    rdd1 = spark.sparkContext.parallelize([("Alice", 1), ("Bob", 2)])
    rdd2 = spark.sparkContext.parallelize([("Charlie", 3), ("David", 4)])

    # Perform the union operation
    union_rdd = rdd1.union(rdd2)

    # Collect and print the results
    print(union_rdd.collect())



    # Create two RDDs with common keys
    rdd3 = spark.sparkContext.parallelize([("Alice", "Apple"), ("Bob", "Banana")])
    rdd4 = spark.sparkContext.parallelize([("Alice", 1), ("Bob", 2)])

    # Perform the join operation
    join_rdd = rdd3.join(rdd4)

    # Collect and print the results
    print(join_rdd.collect())

    # Create two RDDs
    rdd5 = spark.sparkContext.parallelize([1, 2])
    rdd6 = spark.sparkContext.parallelize(["a", "b"])

    # Perform the cartesian operation
    cross_rdd = rdd5.cartesian(rdd6)

    # Collect and print the results
    print(cross_rdd.collect())
    # # to read in data from a text file, first upload the data file into your google drive and then mount your google drive onto colab
    # from google.colab import drive
    # # to attempt to forcibly remount, call drive.mount("/content/drive", force_remount=True)
    # drive.mount('/content/drive', force_remount=True)
    # # install pyspark using pip
    # !pip install --ignore-install -q pyspark
    # # install findspark using pip
    # !pip install --ignore-install -q findspark
    # from pyspark.sql import SparkSession
    # import yfinance as yf
    # stock_folder = "/content/drive/MyDrive/data/StockData"
    # # stock_prices_data.to_csv("aapl_stock_prices_data.csv")

    # from pyspark.sql.functions import udf
    # from pyspark.sql.types import DateType
    # from datetime import datetime

    # from pyspark.sql.types import FloatType
    # from pyspark.sql.types import IntegerType
    # import pyspark.sql.functions as func
    # from pyspark.sql.window import Window

    # ## Reading CSV data => Stocks
    # stocks = spark.read.csv(stock_folder, header=True)

    # # import collections
    # spark = SparkSession.builder.master("local").appName("Stock Ticker").config('spark.ui.port', '4050').getOrCreate()
    # Imports

    ticker = "AAPL"  # Replace with the ticker symbol of the company you want to analyze
    start_date = "2020-01-01"
    end_date = "2023-12-28"

    stock_prices_data = yf.download(ticker, start=start_date, end=end_date)

    stock_prices_data.to_csv("aapl_stock_prices_data.csv")
    ## Seeing Data => Dataframe
    stocks.show(5)
    ## Seeing Schema of the Data => Data Types in Dataframe
    stocks.printSchema()
    ## Basic select operation => Select Ticker, Date and Close price
    stocks.select("Ticker").show(3)
    stocks.select(["Ticker", "Date", "Open"]).show(5)
    ## Filtering Data => Select rows containing Microsoft Stock in last one month
    stocks.filter(stocks.Ticker == "MSFT").show(10)
    stocks.filter((stocks.Ticker == "MSFT") & (stocks.Date == "05/31/2023")).show()
    stocks.filter(((stocks.Ticker == "MSFT") | (stocks.Ticker == "V")) & (stocks.Date == "05/31/2023")).show()
    stocks.filter((stocks.Ticker.isin(["MSFT", "QQQ", "SPY", "V", "TSLA"])) & (stocks.Date == "05/31/2023")).show()
    stocks.printSchema()
    ## User Defined Functions

    date_parser = udf(lambda date: datetime.strptime(date,"%m/%d/%Y"), DateType())
    stocks = stocks.withColumn("ParsedDate", date_parser(stocks.Date))
    stocks.printSchema()
    def num_parser(value):
        if isinstance(value, str):
            return float(value.strip("$"))
        elif isinstance(value, int) or isinstance(value, float):
            return value
        else:
            return None

    parser_number = udf(num_parser, FloatType())
    stocks = (stocks.withColumn("Open", parser_number(stocks.Open))
                    .withColumn("Close", parser_number(stocks["Close/Last"]))
                    .withColumn("Low", parser_number(stocks.Low))
                    .withColumn("High", parser_number(stocks.High)))
    stocks.printSchema()
    parse_int = udf(lambda value: int(value), IntegerType())
    ## Changing the datatype of the column
    stocks = stocks.withColumn("Volume", parse_int(stocks.Volume))
    stocks.printSchema()
    cleaned_stocks = stocks.select(["Ticker", "ParsedDate", "Volume", "Open", "Low", "High", "Close"])
    cleaned_stocks.show(5)
    ## Calculating basic stastics about data => Calculate average stock price
    cleaned_stocks.describe(["Volume", "Open", "Low", "High", "Close"]).show()
    ## Calculate maximum stock price for various stocks
    cleaned_stocks.groupBy("Ticker").max("Open").show(15)

    cleaned_stocks.groupBy("Ticker").max("Open").withColumnRenamed("max(Open)", "MaxStockPrice").show(15)
    cleaned_stocks.groupBy("Ticker").agg(func.max("Open").alias("MaxStockPrice")).show(15)
    cleaned_stocks.groupBy("Ticker").agg(
        func.max("Open").alias("MaxStockPrice"),
        func.sum("Volume").alias("TotalVolume")
    ).show(15)
    ## Calculate maximum price of stocks each year => Basic date manipulation operation
    cleaned_stocks = (cleaned_stocks.withColumn("Year", func.year(cleaned_stocks.ParsedDate))
                                    .withColumn("Month", func.month(cleaned_stocks.ParsedDate))
                                    .withColumn("Day", func.dayofmonth(cleaned_stocks.ParsedDate))
                                    .withColumn("Week", func.weekofyear(cleaned_stocks.ParsedDate))
                    )
    cleaned_stocks.show(10)
    ## Calculate average stock price for stock each month
    monthly = cleaned_stocks.groupBy(['Ticker', 'Year', 'Month']).agg(func.max("Open").alias("MonthHigh"), func.min("Open").alias("MonthLow"))
    weekly = cleaned_stocks.groupBy(['Ticker', 'Year', 'Week']).agg(func.max("Open").alias("WeekHigh"), func.min("Open").alias("WeekLow"))
    monthly.show()
    weekly.show()
    weekly.withColumn("Spread", weekly['WeekHigh'] - weekly['WeekLow']).show()
    yearly = cleaned_stocks.groupBy(['Ticker', 'Year']).agg(func.max("Open").alias("YearlHigh"), func.min("Open").alias("YearlyLow"))
    yearly.show()
    # Joins
    cleaned_stocks.join(yearly, (cleaned_stocks.Ticker==yearly.Ticker) & (cleaned_stocks.Year == yearly.Year),
                                        'inner'
                                        ).show()
    cleaned_stocks.join(yearly,
                        (cleaned_stocks.Ticker==yearly.Ticker) & (cleaned_stocks.Year == yearly.Year),
                        'inner'
                    ).drop(yearly.Year, yearly.Ticker).show()
    historic_stocks = cleaned_stocks.join(yearly,
                        (cleaned_stocks.Ticker==yearly.Ticker) & (cleaned_stocks.Year == yearly.Year),
                        'inner'
                    ).drop(yearly.Year, yearly.Ticker)
    historic_stocks.show(5)
    snapshot = cleaned_stocks.select(['Ticker', 'ParsedDate', 'Open'])
    snapshot.show()
    lag1Day = Window.partitionBy("Ticker").orderBy("ParsedDate")
    snapshot.withColumn("PreviousOpen", func.lag("Open", 1).over(lag1Day)).show()


    ## Calculate moving average
    movingAverage = Window.partitionBy("Ticker").orderBy("ParsedDate").rowsBetween(-50, 0)
    (snapshot.withColumn("MA50", func.avg("Open").over(movingAverage))
            .withColumn("MA50", func.round("MA50", 2))).show()
    maximumStock = Window.partitionBy("Ticker").orderBy(snapshot.Open.desc())
    snapshot.withColumn("MaxOpen", func.row_number().over(maximumStock)).show()
    ## Calculate top 5 highest close price for each stock in a year
    snapshot.withColumn("MaxOpen", func.row_number().over(maximumStock)).filter("MaxOpen<=5").show()
    result = snapshot.withColumn("MaxOpen", func.row_number().over(maximumStock)).filter("MaxOpen<=5")
    moving_avg = (snapshot.withColumn("MA50", func.avg("Open").over(movingAverage))
            .withColumn("MA50", func.round("MA50", 2)))
    moving_avg.show()

    # # #from pyspark import SparkConf,SparkContext
    # # from pyspark.sql import SparkSession
    # # import collections
    # # spark = SparkSession.builder.master("local").appName("Ingestion").config('spark.ui.port', '4050').getOrCreate()

    # df_csv = spark.read.format('csv') \
    #                 .option("inferSchema","true") \
    #                 .option("header","true") \
    #                 .option("sep",";") \
    #                 .load("/content/drive/MyDrive/data/DataFormat/people.csv")

    # df_json = spark.read.format('json') \
    #                 .option("inferSchema","true") \
    #                 .option("header","true") \
    #                 .option("sep",";") \
    #                 .load("/content/drive/MyDrive/data/DataFormat/people.json")

    # peopleDF = spark.read.json("/content/drive/MyDrive/data/DataFormat/people.json")

    # import pyarrow.csv as pv
    # import pyarrow.parquet as pq

    # # read hdb resale price
    # hdb_table = pv.read_csv("/content/drive/MyDrive/data/DataFormat/resale-flat-prices-based-on-registration-date-from-mar-2012-to-dec-2014.csv")
    # # convert the CSV file to a Parquet file
    # pq.write_table(hdb_table,'resale-flat-prices-based-on-registration-date-from-mar-2012-to-dec-2014.parquet')
    # hdb_parquet = pq.ParquetFile('resale-flat-prices-based-on-registration-date-from-mar-2012-to-dec-2014.parquet')

    # from google.colab import drive
    # drive.mount('/content/drive')
    # Read CSV file people.csv
    # Show result
    df_csv.show()
    # Print schema
    df_csv.printSchema()

    # # Write csv file
    # df = spark.read.format('csv') \
    #                 .option("inferSchema","true") \
    #                 .option("header","true") \
    #                 .option("sep",";") \
    #                 .load("people.csv")

    # DataFrames can be saved as Parquet files, maintaining the schema information.
    peopleDF.write.format("parquet").mode("overwrite").save("people.parquet")
    # Read in the Parquet file created above.
    # Parquet files are self-describing so the schema is preserved.
    # The result of loading a parquet file is also a DataFrame.
    parquetFile = spark.read.parquet("people.parquet")

    # Parquet files can also be used to create a temporary view and then used in SQL statements.
    parquetFile.createOrReplaceTempView("parquetFile")
    teenagers = spark.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19")
    teenagers.show()

    # inspect the parquet metadata
    print(hdb_parquet.metadata)
    # inspect the parquet row group metadata
    print(hdb_parquet.metadata.row_group(0))
    # inspect the column chunk metadata
    print(hdb_parquet.metadata.row_group(0).column(9).statistics)

    # Assuming the target variable is "class" and other columns are features
    feature_cols = iris_data.columns[:-1]
    # Convert string labels into numerical labels
    indexer = StringIndexer(inputCol="class", outputCol="label")
    iris_data = indexer.fit(iris_data).transform(iris_data)
    # Create a feature vector by assembling the feature columns
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    data = assembler.transform(iris_data)
    # Split the data into training and testing sets
    (training_data, testing_data) = data.randomSplit([0.8, 0.2], seed=123)
    # Customized parameters
    num_trees = 10
    max_depth = 5
    # Create and train a RandomForestClassifier with customized parameters
    rf = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        numTrees=num_trees,
        maxDepth=max_depth
    )
    model = rf.fit(training_data)
    # Make predictions on the testing data
    predictions = model.transform(testing_data)
    # Evaluate the model
    evaluator = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    # Print the accuracy
    print("Accuracy: {:.2f}".format(accuracy))

    # Show the feature importances
    print("Feature Importances: ", model.featureImportances)

    # Stop the Spark session
    spark.stop()
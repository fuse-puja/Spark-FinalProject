
# # Data Analysis on Brazilian E-Commerce Public Dataset by Olist
# Dataset link: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_customers_dataset.csv


import yaml
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from textblob import TextBlob
from pyspark.sql.types import StringType
from pyspark.sql import functions as f

# ### Initializing Spark
# Initializing spark session
spark = SparkSession.builder.appName("FinalProject")\
        .config('spark.driver.extraClassPath','/usr/lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.6.0.jar')\
        .getOrCreate()

# Define the path to your YAML file
yaml_file_path = 'config.yaml'

# Read the YAML file and parse it into a Python dictionary
with open(yaml_file_path, 'r') as file:
    config = yaml.safe_load(file)

# Set environment variables
os.environ['DB_URL'] = config['database']['url']
os.environ['DB_URL_SAVE'] = config['database']['url_save']
os.environ['DB_DRIVER'] = config['database']['driver']
os.environ['DB_USER'] = config['database']['user']
os.environ['DB_PASSWORD'] = config['database']['password']

# Use environment variables for database connection
db_url = os.environ['DB_URL']
db_url_save = os.environ['DB_URL_SAVE']
db_driver = os.environ['DB_DRIVER']
db_user = os.environ['DB_USER']
db_password = os.environ['DB_PASSWORD']


 # Loading the dataset
# Defining path to the dataset
customer_data_path = "./Data/olist_customers_dataset.csv"  
order_item_path = "./Data/olist_order_items_dataset.csv"
order_payment_path = "./Data/olist_order_payments_dataset.csv"
product_category_translation_path= "./Data/product_category_name_translation.csv"
product_path = './Data/olist_products_dataset.csv'
seller_path = './Data/olist_sellers_dataset.csv'
orders_path = './Data/olist_orders_dataset.csv'
review_path = "Data/reviews_translated.csv"  


# Load the Chipotle dataset into a Spark DataFrame
customer_df = spark.read.csv(customer_data_path, header=True, inferSchema=True)
order_item_df = spark.read.csv(order_item_path, header=True, inferSchema=True)
order_payment_df = spark.read.csv(order_payment_path, header=True, inferSchema=True)
product_category_translation_df = spark.read.csv(product_category_translation_path, header=True, inferSchema=True)
seller_df_uncleaned = spark.read.csv(seller_path, header=True, inferSchema=True)
product_df_uncleaned = spark.read.csv(product_path, header=True, inferSchema=True)
orders_df = spark.read.csv(orders_path, header=True, inferSchema= True)
reviews_df = spark.read.csv(review_path, header=True, inferSchema= True)

# # Data Cleaning and pre-processing
#REMOVING WHITESPACE

# Remove leading and trailing whitespace from all columns
seller_df_uncleaned.select([f.trim(f.col(c)).alias(c) for c in seller_df_uncleaned.columns])

# Remove whitespace characters between words in all columns
seller_df = seller_df_uncleaned.select([f.regexp_replace(f.col(c), r'\s+', ' ').alias(c) for c in seller_df_uncleaned.columns])


#Replacing column on product dataset with content from product category translation dataset

# left join between the 'product_df_uncleaned' DataFrame and 'product_category_translation_df'
product_joined_df= product_df_uncleaned.join(product_category_translation_df, "Product_category_name", "left")

# Drop "product_category_name" will be removed from the DataFrame.
product_df = product_joined_df.drop("product_category_name")

# Rename the "product_category_name_english" column to "product_category_name"
product_df = product_df.withColumnRenamed("product_category_name_english", "product_category_name")

# Replace underscores with spaces in the "product_category_name" column
product_df = product_df.withColumn("product_category_name", f.regexp_replace(f.col("product_category_name"), "_", " "))


#Defining 0 for not_defined payment

# Set payment_installment to 0 where payment_type is "not_defined"
order_payment_df = order_payment_df.withColumn("Payment_installments",
                                   f.when(f.col("Payment_type") == "not_defined", 0)
                                   .otherwise(f.col("Payment_installments")))


# Additional cleaning ON REVIEWS

# Casting review_score to integer 
reviews_df=reviews_df.withColumn("review_score", reviews_df["review_score"].cast("int"))

# Replace 'reviews_df' with your actual DataFrame name
reviews_df = reviews_df.withColumn("review_comment_title", f.coalesce(f.col("review_comment_title"), f.lit("no comment")))
reviews_df = reviews_df.withColumn("review_comment_message", f.coalesce(f.col("review_comment_message"), f.lit("no comment")))

# Dropping null values
reviews_df =reviews_df.na.drop()

# Dropping s_no column
reviews_df=reviews_df.drop("s_no")


# ### Storing in parquet file

customer_df.coalesce(1).write.parquet("data_cleaned/customer.parquet",compression ="snappy", mode="overwrite") 
order_item_df.coalesce(1).write.parquet("./data_cleaned/order_item.parquet",compression ="snappy", mode="overwrite")
order_payment_df.coalesce(1).write.parquet("./data_cleaned/order_payment.parquet",compression ="snappy", mode="overwrite")
seller_df.coalesce(1).write.parquet("./data_cleaned/seller.parquet",compression ="snappy", mode="overwrite")
product_df.coalesce(1).write.parquet("./data_cleaned/product.parquet",compression ="snappy", mode="overwrite")
orders_df.coalesce(1).write.parquet("./data_cleaned/orders.parquet",compression ="snappy", mode="overwrite")
reviews_df.coalesce(1).write.parquet("./data_cleaned/reviews.parquet",compression ="snappy", mode="overwrite")


# ### Writing the clean Dataframes to Postgres

customer_df.write.format('jdbc').options(url=db_url,
                                driver = db_driver,
                                dbtable = 'customer', 
                                user=db_user, 
                                password=db_password).mode('overwrite').save()

order_item_df.write.format('jdbc').options(url=db_url,
                                driver = db_driver,
                                dbtable = 'order_item', 
                                user=db_user, 
                                password=db_password).mode('overwrite').save()

order_payment_df.write.format('jdbc').options(url=db_url,
                                driver = db_driver,
                                dbtable = 'order_payment', 
                                user=db_user, 
                                password=db_password).mode('overwrite').save()

seller_df.write.format('jdbc').options(url=db_url,
                                driver = db_driver,
                                dbtable = 'seller', 
                                user=db_user, 
                                password=db_password).mode('overwrite').save()

product_df.write.format('jdbc').options(url=db_url,
                                driver = db_driver,
                                dbtable = 'product', 
                                user=db_user, 
                                password=db_password).mode('overwrite').save()

orders_df.write.format('jdbc').options(url=db_url,
                                driver = db_driver,
                                dbtable = 'orders', 
                                user=db_user, 
                                password=db_password).mode('overwrite').save()

reviews_df.write.format('jdbc').options(url=db_url,
                                driver = db_driver,
                                dbtable = 'reviews', 
                                user=db_user, 
                                password=db_password).mode('overwrite').save()

# ### Reading from postgres

reviews_df = spark.read.format("jdbc").options(url=db_url,
                                driver = db_driver,
                                dbtable = 'reviews', 
                                user=db_user, 
                                password=db_password).load()

orders_df = spark.read.format("jdbc").options(url=db_url,
                                driver = db_driver,
                                dbtable = 'orders', 
                                user=db_user, 
                                password=db_password).load()

order_item_df = spark.read.format("jdbc").options(url=db_url,
                                driver = db_driver,
                                dbtable = 'order_item', 
                                user=db_user, 
                                password=db_password).load()


product_df = spark.read.format("jdbc").options(url=db_url,
                                driver = db_driver,
                                dbtable = 'product', 
                                user=db_user, 
                                password=db_password).load()



# ### Question 1


# Delivery trend and purphase trend on a day: Compute the delivery deviation in days and actual delivery time also find the order purchase hour in category of Dawn, Morning, Afternoon, Night: 
# Filter delivered orders only
delivered_orders_df = orders_df.filter(orders_df["Order_status"] == "delivered")

# Extract the hour of the day from the timestamp
delivered_orders_df = delivered_orders_df.withColumn("Order_purchase_hour", 
                                                     f.hour("Order_purchase_timestamp"))

# Calculate delivery time in days by finding the time difference between order delivered and purchase timestamps
delivered_orders_df = delivered_orders_df.withColumn(
    "delivery_time_days",
    f.round((f.unix_timestamp("order_delivered_customer_date") - f.unix_timestamp("order_purchase_timestamp")) / (24 * 3600),3)
)

# Calculate delivery deviation in days by finding the time difference between estimated delivery date and actual delivery date
delivered_orders_df = delivered_orders_df.withColumn(
    "delivery_deviation_in_days",
    f.round((f.unix_timestamp("order_estimated_delivery_date") - f.unix_timestamp("order_delivered_customer_date")) / (24 * 3600),3)
)

# Calculate average delivery time 
average_delivery_time = delivered_orders_df.selectExpr("avg(delivery_time_days) as avg_delivery_time").first()["avg_delivery_time"]
print("Average Delivery Time (in days):", average_delivery_time)

# Create a column for the time slot with its category of purchase hour
delivered_orders_df = delivered_orders_df.withColumn("Order_purchase_time_slot",
    f.when((delivered_orders_df["Order_purchase_hour"] >= 0) & (delivered_orders_df["Order_purchase_hour"] <= 6), "Dawn")
    .when((delivered_orders_df["Order_purchase_hour"] >= 7) & (delivered_orders_df["Order_purchase_hour"] <= 12), "Morning")
    .when((delivered_orders_df["Order_purchase_hour"] >= 13) & (delivered_orders_df["Order_purchase_hour"] <= 18), "Afternoon")
    .otherwise("Night")
)

# Selecting columns for final result
delivery_status = delivered_orders_df.select("order_id", "order_purchase_timestamp", "order_delivered_customer_date","delivery_deviation_in_days",
                                             "delivery_time_days","Order_purchase_time_slot","Order_purchase_hour")

# Showing the final result
delivery_status.show()

# ### Question 2

# Find the review sentiment analysis and the coorelation between sentiment score and review score
#
# Defining a UDF for sentiment analysis

def analyze_sentiment(text):
    '''
     Analyzes the sentiment polarity of the given text using the TextBlob library.

    Parameters:
    text (str): The text for sentiment analysis.

    Returns:
    float or None: The sentiment polarity score in the range of -1 to 1, where -1 represents
    a negative sentiment, 1 represents a positive sentiment, and 0 represents neutral sentiment.
    If the input text is "no comment," returns 0.
    '''
    if text.lower() != "no comment":
        analysis = TextBlob(text)
        polarity = analysis.sentiment.polarity
        return polarity
    return 0  

# Create a User-Defined Function (UDF) to analyze sentiment
sentiment_udf = f.udf(analyze_sentiment, StringType())

# Calculate sentiment scores by passing the reviews to UDF
reviews_df = reviews_df.withColumn("sentiment_score", sentiment_udf(reviews_df["review_comment_message"]))

# Cating sentiment_score to float
reviews_df=reviews_df.withColumn("sentiment_score", reviews_df["sentiment_score"].cast("float"))

# Calculate the correlation between 'sentiment_score' and 'review_score' columns
correlation = reviews_df.select(f.corr("sentiment_score", "review_score")).first()[0]

# Create a DataFrame to store the correlation result
correlation_df = spark.createDataFrame([(correlation,)], ["Correlation"])

# Show the correlation result
correlation_df.show()
print("Correlation between sentiment_score and review_score:", correlation)

#preparing final display output
# Joining reviews with order_item and product to get product_category name
results_df = reviews_df.join(order_item_df, "order_id", "inner")
joined_df= results_df.join(product_df, on = "product_id", how ="inner")

# Final selection for output display orderby sentiment score
joined_df = joined_df.select("order_id", "review_id","review_score", "review_comment_message","product_category_name","sentiment_score").orderBy("sentiment_score")

# Showing final output
joined_df.show()
# # Storing final results in postgres


delivery_status.write.format('jdbc').options(url=db_url_save,
                                driver = db_driver,
                                dbtable = 'delivery_status_table', 
                                user=db_user, 
                                password=db_password).mode('overwrite').save()


joined_df.write.format('jdbc').options(url=db_url_save,
                                driver = db_driver,
                                dbtable = 'review_sentiment_analysis', 
                                user=db_user, 
                                password=db_password).mode('overwrite').save()


correlation_df.write.format('jdbc').options(url=db_url_save,
                                driver = db_driver,
                                dbtable = 'correlation_of_reviewscore_and_sentiment', 
                                user=db_user, 
                                password=db_password).mode('overwrite').save()




spark.stop()



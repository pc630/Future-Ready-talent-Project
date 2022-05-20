# Databricks notebook source


# COMMAND ----------

rating_filename = "dbfs:/mnt/Files/Rawdata/rating.csv" 
movie_filename = "dbfs:/mnt/Files/Rawdata/movie.csv"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/Files/Rawdata/

# COMMAND ----------

from pyspark.sql.types import *
#working only on movies.csv right now
movie_with_genres_df_schema = StructType(
  [StructField('ID', IntegerType()),
   StructField('title', StringType()),
   StructField('genres',StringType())]
  )

movie_df_schema = StructType(
  [StructField('ID', IntegerType()),
   StructField('title', StringType())]
  ) #dropping the genres.Also, we will tranform the df to include the Year later


# COMMAND ----------

#Creating the dataframes 
movie_df = sqlContext.read.format('com.databricks.spark.csv').options(header=True, inferSchema=False).schema(movie_df_schema).load(movie_filename)
movie_with_genres_df = sqlContext.read.format('com.databricks.spark.csv').options(header=True, inferSchema=False).schema(movie_with_genres_df_schema).load(movie_filename)

# COMMAND ----------

movie_df.show(4,truncate = False) #we will also use this for Collabrative filtering
movie_with_genres_df.show(4,truncate = False)

# COMMAND ----------

#transforming the Dataframes
from pyspark.sql.functions import split, regexp_extract

# Side note a very nice quote -- Some people, when confronted with a problem, think "I know, I'll use regular expressions." Now they have two problems.(attributed to Jamie #Zawinski)
movie_with_year_df = movie_df.select('ID','title',regexp_extract('title',r'\((\d+)\)',1).alias('year'))


# COMMAND ----------

movie_with_year_df.show(4,truncate = False)

# COMMAND ----------

#from here we can look at the count and find that the maximum number of movies are produced in 2009
display(movie_with_year_df.groupBy('year').count().orderBy('count',ascending = False))

# COMMAND ----------

#again for avoiding the action we are explicitly defining the schema
rating_df_schema = StructType(
  [StructField('userId', IntegerType()),
   StructField('movieId', IntegerType()),
   StructField('rating', DoubleType())]
)              #we are dropping the Time Stamp column


# COMMAND ----------

#creating the df
rating_df = sqlContext.read.format('com.databricks.spark.csv').options(header=True, inferSchema=False).schema(rating_df_schema).load(rating_filename)
rating_df.show(4)

# COMMAND ----------

#We will cache both the dataframes
rating_df.cache()
movie_df.cache()
print ("both dataframes are in cache now for easy accessibility")

# COMMAND ----------

from pyspark.sql import functions as F

# From ratingsDF, create a movie_ids_with_avg_ratings_df that combines the two DataFrames
movie_ids_with_avg_rating_df = rating_df.groupBy('movieId').agg(F.count(rating_df.rating).alias("count"), F.avg(rating_df.rating).alias("average"))
print ('movie_ids_with_avg_rating_df:')
movie_ids_with_avg_rating_df.show(4, truncate=False)

# COMMAND ----------

#this df will have names with movie_id- Make it more understandable
movie_names_with_avg_rating_df = movie_ids_with_avg_rating_df.join(movie_df,F.col('movieID') == F.col('ID')).drop('ID')
movie_names_with_avg_rating_df.show(4,truncate = False)

# COMMAND ----------

#so let us see the global popularity
movie_with_500_rating_or_more = movie_names_with_avg_rating_df.filter(movie_names_with_avg_rating_df['count'] >= 500).orderBy('average',ascending = False)
movie_with_500_rating_or_more.show(truncate = False)

# COMMAND ----------

# We'll hold out 60% for training, 20% of our data for validation, and leave 20% for testing
seed = 4
(split_60_df, split_a_20_df, split_b_20_df) = rating_df.randomSplit([0.6,0.2,0.2],seed)

# Let's cache these datasets for performance
training_df = split_60_df.cache()
validation_df = split_a_20_df.cache()
test_df = split_b_20_df.cache()

print('Training: {0}, validation: {1}, test: {2}\n'.format(
  training_df.count(), validation_df.count(), test_df.count())
)
training_df.show(4,truncate = False)
validation_df.show(4,truncate = False)
test_df.show(4,truncate = False)

# COMMAND ----------

from pyspark.ml.recommendation import ALS
als = ALS()
# Reset the parameters for the ALS object.
als.setPredictionCol("prediction")\
   .setMaxIter(5)\
   .setSeed(seed)\
   .setRegParam(0.1)\
   .setUserCol('userId')\
   .setItemCol('movieId')\
   .setRatingCol('rating')\
   .setRank(8)   #we got rank 8 as optimal


# Create the model with these parameters.
my_rating_model = als.fit(training_df)

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col
# create an RMSE evaluator using the table and predicted colums
# it will essentially calculate the rmse score based on these columns
reg_eval = RegressionEvaluator(predictionCol="prediction", labelCol="rating", metricName="rmse")
my_predict_df = my_rating_model.transform(test_df)

# Remove NaN value from prediction
predicted_test_my_rating_df = my_predict_df.filter(my_predict_df.prediction != float('nan'))

# Run the previously created RMSE evaluator, reg_eval, on the predicted_test_my_ratings_df DataFrame
test_RMSE_my_rating = reg_eval.evaluate(predicted_test_my_rating_df)
print('The model had a RMSE on the test set of {0}'.format(test_RMSE_my_rating))
#The model had a RMSE on the test set of 0.817263646506
dbutils.widgets.text("input","5","")
ins=dbutils.widgets.get("input")
uid=int(ins)
ll=predicted_test_my_rating_df.filter(col("userId")==uid)

# COMMAND ----------

 MovieRec=ll.join(movie_df,F.col('movieID') == F.col('ID')).drop('ID').select('title').take(10)
    
ll=dbutils.notebook.exit(MovieRec)

# COMMAND ----------



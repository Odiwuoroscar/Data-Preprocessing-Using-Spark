from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Mental Health Data Preprocessing") \
    .getOrCreate()

# Load the CSV file
df = spark.read.csv("/opt/spark/mental_health_dataset.csv", header=True, inferSchema=True)

# Show the initial dataframe structure
df.show(5)
df.printSchema()

# Convert columns to appropriate data types
df = df.withColumn("Age", col("Age").cast("int")) \
       .withColumn("Gender", col("Gender").cast("string")) \
       .withColumn("self_employed", col("self_employed").cast("string")) \
       .withColumn("family_history", col("family_history").cast("string")) \
       .withColumn("treatment", col("treatment").cast("string")) \
       .withColumn("work_interfere", col("work_interfere").cast("string")) \
       .withColumn("no_employees", col("no_employees").cast("string")) \
       .withColumn("remote_work", col("remote_work").cast("string")) \
       .withColumn("tech_company", col("tech_company").cast("string")) \
       .withColumn("benefits", col("benefits").cast("string")) \
       .withColumn("care_options", col("care_options").cast("string")) \
       .withColumn("wellness_program", col("wellness_program").cast("string")) \
       .withColumn("seek_help", col("seek_help").cast("string")) \
       .withColumn("anonymity", col("anonymity").cast("string")) \
       .withColumn("leave", col("leave").cast("string")) \
       .withColumn("mental_health_consequence", col("mental_health_consequence").cast("string")) \
       .withColumn("phys_health_consequence", col("phys_health_consequence").cast("string")) \
       .withColumn("coworkers", col("coworkers").cast("string")) \
       .withColumn("supervisor", col("supervisor").cast("string")) \
       .withColumn("mental_health_interview", col("mental_health_interview").cast("string")) \
       .withColumn("phys_health_interview", col("phys_health_interview").cast("string")) \
       .withColumn("mental_vs_physical", col("mental_vs_physical").cast("string")) \
       .withColumn("obs_consequence", col("obs_consequence").cast("string"))

# Handle missing values
df = df.na.fill({"Age": df.select(mean("Age")).collect()[0][0]})
df = df.na.fill("Unknown")

# Encode categorical variables
categorical_cols = [col for col in df.columns if df.select(col).dtypes[0][1] == 'string']

indexers = [StringIndexer(inputCol=col, outputCol=col+"_index", handleInvalid="keep") for col in categorical_cols]
encoders = [OneHotEncoder(inputCol=col+"_index", outputCol=col+"_vec") for col in categorical_cols]

# Assemble features
assembler = VectorAssembler(inputCols=[col+"_vec" for col in categorical_cols] + ["Age"], outputCol="features")

# Create and fit the pipeline
pipeline = Pipeline(stages=indexers + encoders + [assembler])
model = pipeline.fit(df)
df_encoded = model.transform(df)

# Select relevant columns
df_final = df_encoded.select("features", "treatment")

# Show the preprocessed dataframe
df_final.show(5)

# Summary statistics for numeric columns
df.select("Age").describe().show()

# Generate some basic insights
# 1. Distribution of treatment
df.groupBy("treatment").count().show()

# 2. Average age of employees who sought treatment vs those who didn't
df.groupBy("treatment").agg(mean("Age").alias("average_age")).show()

# 3. Count of employees who have a family history of mental illness and sought treatment
df.groupBy("treatment", "family_history").count().show()

# Save preprocessed data
df_final.write.parquet("/opt/spark/preprocessed_mental_health_data.parquet")

# Stop the Spark session
spark.stop()

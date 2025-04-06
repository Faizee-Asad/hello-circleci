# hello-circleci

p2 data reading

#Step 1 Install pyspark and libraries
!pip install pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import * #
#Step 2 Create Objetct
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.read.option('header', True).csv('/content/employee.csv')
df.show(4) #show uptill 4

print("Kaustubh Karande KSMSCIT015")
#Displaying Data Structure and Specific Columns
df.select('salary').show(10)
df.printSchema()

print("Kaustubh Karande KSMSCIT015")
#Converting Pyspark dataframe to pandas
pandas_df = df.toPandas()
print(pandas_df.head())
print(pandas_df.tail())

#3a
print("Kaustubh Karande KSMSCIT015")
#COMBINING DATAFRAME IN PYSPARK
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName('Combined DataFrame').getOrCreate()
data1 = [(1,"Afzal",15),(2,"kaustubh",11),(3,"Asad",23)]
data2 = [(4,"Amin",49),(5,"Hamza",35),(6,"saif",21)]
column = ["ID","Name","Age"]
df1 = spark.createDataFrame(data1,column)
df2 = spark.createDataFrame(data2,column)
df1.show()
print("Kaustubh Karande KSMSCIT015")
df2.show()
#3b
#using joins
print("Kaustubh Karande KSMSCIT015")
Combined_df = df1.union(df2)
Combined_df.show()
df3=df2.select("ID","Name","Age")
Combined_df.show()
data4 = [(1,"Mumbai"),(2,"Uran"),(3,"Nehrul"),(4,"Punjab"),(5,"Panvel"),(6,"Chennai")]
column2 = ["ID","City"]
df4 = spark.createDataFrame(data4,column2)
df4.show()
print("Kaustubh Karande KSMSCIT015")
join_df = df1.join(df4,on="ID",how="inner")
join_df.show()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('read data eg').getOrCreate()
df1 = spark.read.option("Header",True).csv('1.csv')
df1.show()

#4
print("Kaustubh Karande KSMSCIT015")
from pyspark.sql import  SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName('read data eg').getOrCreate()
data = [(1,"IPad",50000,"Electronics"),
        (2,"Watch",5000,"Fashion"),
        (3, "Pant", 2500, "Fashion"),
        (4, "Phone", 11500, "Electronics"),
        (5, "Shirt", 1400, "Fashion")]
column = ["prod_id","product","price","category"]
df = spark.createDataFrame(data,column)
df.show()
collected_data=df.collect()
for row in collected_data :
  print(row)
print("Kaustubh Karande KSMSCIT015")
df.filter(df["price"]>100).show()
from pyspark.sql import SparkSession
print("Kaustubh Karande KSMSCIT015")
spark = SparkSession.builder.appName("SalesTotal").getOrCreate()
sc = spark.sparkContext
sales_data = [
    ("TV", 20000),
    ("Laptop", 2900),
    ("Mobile", 22000),
    ("Tablet", 70000),
    ("laptop", 45000)
]
# RDD create
rdd = sc.parallelize(sales_data)
# Map Step: Convert Product and Price
mapped_rdd = rdd.map(lambda x: (x[0], x[1]))
# Reduce Step: Sum up sales
total_sales_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)
print("Sales Data Analysis : Map reduce")
result = total_sales_rdd.collect()
for product, total in result:
    print(f"{product}: ${total}")

#5
print("Kaustubh Karande KSMSCIT015")
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkSession example').config('Spark Config','config - value').getOrCreate()
from pyspark.sql import Row
data = [Row(Name='Afzal',Age=34),Row(Name='Kaustubh',Age=20),Row(Name='Asad',Age=25)]
df = spark.createDataFrame(data)
df.show()
df.printSchema()
#second method - user input
print("Kaustubh Karande KSMSCIT015")
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('User input wala practical').config('Spark Config','config - value').getOrCreate()
num_rows = int(input("Enter number of rows - "))
data = []
for i in range (num_rows):
  name=input("Enter Name - ")
  age = int(input("Enter Age - "))
  city = input("Enter City - ")
  data.append((name,age,city))
column = ["Name","Age","city"]
df=spark.createDataFrame(data,schema=column)
df.show()
df.printSchema()
print("Original Data")
df.select("Name","Age").show()
print("Age Filter")
df.filter(df['Age']>21).show()
print("Location Filter")
df.filter(df['City']=="Mumbai").show()

#6
#Part A
print("Kaustubh Karande KSMSCIT015")
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFrameExample").getOrCreate()
df = spark.createDataFrame([(1, 31), (2, 21), (3, 27)], ["id", "age"])
df.show()
print("Afzal Sayed KSMSCIT028")
df.createOrReplaceTempView("people")
result = spark.sql("SELECT * FROM people WHERE age > 27").show()
result
Part B
print("Kaustubh Karande KSMSCIT015")
from pyspark.sql import SparkSession
data  = [(1,'furniture',2000,'2025-02-01'),(2,'electronics',1500,'2025-03-10'),(3,'cosmetics',1500,'2025-03-03'),(4,'novels',2500,'2025-04-05')]
columns = ['id','category','price','date']
spark = SparkSession.builder.appName('ecommerce').getOrCreate()
df = spark.createDataFrame(data,columns)
df.show()
df.createOrReplaceTempView("sales")
print("Kaustubh Karande KSMSCIT015")
result = spark.sql("SELECT category,avg(price) as avg_amount FROM sales group by category").show()
print("Kaustubh Karande KSMSCIT015")
result = spark.sql("SELECT * from sales order by price desc limit 2").show()
result = spark.sql("SELECT * from sales order by price asc limit 2").show()

#Part C - Datavizualization
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
result = spark.sql("SELECT category,avg(price) as avg_amount FROM sales group by category")
df_query1 = result.toPandas()
df_query1.head()
#Convert the DataFrames to Pandas
plt.figure(figsize=(8, 6))
sns.barplot(x='category', y='avg_amount', data=df_query1)
plt.title('Kaustubh Karande -15 \n\nCategories by Price')
plt.xlabel('Category')
plt.ylabel('Price')
plt.show()

#7
print("Kaustubh Karande KSMSCIT015")
print("Age and Names")
#import libraies
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
spark =  SparkSession.builder.appName("udf examle").getOrCreate()
data = [("Kaustubh",55),("Asad",44),("Amir",25),] #dataset
columns = ["Name","Age"]
df  = spark.createDataFrame(data,columns)
df.show()
print("Kaustubh Karande KSMSCIT015")
print("Age Group") #Age grouping
def age_group(age):
  if age < 18:
    return "Minor"
  elif age >= 18 and age < 60:
    return "Adult"
  else:
    return "Senior"
age_group_udf = udf(age_group,StringType())
df_with_group = df.withColumn("Age_Group",age_group_udf(df["Age"]))
df_with_group.show()
print("Kaustubh Karande KSMSCIT015")
print("Age Count")
age_count = df_with_group.groupBy("Age_Group").count()
age_count.show()

#8
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
print("Kaustubh Karande KSMSCIT015")
spark =  SparkSession.builder.appName("udf examle").getOrCreate()
data =[(1,200),(2,210),(3,220),(4,240),(5,205)]
columns=["Feature","Target"]
df  = spark.createDataFrame(data,columns)
df.show()
print("Kaustubh Karande KSMSCIT015")
Assembler = VectorAssembler(inputCols=["Feature"],outputCol="Features")
Assemebled_df = Assembler.transform(df).select("Features","Target")
print("Assemebled_df:",Assemebled_df.show())
#Applying Linear regression model
print("Kaustubh Karande KSMSCIT015")
LR = LinearRegression(featuresCol="Features",labelCol="Target")
LR_model = LR.fit(Assemebled_df)
print(LR_model.coefficients)
print(LR_model.intercept)
training_summary = LR_model.summary
RMSE = training_summary.rootMeanSquaredError
print("RMSE,",RMSE)
print("Kaustubh Karande KSMSCIT015")
prediction = LR_model.transform(Assemebled_df)
print(prediction.show())

#9
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
print("Kaustubh Karande KSMSCIT015")
spark = SparkSession.builder.appName("KMeans").getOrCreate()
data = [(1.0,3.0),(2.5,2.8),(6.0,9.0),(8.0,8.0),(1.0,1.6),(1.0,22.0)]
columns = ['Sub1','Sub2']
df = spark.createDataFrame(data,columns)
df.show()
print("Kaustubh Karande KSMSCIT015")
assembler = VectorAssembler(inputCols=['Sub1','Sub2'],outputCol='features')
assembler1=assembler.transform(df)
assembler1.show()
print("Kaustubh Karande KSMSCIT015")
Kmeans = KMeans(k=2,seed=1) #performing KMeans
model = Kmeans.fit(assembler1)
prediction = model.transform(assembler1)
prediction.show()
print("Kaustubh Karande KSMSCIT015")
for center in model.clusterCenters():
print(center)

#10
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import CountVectorizer,IDF,Tokenizer
spark = SparkSession.builder.appName("Logistic Regresion").getOrCreate()
#sample email data
print("Kaustubh Karande KSMSCIT015")
data = [ (1,"win a free iphone") ,(1,"congratulation you won a lottery"),
(0,"lets meet for lunch"),(0,"don't forget to complete the assignment")]
columns = ['label','text']
df = spark.createDataFrame(data,columns)
df.show()
#preprocess
print("Kaustubh Karande KSMSCIT015")
tokenizer = Tokenizer(inputCol='text',outputCol='tokens')
df = tokenizer.transform(df)
df.show()
print("Kaustubh Karande KSMSCIT015")
vectorizer = CountVectorizer(inputCol='tokens',outputCol='rawfeature')
vector_model = vectorizer.fit(df)
df = vector_model.transform(df)
df.show()
print("Kaustubh Karande KSMSCIT015")
df = df.select('label','rawfeature')
df = df.withColumnRenamed("rawfeature","features1")
LR = LogisticRegression(featuresCol='features1',labelCol='label')
lr_model = LR.fit(df)
prediction = lr_model.transform(df)
prediction.show()

#11
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, HashingTF, StringIndexer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
spark = SparkSession.builder.appName("TextClassifier").getOrCreate()
print("Kaustubh Karande KSMSCIT015")
# Sample dataset
data = [
    (0, "Gunther is set to defend his World Heavyweight Championship against Jey Uso", "positive"),
    (1, "Jey lost", "negative"),
    (2, "Jey will win the match", "positive"),
    (3, "I dislike wwe", "negative"),
    (4, "Cena will turn face again", "positive"),
    (5, "You can see me", "negative")
]
columns = ["id", "text", "label"]
df = spark.createDataFrame(data, columns)
label_indexer = StringIndexer(inputCol="label", outputCol="label_idx")
tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashing_tf = HashingTF(inputCol="words", outputCol="features", numFeatures=1000)
# Naive Bayes classifier
nb = NaiveBayes(labelCol="label_idx", featuresCol="features", modelType="multinomial")
# Pipeline
pipeline = Pipeline(stages=[label_indexer, tokenizer, hashing_tf, nb])
# Split data
train_data, test_data = df.randomSplit([0.7, 0.3], seed=1234)
# Train model
model = pipeline.fit(train_data)
predictions = model.transform(test_data)
predictions.select("id", "text", "label", "label_idx", "prediction").show()
# Evaluate model
evaluator = MulticlassClassificationEvaluator(
    labelCol="label_idx", predictionCol="prediction", metricName="accuracy"
)
accuracy = evaluator.evaluate(predictions)
print(f"Test set accuracy: {accuracy:.2f}")

#12
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("WordCount").getOrCreate()
sc = spark.sparkContext
# Sample text data
data = [
    "Kaustubh Karande",
    "KSMSCIT015",
    "pyspark map reduce example",
    "Benzema",
]
rdd = sc.parallelize(data)
mapped_rdd = rdd.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))
word_counts = mapped_rdd.reduceByKey(lambda a, b: a + b) # Reduce - Sum up occurrences of each word
print("Kaustubh Karande KSMSCIT015")
print("Real-Time Word Count with PySpark Streaming")
# Collect and print results
result = word_counts.collect()
for word, count in result:
    print(f"{word}: {count}")

#13
#word count
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,explode,col
spark = SparkSession.builder.appName("Word Count").getOrCreate()
# load the sample text
data = [("Kaustubh Karande",),("KSMSCIT015",),("Patience",)]
df = spark.createDataFrame(data,["text"])
df.show()
word_df = df.withColumn("words",explode(split(col("text")," ")))
word_df.show()
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,explode,col
spark = SparkSession.builder.appName("Word Count").getOrCreate()
# load the sample text
data = [("Kaustubh Karande",),("KSMSCIT015",),("Patience",)]
df = spark.createDataFrame(data,["text"])
df.show()
word_df = df.withColumn("words",explode(split(col("text")," ")))

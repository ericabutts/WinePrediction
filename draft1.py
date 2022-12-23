from pyspark.sql import SparkSession
from pyspark.ml.stat import Correlation
import pyspark.sql.functions as F
from pyspark.ml.linalg import Vector
from pyspark.ml.feature import VectorAssembler                    
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer


spark = SparkSession.builder.master("local").appName("test").getOrCreate()

trainingdata = spark.read.format("com.databricks.spark.csv").csv('s3://aws-logs-369586890818-us-east-1/elasticmapreduce/trainingdata.csv', infer$
validationdata = spark.read.format("com.databricks.spark.csv").csv('s3://aws-logs-369586890818-us-east-1/elasticmapreduce/validationdata.csv', i$


assembler = VectorAssembler(inputCols=['fixed acidity', 'volatile acidity', 'citric acid', 'residual sugar', 'chlorides', 'free sulfur dioxide',$
                               outputCol="features")


output = assembler.transform(trainingdata)


final_data = output.select("features", "quality")

train_data, test_data = final_data.randomSplit([0.7, 0.3])

#
total_columns = train_data.columns
labelIndexer = StringIndexer(inputCol=total_columns[-1], outputCol="indexedLabel").fit(train_data)


#

lr = LinearRegression(featuresCol="features", labelCol="quality")

trained_model = lr.fit(train_data)

trained_model

results = trained_model.evaluate(train_data)

print(results.r2) # not good 0.3743977007220025
print(results.meanSquaredError)
print(results.meanAbsoluteError)

unlabeled_data = test_data.select("features")

# unlabeled_data.show()

predictions = trained_model.transform(unlabeled_data)



predictions.write.save("s3://aws-logs-369586890818-us-east-1/elasticmapreduce/Prediction4.model")

predictions.show()

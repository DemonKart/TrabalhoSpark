# encoding=utf8  
import urllib
import ssl
import csv
import datetime as dt
# import matplotlib.pyplot as pp
import leather
from pyspark.sql.types import StringType, StructField, StructType, BooleanType, ArrayType, IntegerType
from pyspark.sql import Row, Column
from pyspark.sql import SQLContext
import pyspark.sql.functions as sqlfn
from pyspark.ml.feature import VectorAssembler
import pandas as pd
from pyspark.ml.clustering import KMeans

from collections import Counter
# from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier
# from pyspark.ml.clustering import KMeans
# from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
# from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler, SQLTransformer, OneHotEncoder
# from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
# from pyspark.mllib.linalg import Vectors
# from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

from pyspark.sql.types import *
import folium 
import functools
from datetime import datetime
import sys
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext
from decimal import *

reload(sys)  
sys.setdefaultencoding('utf8')
#!/usr/bin/env python
# encoding=utf8  
import urllib
import ssl
import csv
import datetime as dt
import leather
from pyspark.sql.types import StringType, StructField, StructType, BooleanType, ArrayType, IntegerType
from pyspark.sql import Row, Column
from pyspark.sql import SQLContext
import pyspark.sql.functions as sqlfn
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

from collections import Counter
from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier

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

SparkContext.setSystemProperty('spark.executor.memory', '2g')
SparkContext.setSystemProperty('spark.driver.maxResultSize', '5g')
SparkContext.setSystemProperty('spark.driver.memory', '5g')

sc = SparkContext("local", "App Name")

print "----\n\n\n"
spark = SQLContext(sc)

print "Lendo dados\n"

sources =  [spark.read.format("com.databricks.spark.csv").option("header", "true").load("dados/10kstudents.csv", inferSchema=True),\
        	spark.read.format("com.databricks.spark.csv").option("header", "true").load("dados/course_no_header.csv", inferSchema=True),\
        	spark.read.format("com.databricks.spark.csv").option("header", "true").load("dados/institution_no_header.csv", inferSchema=True)]

# ALUNO idade 		    			NU_IDADE_ALUNO
# ALUNO sexo	 					DS_SEXO_ALUNO
# IES   estado						SGL_UF_IES
# ALUNO escola publica x privada	CO_TIPO_ESCOLA_ENS_MEDIO
# ALUNO raça 						DS_COR_RACA_ALUNO
# CURSO nome						NO_CURSO
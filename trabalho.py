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

print "----\n"
spark = SQLContext(sc)

print "Reading data\n"

students = spark.read.format("com.databricks.spark.csv").option("header",'true').option("delimiter", '|').load("dados/10kstudents.csv", inferSchema=True)
courses = spark.read.format("com.databricks.spark.csv").option("header",'true').option("delimiter", '|').load("dados/course_no_header.csv", inferSchema=True)
institutions = spark.read.format("com.databricks.spark.csv").option("header",'true').option("delimiter", '|').load("dados/institution_no_header.csv", inferSchema=True)

students = students.select("CO_CURSO", "CO_IES", "IN_SEXO_ALUNO", "NU_IDADE_ALUNO", "CO_COR_RACA_ALUNO", "CO_TIPO_ESCOLA_ENS_MEDIO")
courses = courses.select("CO_CURSO", "NO_CURSO")
institutions = institutions.select("CO_IES", "CO_UF_IES")

course_names = courses.select("NO_CURSO").distinct().collect()

print "Resulting table created\n---\n"

print "Numbering course names\n"

names = []
for course_name in course_names:
	names.append(course_name)

courses2 = courses.collect()
courses = []
for course in courses2:
	courses.append((course[0],names.index(Row(NO_CURSO=course[1]))))

courses = spark.createDataFrame(courses, ["CO_CURSO", "NO_CURSO"])

print "Applying ML\n"

sc = students.join(courses, "CO_CURSO")
sci = sc.join(institutions, "CO_IES")

# CO_UF_IES
# NO_CURSO
# IN_SEXO_ALUNO
# NU_IDADE_ALUNO
# CO_COR_RACA_ALUNO
# CO_TIPO_ESCOLA_ENS_MEDIO

# features = ['type', 'responsable', 'stages']
# assembler = VectorAssembler(inputCols=features, outputCol="features")
# dataFinal = assembler.transform(completeData)

# dt = DecisionTreeClassifier(labelCol='uf_code', featuresCol='features', maxDepth=5)
# (treinamento, teste) = dataFinal.randomSplit([0.8, 0.2])
# model = dt.fit(treinamento)
# predictions = model.transform(teste)
# print model.toDebugString
# total = predictions.count()
# missed = predictions.where("uf_code != prediction").count()

# resultados.append({"id": "Unidade Federativa", "TOTAL": total, "MISSED": missed})

# SELECT  i.CO_UF_IES, 
#         c.NO_CURSO,
#         s.IN_SEXO_ALUNO,
#         s.NU_IDADE_ALUNO,
#         s.CO_COR_RACA_ALUNO,
#         s.CO_TIPO_ESCOLA_ENS_MEDIO
# FROM students s 
#     JOIN courses c on c.CO_CURSO = s.CO_CURSO
#     JOIN institutions i on i.CO_IES = s.CO_IES

# ALUNO idade                                   NU_IDADE_ALUNO
# ALUNO sexo                                            IN_SEXO_ALUNO
# IES   estado                                          CO_UF_IES
# ALUNO escola publica x privada        CO_TIPO_ESCOLA_ENS_MEDIO
# ALUNO raça                                            CO_COR_RACA_ALUNO
# CURSO nome                                            NO_CURSO

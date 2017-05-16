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

spark = SQLContext(sc)

# print "Reading data\n"

s = spark.read.format("com.databricks.spark.csv").option("header",'true').option("delimiter", '|').load("dados/"+sys.argv[1]+"/student_no_header.csv", inferSchema=True)
courses = spark.read.format("com.databricks.spark.csv").option("header",'true').option("delimiter", '|').load("dados/"+sys.argv[1]+"/course_no_header.csv", inferSchema=True)
institutions = spark.read.format("com.databricks.spark.csv").option("header",'true').option("delimiter", '|').load("dados/"+sys.argv[1]+"/institution_no_header.csv", inferSchema=True)

# CO_ALUNO_SITUACAO
# 2. Cursando
# 3. Matrícula trancada
# 4. Desvinculado do curso
# 5. Transferido para outro curso da mesma IES
# 6. Formado
# 7. Falecido"

s_fields = [s.CO_CURSO,\
			s.CO_IES,\
			s.IN_RESERVA_ENSINO_PUBLICO,\
			s.IN_RESERVA_RENDA_FAMILIAR,\
			sqlfn.when(s.CO_ALUNO_SITUACAO == 4, 1).when(s.CO_ALUNO_SITUACAO == 5, 1).otherwise(0).alias('EVASOR')]

# Evasores
s_filters = "IN_RESERVA_ENSINO_PUBLICO IS NOT NULL AND IN_RESERVA_RENDA_FAMILIAR IS NOT NULL"
students = s.select(s_fields).where(s_filters)

# CO_NIVEL_ACAD 1 -> graduacao
courses = courses.select("CO_CURSO", "CO_OCDE_AREA_GERAL").where("CO_NIVEL_ACADEMICO = 1")
institutions = institutions.select("CO_IES", "CO_UF_IES")

# print "Applying ML\n"

sc = students.join(courses, "CO_CURSO")
sci = sc.join(institutions, "CO_IES").drop("CO_ALUNO_SITUACAO","CO_OCDE_AREA_GERAL", "CO_UF_IES", "CO_IES", "CO_CURSO")

todas = ["EVASOR", "IN_RESERVA_ENSINO_PUBLICO", "IN_RESERVA_RENDA_FAMILIAR"]

# for i in range(0,len(todas)-1):
features = todas
i=0
varx = features.pop(0)
assembler = VectorAssembler(inputCols=features, outputCol="features")
dataFinal = assembler.transform(sci)
dt = DecisionTreeClassifier(labelCol=varx, featuresCol='features', maxDepth=5)
(treinamento, teste) = dataFinal.randomSplit([0.8, 0.2])
model = dt.fit(treinamento)
predictions = model.transform(teste)
# print model.toDebugString
total = predictions.count()
missed = predictions.where(str(varx)+" != prediction").count()
_00 = predictions.where(varx+"=0 and prediction = 0").count()
_01 = predictions.where(varx+"=0 and prediction = 1").count()
_10 = predictions.where(varx+"=1 and prediction = 0").count()
_11 = predictions.where(varx+"=1 and prediction = 1").count()
print sys.argv[1]
print "-----\n"
print total, "Erradas: ", missed, "Erro(%): ", float(missed) / float(total) * 100
print "0\t", _00, "\t|\t", _01
print "1\t", _10, "\t|\t", _11

# resultados.append({"id": "Unidade Federativa", "TOTAL": total, "MISSED": missed})

# SELECT  i.CO_UF_IES,
#         c.CO_OCDE_AREA_GERAL,
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
# CURSO nome                                            CO_OCDE_AREA_GERAL

# course_names = courses.select("CO_OCDE_AREA_GERAL").distinct().collect()

# print "Resulting table created\n---\n"

# print "Numbering course names\n"

# names = []
# for course_name in course_names:
#       names.append(course_name)

# courses2 = courses.collect()
# courses = []
# for course in courses2:
#       courses.append((course[0],names.index(Row(CO_OCDE_AREA_GERAL=course[1]))))

# courses = spark.createDataFrame(courses, ["CO_CURSO", "CO_OCDE_AREA_GERAL"])
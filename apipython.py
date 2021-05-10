import requests
import json
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark import SQLContext
#url = "http://api.equipo1.tech/get?access_key=9d1f065a6a9816f9dd7667b0c9b26bab"
url = "http://api.equipo1.tech/get"
response = requests.get(url)
print(response)
data= response.text
general=0
#print(type(data))
#print(data)
#print(data)
parsed = json.loads(data)
#print("hola",type(parsed))
rdd= sc.parallelize(parsed)
rddCollect = rdd.collect()
#print(rddCollect)
print("----Apagados---")
apagado_filter= rdd.filter(lambda x: 'A' in x)
apagado_filtered = apagado_filter.collect()
apagado_filtered = sc.parallelize(apagado_filtered)
napagado= apagado_filtered.count()
print("Numero de apagados:",napagado)
general=general+napagado
#print("filtro",filtered)
print("----DERECHA---")
d_filter= rdd.filter(lambda x: 'D' in x)
der_filtered = d_filter.collect()
der_filtered = sc.parallelize(der_filtered)
nder= der_filtered.count()
print("Numero de derecha:",nder)
general=general+nder
print("----IZQUIERDA---")
i_filter= rdd.filter(lambda x: 'I' in x)
iz_filtered = i_filter.collect()
iz_filtered = sc.parallelize(iz_filtered)
niz= iz_filtered.count()
print("Numero de izquierdaz:",niz)
general=general+niz
print("--------")
print("Numero de registros:",general)
print("Porcentaje derecha",(int((nder*100)/general)),"%")
print("Porcentaje izquierda",(int((niz*100)/general)),"%")
print("Porcentaje apagado",(int((napagado*100)/general)),"%")

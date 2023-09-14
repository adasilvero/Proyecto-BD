# Importar las bibliotecas necesarias
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import matplotlib.pyplot as plt

# Configurar la sesión de Spark
spark = SparkSession.builder \
    .appName("GestionDeDatosEcommerce") \
    .getOrCreate()

# Cargar los datos del comportamiento de los usuarios
user_data = spark.read.csv("datos_usuario.csv", header=True, inferSchema=True)

# Realizar análisis de datos
# Ejemplo: Calcular la tasa de abandono en el proceso de compra
abandon_rate = user_data.filter(col("etapa_compra") == "abandono").count() / user_data.count()

# Generar un gráfico de las páginas más visitadas
page_views = user_data.groupBy("pagina_visitada").count().orderBy(col("count").desc())
page_views.limit(10).toPandas().plot(kind="bar", x="pagina_visitada", y="count")
plt.title("Páginas más visitadas")
plt.xlabel("Página")
plt.ylabel("Número de visitas")
plt.show()

# Realizar más análisis y generación de informes

# Cerrar la sesión de Spark
spark.stop()

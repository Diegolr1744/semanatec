from pyspark.sql import SparkSession
import json
import os

if __name__ == "__main__":
    # Crear sesión de Spark
    spark = SparkSession.builder.appName("mental_health_analysis").getOrCreate()

    # Cargar dataset
    path_data = "student_depression_dataset.csv"
    df_health = spark.read.csv(path_data, header=True, inferSchema=True)

    # Asegurar que el directorio de resultados existe
    os.makedirs("results", exist_ok=True)

    # Guardar datos sin procesar
    df_health.write.mode("overwrite").json("results/raw_health_data.json")

    # Crear vista temporal
    df_health.createOrReplaceTempView("mental_health")

    # Top 10 personas con mayor presión académica
    query_academic_pressure = """
        SELECT id, Gender, Age, City, Profession, Academic_Pressure
        FROM mental_health
        ORDER BY Academic_Pressure DESC
        LIMIT 10
    """
    df_top_academic_pressure = spark.sql(query_academic_pressure)
    df_top_academic_pressure.show()
    with open("results/top_academic_pressure.json", "w") as f:
        json.dump(df_top_academic_pressure.toJSON().collect(), f)

    # Top 10 personas con mayor estrés financiero
    query_financial_stress = """
        SELECT id, Gender, Age, City, Profession, Financial_Stress
        FROM mental_health
        ORDER BY Financial_Stress DESC
        LIMIT 10
    """
    df_top_financial_stress = spark.sql(query_financial_stress)
    df_top_financial_stress.show()
    with open("results/top_financial_stress.json", "w") as f:
        json.dump(df_top_financial_stress.toJSON().collect(), f)

    # Top 10 personas con menos horas de sueño
    query_sleep_duration = """
        SELECT id, Gender, Age, City, Profession, Sleep_Duration
        FROM mental_health
        ORDER BY Sleep_Duration ASC
        LIMIT 10
    """
    df_top_sleep_deprivation = spark.sql(query_sleep_duration)
    df_top_sleep_deprivation.show()
    with open("results/top_sleep_deprivation.json", "w") as f:
        json.dump(df_top_sleep_deprivation.toJSON().collect(), f)

    # Apagar sesión de Spark
    spark.stop()
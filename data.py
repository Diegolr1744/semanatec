from pyspark.sql import SparkSession
import json
import os

if __name__ == "__main__":
 
    spark = SparkSession.builder.appName("mental_health_analysis").getOrCreate()


    path_data = "student_depression_dataset.csv"
    df_health = spark.read.csv(path_data, header=True, inferSchema=True)

 
    os.makedirs("results", exist_ok=True)


    df_health.write.mode("overwrite").json("results/raw_health_data.json")


    df_health.createOrReplaceTempView("mental_health")


    query_academic_pressure = """
        SELECT id, Gender, Age, City, Profession, `Academic Pressure`
        FROM mental_health
        ORDER BY `Academic Pressure` DESC
        LIMIT 10
    """
    df_top_academic_pressure = spark.sql(query_academic_pressure)
    df_top_academic_pressure.show()
    with open("results/top_academic_pressure.json", "w") as f:
        json.dump(df_top_academic_pressure.toJSON().collect(), f)

  
    query_financial_stress = """
        SELECT id, Gender, Age, City, Profession, `Financial Stress`
        FROM mental_health
        ORDER BY `Financial Stress` DESC
        LIMIT 10
    """
    df_top_financial_stress = spark.sql(query_financial_stress)
    df_top_financial_stress.show()
    with open("results/top_financial_stress.json", "w") as f:
        json.dump(df_top_financial_stress.toJSON().collect(), f)

  
    query_sleep_duration = """
        SELECT id, Gender, Age, City, Profession, `Sleep Duration`
        FROM mental_health
        ORDER BY `Sleep Duration` ASC
        LIMIT 10
    """
    df_top_sleep_deprivation = spark.sql(query_sleep_duration)
    df_top_sleep_deprivation.show()
    with open("results/top_sleep_deprivation.json", "w") as f:
        json.dump(df_top_sleep_deprivation.toJSON().collect(), f)

   
    spark.stop()

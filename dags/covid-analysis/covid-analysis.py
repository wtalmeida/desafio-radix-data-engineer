from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Airflow',
    'email': ['wtalmeida.1986@gmail.com'],
    'start_date': days_ago(1),
    'email_on_failure' : False
}

dag=DAG(
    dag_id = 'dag-covid-analysis',
    description = 'Desafio - Analise COVID',
    default_args = default_args,
    catchup=False,
    schedule_interval='0 09 * * *',
    tags=['desafio-covid']
)

# define function
def fuc_ingestion():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as f
    from pyspark.sql.types import IntegerType, DoubleType, TimestampType, LongType
    from pyspark.sql import Window

    sc = SparkSession.builder \
        .master("local[*]") \
        .appName("airflow_app") \
        .config('spark.executor.memory', '6g') \
        .config('spark.driver.memory', '6g') \
        .config("spark.driver.maxResultSize", "1048MB") \
        .config("spark.port.maxRetries", "100") \
        .getOrCreate()
    
    sc.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    print(' >>>> TASK 01 <<<<<')        

    # --------------------------------------------------------------
    def get_info_csv(type):
        df_spark = sc.read.csv(f"/home/airflow/datalake/landing/covid19/time_series_covid19_{type}_global.csv", inferSchema=True, header=True)
        #df_spark.printSchema()
        cols_fixed = ['Province/State', 'Country/Region', 'Lat', 'Long']
        cols_dyn = [colname for colname in df_spark.schema.names if colname not in cols_fixed]
        stack = ''
        for x in cols_dyn:
            stack += f",'{x}', `{x}`"

        # TIRAR DO PIVOT
        unpivot_expr = f'stack ({len(cols_dyn)} {stack}) as (Date, Qty_{type})'
        unpivot_df = df_spark.select('Province/State', 'Country/Region', 'Lat', 'Long', f.expr(unpivot_expr))
        new_names = ["estado", "pais", "latitude", "longitude", "data", "acumulados"]
        unpivot_df = unpivot_df.toDF(*new_names)
        unpivot_df = unpivot_df.select("pais", "estado", "latitude", "longitude", "data", "acumulados")
        unpivot_df = unpivot_df.withColumn('data',f.to_date(f.unix_timestamp('data', 'MM/dd/yy').cast('timestamp')))
        unpivot_df = unpivot_df.na.fill(value='none',subset=["estado"])
        unpivot_df = unpivot_df.na.fill(value='0',subset=new_names)

        # CRIAR CAMPO "SOMENTE NOVOS CASOS"
        new_field = 'quantidade_'
        if type == 'confirmed':
            new_field += 'confirmados'
        elif type == 'deaths':
            new_field += 'mortes'
        elif type == 'recovered':
            new_field += 'recuperados'

        w = Window.partitionBy("pais", "estado", "latitude", "longitude").orderBy('data')
        unpivot_df = unpivot_df.withColumn('lead', f.lag(f"acumulados", 1).over(w)) \
            .withColumn(new_field, f.when(f.col('lead').isNotNull(), f.col('acumulados') - f.col('lead')).otherwise(f.lit(None)))

        unpivot_df = unpivot_df.select("pais", "estado", "latitude", "longitude", "data", new_field)
        unpivot_df = unpivot_df.withColumn(new_field, unpivot_df[new_field].cast(LongType()))
        unpivot_df = unpivot_df.na.fill(value=0,subset=[new_field])

        return unpivot_df
        # --------------------------------------------------------------

    # 3 DFs
    print('...Confirmed...=', end=' ')
    df_confirmed = get_info_csv(type = 'confirmed')
    print(df_confirmed.count())

    print('...Deaths......=', end=' ')
    df_deaths = get_info_csv(type = 'deaths')
    print(df_deaths.count())

    print('...Recovered...=', end=' ')
    df_recovered = get_info_csv(type = 'recovered')
    print(df_recovered.count())

    # MERGE INFO
    print('...MERGING INFO....')
    full_df = df_confirmed.join(df_deaths, ["pais", "estado", "data", "latitude", "longitude"], how = "full").select("*")
    full_df = full_df.join(df_recovered, ["pais", "estado", "data", "latitude", "longitude"], how = "full").select("*")
    full_df = full_df.orderBy(["pais", "estado", "data"], ascending=True)

    # CAMPOS PARA PARTICAO
    print('...FIELDS TO PARTITION...')
    full_df = full_df.withColumn("year", f.date_format(f.col("data"), "yyyy"))\
                    .withColumn("month", f.date_format(f.col("data"), "MM"))

    # SALVAR EM PARQUET - PARTICAO POR "ANO"/"MES" - COM 1 ARQUIVO POR PARTICAO
    print('...SAVING FILES - PARQUET')
    full_df.coalesce(1) \
            .write.option("header",True) \
            .partitionBy("year", "month") \
            .mode("overwrite") \
            .parquet("/home/airflow/datalake/raw")      

    print(' >>>> TASK 01 :: FINISHED <<<<<')                   


# define function
def fuc_processing():
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as f
    from pyspark.sql.types import IntegerType, DoubleType, TimestampType, LongType, FloatType
    from pyspark.sql import Window

    sc = SparkSession.builder \
        .master("local[*]") \
        .appName("airflow_app") \
        .config('spark.executor.memory', '6g') \
        .config('spark.driver.memory', '6g') \
        .config("spark.driver.maxResultSize", "1048MB") \
        .config("spark.port.maxRetries", "100") \
        .getOrCreate()

    sc.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    print(' >>>> TASK 02 <<<<<') 

    print('...READING PARQUET....')
    df_casos = sc.read.parquet("/home/airflow/datalake/raw")

    # MEDIA MOVEL - ULTIMOS 7 DIAS
    print('...CALCULATING MOVING AVERAGE....')
    days = lambda i: i * 86400
    windowSpec = (Window().partitionBy([f.col("pais"), f.col("estado")]).orderBy(df_casos.data.cast('timestamp').cast('long')).rangeBetween(-days(6), 0))
    df_casos_media = df_casos.withColumn("media_movel_confirmados", f.round(f.avg("quantidade_confirmados").over(windowSpec), 2).cast('long'))
    df_casos_media = df_casos_media.withColumn("media_movel_mortes", f.round(f.avg("quantidade_mortes").over(windowSpec), 2).cast('long'))
    df_casos_media = df_casos_media.withColumn("media_movel_recuperados", f.round(f.avg("quantidade_recuperados").over(windowSpec), 2).cast('long'))

    df_casos_media = df_casos_media.select("pais", "data", "media_movel_confirmados", "media_movel_mortes", "media_movel_recuperados", "year") 
    df_casos_media = df_casos_media.orderBy("pais", "data", ascending=True)

    # SALVAR EM PARQUET - PARTICAO POR "ANO" - COM 1 ARQUIVO POR PARTICAO
    print('...SAVING "REFINED"....')
    df_casos_media.coalesce(1)\
            .write.option("header",True) \
            .partitionBy("year") \
            .mode("overwrite") \
            .parquet("/home/airflow/datalake/refined")

    print(' >>>> TASK 02 :: FINISHED <<<<<')       

# region BASE TABLES
t01 = PythonOperator(
    task_id='t01_ingestion',
    python_callable=fuc_ingestion,
    dag=dag
)

# region BASE TABLES
t02 = PythonOperator(
    task_id='t02_processing',
    python_callable=fuc_processing,
    dag=dag
)    

t01 >> t02
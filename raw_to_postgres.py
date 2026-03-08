import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, when, regexp_replace
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, TimestampType, IntegerType
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Параметры подключения к PostgreSQL из переменных окружения
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'beeline_db')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'beeline_user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'beeline_password')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')

# URL для подключения к PostgreSQL
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Путь к исходным данным
RAW_PATH = "/app/raw"


def create_spark_session():
    """Создание Spark сессии"""
    return SparkSession.builder \
        .appName("Raw to PostgreSQL") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()


def read_csv_with_delimiter(spark, file_path):
    """Чтение CSV с разделителем ;"""
    return spark.read \
        .option("header", "true") \
        .option("delimiter", ";") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .option("multiLine", "true") \
        .csv(file_path)


def clean_column_names(df):
    """Очистка имен колонок"""
    for old_name in df.columns:
        clean_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in old_name)
        while '__' in clean_name:
            clean_name = clean_name.replace('__', '_')
        clean_name = clean_name.strip('_')

        if clean_name != old_name and clean_name:
            logger.info(f"   Переименование: '{old_name}' -> '{clean_name}'")
            df = df.withColumnRenamed(old_name, clean_name)
    return df


def write_to_postgres(df, table_name, mode="overwrite"):
    """Запись DataFrame в PostgreSQL"""
    try:
        # Пробуем записать с текущими настройками
        df.write \
            .mode(mode) \
            .option("truncate", "true") \
            .jdbc(POSTGRES_URL, f"ods.{table_name}", properties=POSTGRES_PROPERTIES)
        logger.info(f"   ✓ Данные записаны в ods.{table_name}")
    except Exception as e:
        logger.error(f"   ✗ Ошибка при записи в ods.{table_name}: {str(e)}")

        # Альтернативный метод записи с дополнительными параметрами
        try:
            logger.info(f"   Повторная попытка с batchsize=1000...")
            df.write \
                .mode(mode) \
                .option("truncate", "true") \
                .option("batchsize", "1000") \
                .option("numPartitions", "4") \
                .jdbc(POSTGRES_URL, f"ods.{table_name}", properties=POSTGRES_PROPERTIES)
            logger.info(f"   ✓ Данные записаны в ods.{table_name}")
        except Exception as e2:
            logger.error(f"   ✗ Ошибка при повторной записи: {str(e2)}")
            raise


def process_banners(spark):
    """Обработка fct_banners_show"""
    logger.info("1. Обработка fct_banners_show...")

    df = read_csv_with_delimiter(spark, f"{RAW_PATH}/Fct_banners_show.csv")
    df = clean_column_names(df)

    # Преобразование типов
    df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
    df = df.withColumn("is_clicked_0_1", col("is_clicked_0_1").cast(IntegerType()))


    df = df.withColumnRenamed("timestamp", "event_timestamp")
    df = df.withColumnRenamed("placement_сайт_приложение_соцсеть", "placement")
    df = df.withColumnRenamed("is_clicked_0_1", "is_clicked")

    # Очистка
    initial_count = df.count()
    logger.info(f"   Прочитано строк: {initial_count}")

    df = df.filter(col("user_id").isNotNull())
    logger.info(f"   После удаления NULL user_id: {df.count()} строк")

    # Дедупликация
    window_spec = Window.partitionBy("banner_id", "user_id", "event_timestamp").orderBy("event_timestamp")
    df = df.withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn")
    logger.info(f"   После дедупликации: {df.count()} строк")

    write_to_postgres(df, "fct_banners")
    return df


def process_installs(spark):
    """Обработка installs"""
    logger.info("\n2. Обработка installs...")

    df = read_csv_with_delimiter(spark, f"{RAW_PATH}/Installs.csv")
    df = clean_column_names(df)

    df = df.withColumnRenamed("source_баннер_органика_другое", "source")
    df = df.withColumn("install_timestamp", col("install_timestamp").cast(TimestampType()))

    logger.info(f"   Прочитано строк: {df.count()}")
    df = df.filter(col("user_id").isNotNull())
    logger.info(f"   После удаления NULL user_id: {df.count()} строк")

    write_to_postgres(df, "installs")
    return df


def process_actions(spark):
    """Обработка fct_actions"""
    logger.info("\n3. Обработка fct_actions...")

    df = read_csv_with_delimiter(spark, f"{RAW_PATH}/Fct_actions.csv")
    df = clean_column_names(df)

    # Переименовываем после clean_column_names, используя новое имя
    df = df.withColumnRenamed("actions_регистрация_первый_заказ_и_т_д", "actions")

    df = df.withColumn("session_start", col("session_start").cast(TimestampType()))

    logger.info(f"   Прочитано строк: {df.count()}")
    df = df.filter(col("user_id").isNotNull())
    logger.info(f"   После удаления NULL user_id: {df.count()} строк")

    write_to_postgres(df, "fct_actions")
    return df


def process_banner_meta(spark):
    """Обработка cd_banner"""
    logger.info("\n4. Обработка cd_banner...")

    df = read_csv_with_delimiter(spark, f"{RAW_PATH}/CD_banner.csv")
    df = clean_column_names(df)


    df = df.withColumnRenamed("creative_type_статика_видео_анимация", "creative_type")
    df = df.withColumnRenamed("message_сообщение_на_баннере", "message")

    logger.info(f"   Прочитано строк: {df.count()}")
    df = df.dropDuplicates(["banner_id"])
    logger.info(f"   После дедупликации: {df.count()} строк")

    write_to_postgres(df, "cd_banner")
    return df


def process_campaign(spark, df_banners):
    """Обработка cd_campaign с расчетом CPM/CPC"""
    logger.info("\n5. Обработка cd_campaign...")

    df = read_csv_with_delimiter(spark, f"{RAW_PATH}/CD_campaign.csv")
    df = clean_column_names(df)

    # Преобразование daily_budget в число
    df = df.withColumn("daily_budget",
                       regexp_replace(col("daily_budget"), ",", ".").cast(DoubleType()))

    # Преобразование дат
    from pyspark.sql.functions import to_date
    df = df.withColumn("start_date", to_date(col("start_date"), "dd.MM.yyyy"))
    df = df.withColumn("end_date", to_date(col("end_date"), "dd.MM.yyyy"))

    logger.info(f"   Прочитано строк: {df.count()}")

    # Расчет статистики по баннерам
    logger.info("   Расчет CPM/CPC...")

    banner_stats = df_banners.groupBy("campaign_id").agg(
        {"*": "count", "is_clicked": "sum"}
    ).withColumnRenamed("count(1)", "impressions") \
        .withColumnRenamed("sum(is_clicked)", "clicks")

    banner_stats = banner_stats.withColumn("impressions", col("impressions").cast(DoubleType()))
    banner_stats = banner_stats.withColumn("clicks", col("clicks").cast(DoubleType()))

    df_with_stats = df.join(banner_stats, "campaign_id", "left")

    # Заполнение NULL
    df_with_stats = df_with_stats \
        .withColumn("impressions", when(col("impressions").isNull(), 0).otherwise(col("impressions"))) \
        .withColumn("clicks", when(col("clicks").isNull(), 0).otherwise(col("clicks")))

    # Расчет метрик
    df_final = df_with_stats \
        .withColumn("calculated_cpm",
                    when(col("impressions") > 0,
                         (col("daily_budget") * 1000 / col("impressions")).cast(DoubleType()))
                    .otherwise(0.0)) \
        .withColumn("calculated_cpc",
                    when(col("clicks") > 0,
                         (col("daily_budget") / col("clicks")).cast(DoubleType()))
                    .otherwise(0.0))

    df_final = df_final.dropDuplicates(["campaign_id"])
    logger.info(f"   После обработки: {df_final.count()} строк")

    write_to_postgres(df_final, "cd_campaign")
    return df_final

def process_user(spark):
    """Обработка cd_user"""
    logger.info("\n6. Обработка cd_user...")

    df = read_csv_with_delimiter(spark, f"{RAW_PATH}/CD_user.csv")
    df = clean_column_names(df)

    # Преобразование дат
    df = df.withColumn("date_create", col("date_create").cast(TimestampType()))
    df = df.withColumn("date_end", col("date_end").cast(TimestampType()))

    logger.info(f"   Прочитано строк: {df.count()}")
    df = df.filter(col("user_id").isNotNull())
    logger.info(f"   После удаления NULL user_id: {df.count()} строк")

    write_to_postgres(df, "cd_user")
    return df


def main():
    """Основная функция"""
    logger.info("=" * 60)
    logger.info("Начало загрузки данных в PostgreSQL")
    logger.info("=" * 60)

    spark = None
    try:
        # Создание Spark сессии
        spark = create_spark_session()

        # Проверка подключения к PostgreSQL
        logger.info(f"Подключение к PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

        # Обработка данных
        df_banners = process_banners(spark)
        df_installs = process_installs(spark)
        df_actions = process_actions(spark)
        df_banner_meta = process_banner_meta(spark)
        df_campaign = process_campaign(spark, df_banners)
        df_user = process_user(spark)

        logger.info("\n" + "=" * 60)
        logger.info("✓ Все данные успешно загружены в PostgreSQL")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Ошибка при выполнении: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, when

# Инициализация Spark
spark = SparkSession.builder \
    .appName("Raw to ODS") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Путь к исходным данным (локально)
RAW_PATH = "/Users/marinamarina/PycharmProjects/data_beeline/raw"
ODS_PATH = "/Users/marinamarina/PycharmProjects/data_beeline/ods"


# Функция для чтения CSV с разделителем ;
def read_csv_with_delimiter(file_path):
    return spark.read \
        .option("header", "true") \
        .option("delimiter", ";") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .option("multiLine", "true") \
        .csv(file_path)


# Функция для очистки имен колонок (удаление лишних символов)
def clean_column_names(df):
    for old_name in df.columns:
        # Оставляем только буквы, цифры и подчеркивания
        clean_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in old_name)
        # Удаляем множественные подчеркивания
        while '__' in clean_name:
            clean_name = clean_name.replace('__', '_')
        # Убираем подчеркивания в начале и конце
        clean_name = clean_name.strip('_')

        # Если имя изменилось или содержит спецсимволы
        if clean_name != old_name:
            print(f"   Переименование: '{old_name}' -> '{clean_name}'")
            df = df.withColumnRenamed(old_name, clean_name)

    return df


print("=" * 50)
print("Начало загрузки данных в ODS слой")
print("=" * 50)

# 1. Чтение и очистка fct_banners_show
print("\n1. Обработка fct_banners_show...")
df_banners = read_csv_with_delimiter(f"{RAW_PATH}/fct_banners_show.csv")
df_banners = clean_column_names(df_banners)

print(f"   Прочитано строк: {df_banners.count()}")
print(f"   Колонки: {df_banners.columns}")

# Удаление NULL user_id
df_banners = df_banners.filter(col("user_id").isNotNull())
print(f"   После удаления NULL user_id: {df_banners.count()} строк")

# Дедупликация по (banner_id, user_id, timestamp)
window_spec = Window.partitionBy("banner_id", "user_id", "timestamp").orderBy("timestamp")
df_banners = df_banners.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")
print(f"   После дедупликации: {df_banners.count()} строк")

# Запись в ODS
df_banners.write.mode("overwrite").parquet(f"{ODS_PATH}/fct_banners")
print("   ✓ Записано в ODS")

# 2. Установки приложения
print("\n2. Обработка installs...")
df_installs = read_csv_with_delimiter(f"{RAW_PATH}/installs.csv")
df_installs = clean_column_names(df_installs)

print(f"   Прочитано строк: {df_installs.count()}")
print(f"   Колонки: {df_installs.columns}")

df_installs = df_installs.filter(col("user_id").isNotNull())
print(f"   После удаления NULL user_id: {df_installs.count()} строк")

df_installs.write.mode("overwrite").parquet(f"{ODS_PATH}/installs")
print("   ✓ Записано в ODS")

# 3. Поведение пользователей
print("\n3. Обработка fct_actions...")
df_actions = read_csv_with_delimiter(f"{RAW_PATH}/fct_actions.csv")
df_actions = clean_column_names(df_actions)

print(f"   Прочитано строк: {df_actions.count()}")
print(f"   Колонки: {df_actions.columns}")

df_actions = df_actions.filter(col("user_id").isNotNull())
print(f"   После удаления NULL user_id: {df_actions.count()} строк")

df_actions.write.mode("overwrite").parquet(f"{ODS_PATH}/fct_actions")
print("   ✓ Записано в ODS")

# 4. Метаданные баннеров
print("\n4. Обработка cd_banner...")
df_banner_meta = read_csv_with_delimiter(f"{RAW_PATH}/cd_banner.csv")
df_banner_meta = clean_column_names(df_banner_meta)

print(f"   Прочитано строк: {df_banner_meta.count()}")
print(f"   Колонки: {df_banner_meta.columns}")

df_banner_meta = df_banner_meta.dropDuplicates(["banner_id"])
print(f"   После дедупликации: {df_banner_meta.count()} строк")

df_banner_meta.write.mode("overwrite").parquet(f"{ODS_PATH}/cd_banner")
print("   ✓ Записано в ODS")

# 5. Бюджет и ставки
print("\n5. Обработка cd_campaign...")
df_campaign = read_csv_with_delimiter(f"{RAW_PATH}/cd_campaign.csv")
df_campaign = clean_column_names(df_campaign)

# Приводим daily_budget к числовому типу
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, when, regexp_replace

# Очищаем и преобразуем daily_budget в число
df_campaign = df_campaign.withColumn("daily_budget",
    regexp_replace(col("daily_budget"), ",", ".").cast(DoubleType()))

print(f"   Прочитано строк: {df_campaign.count()}")
print(f"   Колонки: {df_campaign.columns}")
print("   Типы данных:")
df_campaign.printSchema()

# Расчет cpm/cpc (если есть показы и клики)
print("   Расчет CPM/CPC...")

# Используем актуальное имя колонки is_clicked_0_1
banner_stats = df_banners.groupBy("campaign_id").agg(
    {"*": "count", "is_clicked_0_1": "sum"}
).withColumnRenamed("count(1)", "impressions") \
 .withColumnRenamed("sum(is_clicked_0_1)", "clicks")

# Приводим impressions и clicks к числовому типу
banner_stats = banner_stats.withColumn("impressions", col("impressions").cast(DoubleType()))
banner_stats = banner_stats.withColumn("clicks", col("clicks").cast(DoubleType()))

print(f"   Статистика по баннерам:")
banner_stats.show()

df_campaign_with_stats = df_campaign.join(banner_stats, "campaign_id", "left")

# Заполняем NULL значения в impressions и clicks
df_campaign_with_stats = df_campaign_with_stats \
    .withColumn("impressions", when(col("impressions").isNull(), 0).otherwise(col("impressions"))) \
    .withColumn("clicks", when(col("clicks").isNull(), 0).otherwise(col("clicks")))

# Рассчитываем CPM и CPC с проверкой на деление на ноль
df_campaign_with_stats = df_campaign_with_stats \
    .withColumn("calculated_cpm",
                when(col("impressions") > 0,
                     (col("daily_budget") * 1000 / col("impressions")).cast(DoubleType()))
                .otherwise(0.0)) \
    .withColumn("calculated_cpc",
                when(col("clicks") > 0,
                     (col("daily_budget") / col("clicks")).cast(DoubleType()))
                .otherwise(0.0))

# Удаляем дубликаты если есть
df_campaign_final = df_campaign_with_stats.dropDuplicates(["campaign_id"])
print(f"   После дедупликации: {df_campaign_final.count()} строк")

print("   Результат расчета:")
df_campaign_final.select(
    "campaign_id",
    "daily_budget",
    "impressions",
    "clicks",
    "calculated_cpm",
    "calculated_cpc"
).show(truncate=False)

df_campaign_final.write.mode("overwrite").parquet(f"{ODS_PATH}/cd_campaign")
print("   ✓ Записано в ODS")

# 6. Данные пользователей
print("\n6. Обработка cd_user...")
df_user = read_csv_with_delimiter(f"{RAW_PATH}/cd_user.csv")
df_user = clean_column_names(df_user)

print(f"   Прочитано строк: {df_user.count()}")
print(f"   Колонки: {df_user.columns}")

df_user = df_user.filter(col("user_id").isNotNull())
print(f"   После удаления NULL user_id: {df_user.count()} строк")

df_user.write.mode("overwrite").parquet(f"{ODS_PATH}/cd_user")
print("   ✓ Записано в ODS")

print("\n" + "=" * 50)
print("Данные успешно загружены в ODS слой")
print(f"ODS слой сохранен в: {ODS_PATH}")
print("=" * 50)

spark.stop()
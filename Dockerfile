FROM python:3.9-slim-bullseye

# Установка Java (используем OpenJDK 17, который доступен)
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Установка Python зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Создание рабочей директории
WORKDIR /app

# Копирование скрипта
COPY raw_to_postgres.py .

# Запуск приложения
CMD ["python", "raw_to_postgres.py"]
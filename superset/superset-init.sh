#!/bin/bash

# Инициализация базы данных Superset
superset db upgrade

# Создание учетной записи администратора
superset fab create-admin --username "${ADMIN_USERNAME}" --firstname Superset --lastname Admin --email "${ADMIN_EMAIL}" --password "${ADMIN_PASSWORD}"

# Загрузка примерных данных (если необходимо)
# superset load_examples

# Настройка Superset
superset init

# Запуск Superset
gunicorn \
  --bind 0.0.0.0:8088 \
  --workers 4 \
  --timeout 60 \
  --keep-alive 10 \
  "superset.app:create_app()"

# Ждем 60 секунд перед добавлением подключения к базе данных
# sleep 60

# # Добавление подключения к базе данных PostgreSQL
# superset dbs add \
#   --database-name box_office \
#   --sqlalchemy-uri postgresql+psycopg2://airflow:airflow@postgres:5432/box_office \
#   --configuration-method src \
#   --username admin \
#   --password admin
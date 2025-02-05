# Использование официального образа PostgreSQL
FROM postgres:15

# Установка кодировки UTF-8
ENV LANG=C.UTF-8

# Копирование SQL-скрипта для инициализации БД
COPY init.sql /docker-entrypoint-initdb.d/

# Запуск PostgreSQL
CMD ["postgres", "-c", "log_statement=all"]
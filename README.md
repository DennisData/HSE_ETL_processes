# HSE_ETL_processes
Repository for HSE_ETL_processes course

Kravets Denis

В результате проделанной работы были развёрнуты MongoDB, PostgreSQL, Airflow. Настроена репликация данных из MongoDB в PostgreSQL.

Коллекции в MongpDB заполнены данными вручную. Трансформация данных вызывается в Airflow, но рассчитывается на стороне PostgreSQL.

Также созданы 2 аналитические витрины на основе VIEW, настроен “пайплайн” для их пересоздания в Airflow.

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from pymongo import MongoClient
import psycopg2
import json
from datetime import datetime

MONGO_URI = "mongodb://mongodb:27017/"
MONGO_DB = "local"
POSTGRES_CONN = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "q1q1q1q1",
    "host": "my-postgres"
}

TABLE_MAPPING = {
    "UserSessions": {
        "fields": {
            "session_id": "json_data->>'session_id'",
            "user_id": "json_data->>'user_id'",
            "start_time": "(json_data->>'start_time')::TIMESTAMP",
            "end_time": "(json_data->>'end_time')::TIMESTAMP",
            "pages_visited": "json_data->'pages_visited'",
            "device": "json_data->'device'",
            "actions": "json_data->'actions'"
        }
    },
    "ProductPriceHistory": {
        "fields": {
            "product_id": "json_data->>'product_id'",
            "price_changes": "json_data->'price_changes'",
            "current_price": "(json_data->>'current_price')::NUMERIC",
            "currency": "json_data->>'currency'"
        }
    },
    "EventLogs": {
        "fields": {
            "event_id": "json_data->>'event_id'",
            "timestamp": "(json_data->>'timestamp')::TIMESTAMP",
            "event_type": "json_data->>'event_type'",
            "details": "json_data->'details'"
        }
    },
    "SupportTickets": {
        "fields": {
            "ticket_id": "json_data->>'ticket_id'",
            "user_id": "json_data->>'user_id'",
            "status": "json_data->>'status'",
            "issue_type": "json_data->>'issue_type'",
            "messages": "json_data->'messages'",
            "created_at": "(json_data->>'created_at')::TIMESTAMP",
            "updated_at": "(json_data->>'updated_at')::TIMESTAMP"
        }
    },
    "UserRecommendations": {
        "fields": {
            "user_id": "json_data->>'user_id'",
            "recommended_products": "json_data->'recommended_products'",
            "last_updated": "(json_data->>'last_updated')::TIMESTAMP"
        }
    },
    "ModerationQueue": {
        "fields": {
            "review_id": "json_data->>'review_id'",
            "user_id": "json_data->>'user_id'",
            "product_id": "json_data->>'product_id'",
            "review_text": "json_data->>'review_text'",
            "rating": "(json_data->>'rating')::INTEGER",
            "moderation_status": "json_data->>'moderation_status'",
            "flags": "json_data->'flags'",
            "submitted_at": "(json_data->>'submitted_at')::TIMESTAMP"
        }
    },
    "SearchQueries": {
        "fields": {
            "query_id": "json_data->>'query_id'",
            "user_id": "json_data->>'user_id'",
            "query_text": "json_data->>'query_text'",
            "timestamp": "(json_data->>'timestamp')::TIMESTAMP",
            "filters": "json_data->'filters'",
            "results_count": "(json_data->>'results_count')::INTEGER"
        }
    }
}

COLLECTIONS = list(TABLE_MAPPING.keys())

def replicate_collection(collection_name):
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    mongo_collection = mongo_db[collection_name]

    pg_conn = psycopg2.connect(**POSTGRES_CONN)
    pg_cursor = pg_conn.cursor()

    pg_cursor.execute(f"TRUNCATE TABLE nosql.{collection_name}")

    query = f"""
        INSERT INTO nosql.{collection_name}_tmp (json_data)
        VALUES (%s)
    """

    for doc in mongo_collection.find():
        json_data = json.dumps(doc, default=str)
        pg_cursor.execute(query, (json_data,))

    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()
    mongo_client.close()
    return 1

def get_data_from_json():
    pg_conn = psycopg2.connect(**POSTGRES_CONN)
    pg_cursor = pg_conn.cursor()


    query = f"""
            INSERT INTO nosql.UserSessions (json_data, session_id, user_id, start_time, end_time, pages_visited, device, actions)
            SELECT json_data,
                json_data->>'session_id',
                json_data->>'user_id',
                (json_data->>'start_time')::TIMESTAMP,
                (json_data->>'end_time')::TIMESTAMP,
                json_data->'pages_visited',
                json_data->'device',
                json_data->'actions'
            FROM nosql.UserSessions_tmp;

            INSERT INTO nosql.ProductPriceHistory (json_data, product_id, price_changes, current_price, currency)
            SELECT json_data,
                json_data->>'product_id',
                json_data->'price_changes',
                (json_data->>'current_price')::NUMERIC,
                json_data->>'currency'
            FROM nosql.ProductPriceHistory_tmp;


            INSERT INTO nosql.EventLogs (json_data, event_id, timestamp, event_type, details)
            SELECT json_data,
                json_data->>'event_id',
                (json_data->>'timestamp')::TIMESTAMP,
                json_data->>'event_type',
                json_data->'details'
            FROM nosql.EventLogs_tmp;


            INSERT INTO nosql.SupportTickets (json_data, ticket_id, user_id, status, issue_type, messages, created_at, updated_at)
            SELECT json_data,
                json_data->>'ticket_id',
                json_data->>'user_id',
                json_data->>'status',
                json_data->>'issue_type',
                json_data->'messages',
                (json_data->>'created_at')::TIMESTAMP,
                (json_data->>'updated_at')::TIMESTAMP
            FROM nosql.SupportTickets_tmp;


            INSERT INTO nosql.UserRecommendations (json_data, user_id, recommended_products, last_updated)
            SELECT json_data,
                json_data->>'user_id',
                json_data->'recommended_products',
                (json_data->>'last_updated')::TIMESTAMP
            FROM nosql.UserRecommendations_tmp;



            INSERT INTO nosql.ModerationQueue (json_data, review_id, user_id, product_id, review_text, rating, moderation_status, flags, submitted_at)
            SELECT json_data,
                json_data->>'review_id',
                json_data->>'user_id',
                json_data->>'product_id',
                json_data->>'review_text',
                (json_data->>'rating')::INTEGER,
                json_data->>'moderation_status',
                json_data->'flags',
                (json_data->>'submitted_at')::TIMESTAMP
            FROM nosql.ModerationQueue_tmp;



            INSERT INTO nosql.SearchQueries (json_data, query_id, user_id, query_text, timestamp, filters, results_count)
            SELECT json_data,
                json_data->>'query_id',
                json_data->>'user_id',
                json_data->>'query_text',
                (json_data->>'timestamp')::TIMESTAMP,
                json_data->'filters',
                (json_data->>'results_count')::INTEGER
            FROM nosql.SearchQueries_tmp;
    """
        
    pg_cursor.execute(query)
    print(f"Data has been moved.")

    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()


def recreate_analytics():
    pg_conn = psycopg2.connect(**POSTGRES_CONN)
    pg_cursor = pg_conn.cursor()

    query = f"""
        --Витрина: Статистика пользовательских сессий
        DROP MATERIALIZED VIEW nosql.session_analytics;
        CREATE MATERIALIZED VIEW nosql.session_analytics AS
        SELECT 
            user_id,
            COUNT(*) AS total_sessions,
            AVG(EXTRACT(EPOCH FROM (end_time - start_time)))::INT AS avg_duration_seconds,
            AVG(jsonb_array_length(pages_visited))::NUMERIC(5,1) AS avg_pages_visited,
            MAX(end_time) AS last_session_date
        FROM nosql.UserSessions
        GROUP BY user_id;


        --Витрина: Анализ ценовой истории товаров
        DROP MATERIALIZED VIEW nosql.price_analytics;
        CREATE MATERIALIZED VIEW nosql.price_analytics AS
        SELECT 
            product_id,
            currency,
            COUNT(*) AS price_changes_count,
            MIN(current_price) AS min_price,
            MAX(current_price) AS max_price,
            AVG(current_price)::NUMERIC(10,2) AS avg_price,
            jsonb_array_length(price_changes) AS history_length
        FROM nosql.ProductPriceHistory
        GROUP BY product_id, currency, jsonb_array_length(price_changes);
    """
    
    pg_cursor.execute(query)

    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

dag = DAG(
    "mongo_to_postgres_replication",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

task_2 = PythonOperator(
        task_id=f"get_data_from_json",
        python_callable=get_data_from_json,
        dag=dag,
    )
    
    
task_3 = PythonOperator(
        task_id=f"recreate_analytics",
        python_callable=recreate_analytics,
        dag=dag,
    )

tasks = []
for collection in COLLECTIONS:
    task = PythonOperator(
        task_id=f"replicate_{collection}",
        python_callable=replicate_collection,
        op_kwargs={"collection_name": collection},
        dag=dag,
    )
    tasks.append(task)


for i in range(len(tasks) - 1):
    tasks[i] >> tasks[i + 1]   
tasks[-1] >> task_2 >> task_3



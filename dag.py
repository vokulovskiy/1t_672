from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def generate_pascals_triangle(levels):
    triangle = []
    for i in range(levels):
        row = [1]  # Начинаем каждую строку с 1
        if triangle:  # Если это не первая строка
            last_row = triangle[-1]
            row.extend([last_row[j] + last_row[j + 1] for j in range(len(last_row) - 1)])
            row.append(1)  # Завершаем строку 1
        triangle.append(row)
    return triangle

def print_pascals_triangle(triangle):
    max_width = len(" ".join(map(str, triangle[-1])))
    for row in triangle:
        row_str = " ".join(map(str, row))
        print(row_str.center(max_width))

def my_function():
    # Генерация и вывод
    levels = 10
    triangle = generate_pascals_triangle(levels)
    print_pascals_triangle(triangle)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 19),
}

dag = DAG('python_operator_dag', default_args=default_args, schedule_interval='@daily')

python_task = PythonOperator(
    task_id='python_task',
    python_callable=my_function,
    dag=dag,
)

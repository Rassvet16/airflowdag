import argparse

from airflow.sensors.external_task_sensor import ExternalTaskSensor


from sqlalchemy import create_engine
import pandas as pd
if __name__ == '__main__':
    parser = argparse.ArgumentParser("Описание")
    parser.add_argument('host', type=str)
    parser.add_argument('user', type=str)
    parser.add_argument('password', type=str)
    parser.add_argument('port', type=int)
    args = parser.parse_args()

    host, user, password, port = args.host, args.user, args.password, args.port

    print(args.host, args.user, args.password, args.port)

    sql = """
        select 
            *
        from titanic.titanic_table
    """

    # Создание объекта engine с использованием SQLAlchemy
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/postgres')

    df = pd.read_sql(sql, engine)

    # Сохранение данных в Excel
    excel_file_path = 'table.xlsx'
    df.to_excel(excel_file_path, index=False)
    print(df.info())
    print('Первый даг закончил свою работу')


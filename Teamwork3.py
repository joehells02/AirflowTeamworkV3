from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import sqlite3
from kaggle.api.kaggle_api_extended import KaggleApi

# DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id='weather_etl_pipeline3',
    default_args=default_args,
    description='ETL pipeline for processing historical weather data',
    schedule_interval=None,
    start_date=datetime(2024, 11, 20),
    catchup=False,
) as dag:

    def extract_data(**kwargs):
        """Extract weather data using Kaggle API."""
        api = KaggleApi()
        api.authenticate()

        # Kaggle Dataset
        dataset = 'muthuj7/weather-dataset'
        download_path = '/home/hells/airflow/datasets'
        api.dataset_download_files(dataset, path=download_path, unzip=True)
        
        file_path = os.path.join(download_path, 'weatherHistory.csv')
        kwargs['ti'].xcom_push(key='file_path', value=file_path)

    def transform_data(**kwargs):
        """Transform weather data by aggregating daily and monthly averages."""
        try:
            ti = kwargs['ti']

            # Fetch file path from XCom
            file_path = ti.xcom_pull(task_ids='extract_data', key='file_path')
            if not file_path:
                raise ValueError("File path not found in XCom")

            # Load data
            df = pd.read_csv(file_path)

            # Convert to datetime and handle time zones
            df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], utc=True)

            # Set index for resampling
            df.set_index('Formatted Date', inplace=True)

            # Separate numeric and non-numeric columns
            numeric_cols = df.select_dtypes(include=['number']).columns
            non_numeric_cols = df.select_dtypes(exclude=['number']).columns

            # Resample numeric columns for daily mean
            daily_numeric = df[numeric_cols].resample('D').mean()

            # Handle non-numeric columns separately for daily data
            daily_non_numeric = df[non_numeric_cols].resample('D').agg(
                lambda x: x.mode().iloc[0] if not x.mode().empty else None
            )

            # Combine numeric and non-numeric data for daily
            daily_df = pd.concat([daily_numeric, daily_non_numeric], axis=1)

            # Resample and calculate monthly aggregates
            monthly_numeric = df[numeric_cols].resample('M').mean()
            monthly_non_numeric = df[non_numeric_cols].resample('M').agg(
                lambda x: x.mode().iloc[0] if not x.mode().empty else None
            )
            monthly_df = pd.concat([monthly_numeric, monthly_non_numeric], axis=1)

            # Save transformed data
            daily_path = '/home/hells/airflow/datasets/daily_weather.csv'
            monthly_path = '/home/hells/airflow/datasets/monthly_weather.csv'

            daily_df.to_csv(daily_path)
            monthly_df.to_csv(monthly_path)

            # Push paths to XCom for downstream tasks
            ti.xcom_push(key='daily_path', value=daily_path)
            ti.xcom_push(key='monthly_path', value=monthly_path)

        except Exception as e:
            raise RuntimeError(f"Transform data failed: {e}")

    def validate_data(**kwargs):
        """Validate transformed data."""
        ti = kwargs['ti']

        # Pull file paths from XCom
        daily_path = ti.xcom_pull(key='daily_path', task_ids='transform_data')
        monthly_path = ti.xcom_pull(key='monthly_path', task_ids='transform_data')

        # Load data
        daily_df = pd.read_csv(daily_path)
        monthly_df = pd.read_csv(monthly_path)

        # Check for missing values in daily data
        missing_daily_values = daily_df.isnull().sum()
        if missing_daily_values.sum() > 0:
            print("Missing values in daily data:")
            print(missing_daily_values[missing_daily_values > 0])  # Log columns with missing values
            daily_df = daily_df.dropna()  # Drop rows with missing values

        # Check for missing values in monthly data
        missing_monthly_values = monthly_df.isnull().sum()
        if missing_monthly_values.sum() > 0:
            print("Missing values in monthly data:")
            print(missing_monthly_values[missing_monthly_values > 0])  # Log columns with missing values
            monthly_df = monthly_df.dropna()  # Drop rows with missing values

        # Ensure no missing values after handling them
        assert daily_df.isnull().sum().sum() == 0, "Missing values in daily data after handling"
        assert monthly_df.isnull().sum().sum() == 0, "Missing values in monthly data after handling"

    def load_data(**kwargs):
        """Load data into SQLite database with table creation."""
        ti = kwargs['ti']

        # Pull file paths from XCom
        daily_path = ti.xcom_pull(key='daily_path', task_ids='transform_data')
        monthly_path = ti.xcom_pull(key='monthly_path', task_ids='transform_data')

        conn = sqlite3.connect('/home/hells/airflow/databases/weather.db')
        cursor = conn.cursor()

        # Create daily_weather table if it does not exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            formatted_date TEXT,
            avg_temperature_c REAL,
            avg_apparent_temperature_c REAL,
            avg_humidity REAL,
            avg_wind_speed_kmh REAL,
            avg_visibility_km REAL,
            avg_pressure_millibars REAL
        )
        ''')

        # Create monthly_weather table if it does not exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS monthly_weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            month TEXT,
            avg_temperature_c REAL,
            avg_apparent_temperature_c REAL,
            avg_humidity REAL,
            avg_visibility_km REAL,
            avg_pressure_millibars REAL,
            mode_precip_type TEXT
        )
        ''')

        # Load data into DataFrames
        daily_df = pd.read_csv(daily_path)
        monthly_df = pd.read_csv(monthly_path)

        # Handle daily data: ensure correct schema and add 'formatted_date'
        daily_df['Formatted Date'] = pd.to_datetime(daily_df['Formatted Date'], utc=True)
        daily_df['formatted_date'] = daily_df['Formatted Date'].dt.strftime('%Y-%m-%d')
        daily_df.drop(columns=['Formatted Date'], inplace=True)

        daily_df = daily_df.rename(columns={
            'Temperature (C)': 'avg_temperature_c',
            'Apparent Temperature (C)': 'avg_apparent_temperature_c',
            'Humidity': 'avg_humidity',
            'Wind Speed (km/h)': 'avg_wind_speed_kmh',
            'Visibility (km)': 'avg_visibility_km',
            'Pressure (millibars)': 'avg_pressure_millibars',
        })

        # Handle monthly data: ensure correct schema
        monthly_df = monthly_df.rename(columns={
            'month': 'month',
            'Temperature (C)': 'avg_temperature_c',
            'Apparent Temperature (C)': 'avg_apparent_temperature_c',
            'Humidity': 'avg_humidity',
            'Visibility (km)': 'avg_visibility_km',
            'Pressure (millibars)': 'avg_pressure_millibars',
            'Precip Type': 'mode_precip_type'
        })

        cursor.execute("PRAGMA table_info(daily_weather);")
        daily_columns = [col[1] for col in cursor.fetchall()]
        for col in daily_df.columns:
            if col not in daily_columns:
                # Quote column name to handle spaces or special characters
                safe_col = f'"{col}"'
                cursor.execute(f"ALTER TABLE daily_weather ADD COLUMN {safe_col} TEXT")

        cursor.execute("PRAGMA table_info(monthly_weather);")
        monthly_columns = [col[1] for col in cursor.fetchall()]
        for col in monthly_df.columns:
            if col not in monthly_columns:
                # Quote column name to handle spaces or special characters
                safe_col = f'"{col}"'
                cursor.execute(f"ALTER TABLE monthly_weather ADD COLUMN {safe_col} TEXT")


        # Insert data into SQLite tables
        daily_df.to_sql('daily_weather', conn, if_exists='append', index=False)
        monthly_df.to_sql('monthly_weather', conn, if_exists='append', index=False)

        conn.commit()
        conn.close()

    # Define tasks
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )

    # Task pipeline
    extract_task >> transform_task >> validate_task >> load_task

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  4 11:35:39 2025

@author: dinglin
"""

import boto3
import csv
import os
import random
import pandas as pd
from io import StringIO
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# AWS S3 Configuration
S3_BUCKET = os.getenv("amde300", "amde300")  # Replace with your actual S3 bucket
S3_PREFIX = "airflow-data"  # Folder within S3 bucket
MY_KEY_ID = 'ASIAYAAO5HRMG2VQK62N'
MY_ACCESS_KEY = '9ccDgbUNavZKKBzxauQsg1/lFyiA2GYCT3KScIZG'
MY_SESSION_TOKEN = 'IQoJb3JpZ2luX2VjEDYaCXVzLWVhc3QtMiJHMEUCIAkUDAoCLKJXH+Pau8VyCilNUlHHEXyNBsCVDQcJjnheAiEAjYsW+IDtOpsDgtfJ03EEKnI66XxhOFovMuoaptAXJtYq6wIIfxAAGgw1NDk3ODcwOTAwMDgiDMpEfPTp0DiDvgD2pirIAndzaTvX//B+UTfPilU+5V8Q80ommjKtZ7n5OsriSzUgaTud2VrmVO+NC7u8vs6gGXKqJ5eiOxSkXCtpXkYh7ZQS6yI8VldGYIg4Fy5o510a1f5LW5ec4ocSLvE3K4VoxeOsmuT+sW9oOyVQPFTsaLP/Crxm/2BO1ZRw5bvd/p8qUm8zHIzaOjVZVDqrthHpFj/fG/mamYpTohqOAel7bGHBEwaQnHbQoHC03KpRfTJ65oWBquPqJYGtldIaqB6y+E/9XE+TpMHuLrwLIunS2ufp77U8Iuenl/yNMtiXVXMav5HRPCWErV5w4NBHHEKc5jDZAq9Z8nsUty4pNb5NAdcqszByznEJshe3/1OhkK/wQxANm0ZRJo2L4ngO4R6wmDWI0xcEJPDeralDPqik2PFC22BlK/TMpLHHL5QCQM8MZRLI1OuxEaMw6Je4vgY6pwF68bE1hmdc23dHd5OhRkDAfnr1N0aJYiffZytOwiAGHdFAlaKv8d/sbcTmVC+IVDEKZnYjiDVVUIa5sBOd99zb16YbEdrhDMyuXhcS9q1GLjPMmrB8x+1pTEI5878S70baBX/QmnZXVkOhxHLnFIhvdgdRhIf7oU16dLCiRX7Lhist/4/2JRl065oZIrFASGb7GD0T7grcR+/Loxgdiog5EWrtITOvFQ=='

s3_client = boto3.client('s3',
                         aws_access_key_id=MY_KEY_ID,
                         aws_secret_access_key=MY_ACCESS_KEY,
                         aws_session_token=MY_SESSION_TOKEN)

# Helper function to upload CSV to S3
def upload_to_s3(data, filename):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}/{filename}", Body=csv_buffer.getvalue())

# Helper function to download CSV from S3
def download_from_s3(filename):
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}/{filename}")
    return pd.read_csv(obj['Body'])

# DAG Definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 1),
    'retries': 1,
}

dag = DAG(
    'ml_pipeline_s3',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# Install pandas
install_pandas = BashOperator(
    task_id="install_pandas",
    bash_command="pip install pandas boto3 scikit-learn",
    dag=dag,
)

# Task: Generate and upload random data to S3
def generate_data():
    df = pd.DataFrame({
        'feature1': [random.randint(0, 100) for _ in range(100)],
        'feature2': [random.randint(0, 100) for _ in range(100)],
        'label': [random.choice([0, 1]) for _ in range(100)]
    })
    upload_to_s3(df, "data.csv")

generate_task = PythonOperator(
    task_id="generate_data",
    python_callable=generate_data,
    dag=dag,
)

# Task: Read data from S3 and clean
def clean_impute():
    df = download_from_s3("data.csv")
    df.fillna(df.mean(), inplace=True)
    upload_to_s3(df, "cleaned_data.csv")

clean_task = PythonOperator(
    task_id="clean_impute",
    python_callable=clean_impute,
    dag=dag,
)

# Task: Perform feature engineering
def feature_engineering():
    df = download_from_s3("cleaned_data.csv")
    df['new_feature'] = df['feature1'] * 0.5
    upload_to_s3(df, "fe_data.csv")

feature_task = PythonOperator(
    task_id="feature_engineering",
    python_callable=feature_engineering,
    dag=dag,
)

# Task: Train SVM Model
def train_svm():
    from sklearn.svm import SVC
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score

    df = download_from_s3("fe_data.csv")
    X = df[['feature1', 'new_feature']]
    y = df['label']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
    model = SVC()
    model.fit(X_train, y_train)
    
    acc = accuracy_score(y_test, model.predict(X_test))
    print(f"SVM Accuracy: {acc}")

svm_task = PythonOperator(
    task_id="train_svm",
    python_callable=train_svm,
    dag=dag,
)

# Task: Train Logistic Regression Model
def train_logistic():
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score

    df = download_from_s3("fe_data.csv")
    X = df[['feature1', 'new_feature']]
    y = df['label']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
    model = LogisticRegression()
    model.fit(X_train, y_train)
    
    acc = accuracy_score(y_test, model.predict(X_test))
    print(f"Logistic Regression Accuracy: {acc}")

logistic_task = PythonOperator(
    task_id="train_logistic",
    python_callable=train_logistic,
    dag=dag,
)

# Task: Merge Model Results
def merge_results():
    print("Merging model results and selecting the best model.")

merge_task = PythonOperator(
    task_id="merge_results",
    python_callable=merge_results,
    dag=dag,
)

# Task: Final Evaluation
def evaluate_test():
    print("Evaluating final model performance.")

evaluate_task = PythonOperator(
    task_id="evaluate_test",
    python_callable=evaluate_test,
    dag=dag,
)

# DAG dependencies
install_pandas >> generate_task  # Install dependencies before starting
generate_task >> clean_task  # Data Cleaning
clean_task >> feature_task  # Feature Engineering
feature_task >> [svm_task, logistic_task]  # Train models in parallel
[svm_task, logistic_task] >> merge_task  # Merge results
merge_task >> evaluate_task  # Final evaluation

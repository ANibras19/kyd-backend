# datadive_routes.py
@datadive.route('/upload-dataset', methods=['POST'])

from flask import Blueprint, request, jsonify
import pandas as pd
import mysql.connector
import joblib
import os
import io
from datetime import datetime
import numpy as np

datadive = Blueprint('datadive', __name__)

# Load models only once
model_strength = joblib.load("ml_models/model_dataset_strength.pkl")
model_action = joblib.load("ml_models/model_dataset_action.pkl")

# Replace with your actual DB config
db_config = {
    'host': 'enqhzd10cxh7hv2e.cbetxkdyhwsb.us-east-1.rds.amazonaws.com',
    'user': 'v4jbqslxdkfz0ox0',
    'password': 'fxawiuzv6nu61c70',
    'database': 'rnqxqwaljdwgx3un'
}

def extract_features(df):
    df = df.head(150)
    num_rows = df.shape[0]
    num_cols = df.shape[1]
    num_missing = df.isnull().sum().sum()
    missing_ratio = num_missing / (num_rows * num_cols) if num_rows * num_cols > 0 else 0

    num_numeric = len(df.select_dtypes(include=np.number).columns)
    num_categorical = len(df.select_dtypes(include='object').columns)
    avg_unique_per_column = np.mean([df[col].nunique() for col in df.columns]) if num_cols > 0 else 0
    imbalance = np.std([
        df[col].value_counts(normalize=True).max() if df[col].dtype == 'object' else 0
        for col in df.columns
    ])

    return [[
        num_rows,
        num_cols,
        missing_ratio,
        num_numeric,
        num_categorical,
        avg_unique_per_column,
        imbalance
    ]]

@datadive.route('/upload-dataset', methods=['POST'])
def upload_dataset():
    try:
        uploaded_file = request.files['file']
        email = request.form.get('email')
        plan = request.form.get('plan')

        if not uploaded_file or not email or not plan:
            return jsonify({'error': 'Missing file, email, or plan'}), 400

        filename = uploaded_file.filename
        ext = filename.split('.')[-1].lower()

        if ext not in ['csv', 'xlsx']:
            return jsonify({'error': 'Only CSV or XLSX files supported'}), 400

        # Read file into pandas
        if ext == 'csv':
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)

        features = extract_features(df)
        is_strong = bool(model_strength.predict(features)[0])
        suggested_action = model_action.predict(features)[0]

        # Only log to DB if Basic/Advanced
        if plan.lower() in ['basic', 'advanced']:
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor()

            table_name = f"{email.split('@')[0]}_uploads"
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    filename VARCHAR(255),
                    upload_time DATETIME,
                    row_count INT,
                    col_count INT,
                    dataset LONGBLOB
                )
            """)

            # Convert file to bytes for BLOB
            uploaded_file.seek(0)
            file_blob = uploaded_file.read()

            cursor.execute(f"""
                INSERT INTO `{table_name}` (filename, upload_time, row_count, col_count, dataset)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                filename,
                datetime.now(),
                df.shape[0],
                df.shape[1],
                file_blob
            ))

            conn.commit()
            cursor.close()
            conn.close()

        return jsonify({
            'is_strong_dataset': is_strong,
            'suggested_action': suggested_action,
            'row_count': df.shape[0],
            'col_count': df.shape[1]
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

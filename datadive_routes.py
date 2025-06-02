from flask import Blueprint, request, jsonify
import pandas as pd
import numpy as np
import mysql.connector
from datetime import datetime

datadive = Blueprint('datadive', __name__)

# DB config
db_config = {
    'host': 'enqhzd10cxh7hv2e.cbetxkdyhwsb.us-east-1.rds.amazonaws.com',
    'user': 'v4jbqslxdkfz0ox0',
    'password': 'fxawiuzv6nu61c70',
    'database': 'rnqxqwaljdwgx3un'
}

@datadive.route('/upload-dataset', methods=['POST'])
def upload_dataset():
    try:
        uploaded_file = request.files['file']
        username = request.form.get('username')  # must be sent from frontend
        if not uploaded_file or not username:
            return jsonify({'error': 'Missing file or username'}), 400

        filename = uploaded_file.filename
        ext = filename.split('.')[-1].lower()

        if ext not in ['csv', 'xlsx']:
            return jsonify({'error': 'Only CSV or XLSX files are supported'}), 400

        # Read the file
        if ext == 'csv':
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)

        row_count = df.shape[0]
        col_count = df.shape[1]
        column_headers = ', '.join(df.columns.tolist())

        # Column profiling
        column_details = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            entry = {
                'column_name': col,
                'data_type': dtype
            }

            if dtype in ['object', 'category']:
                unique_vals = df[col].dropna().unique().tolist()
                entry['type'] = 'categorical'
                entry['num_unique'] = len(unique_vals)
                entry['unique_values'] = unique_vals
            elif np.issubdtype(df[col].dtype, np.number):
                series = df[col].dropna()
                entry['type'] = 'numerical'
                entry['count'] = int(series.count())
                entry['mean'] = series.mean()
                entry['median'] = series.median()
                entry['mode'] = series.mode().tolist()
                entry['min'] = series.min()
                entry['max'] = series.max()
                entry['std'] = series.std()
                entry['sum'] = series.sum()
            else:
                entry['type'] = 'other'

            column_details.append(entry)

        # Save to DB as BLOB
        uploaded_file.seek(0)
        file_blob = uploaded_file.read()
        table_name = f"{username}_uploads"

        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255),
                upload_time DATETIME,
                row_count INT,
                col_count INT,
                column_headers TEXT,
                dataset LONGBLOB
            )
        """)

        cursor.execute(f"""
            INSERT INTO `{table_name}` (filename, upload_time, row_count, col_count, column_headers, dataset)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            filename,
            datetime.now(),
            row_count,
            col_count,
            column_headers,
            file_blob
        ))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({
            'row_count': row_count,
            'col_count': col_count,
            'column_headers': df.columns.tolist(),
            'columns': column_details
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

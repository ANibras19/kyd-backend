from flask import Blueprint, request, jsonify
from flask_cors import cross_origin
from werkzeug.utils import secure_filename
import os
import pandas as pd
import traceback
from sqlalchemy import text
from app import db  # Assuming `db` is SQLAlchemy() instance from app.py

UPLOAD_FOLDER = './uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

latest_uploaded_file = None

datadive = Blueprint('datadive', __name__)

@datadive.route('/upload', methods=['POST'])
@cross_origin()
def upload_file():
    global latest_uploaded_file
    try:
        print("⚠️ Upload route hit")

        username = request.form.get('username')
        if not username:
            print("❌ Username not provided")
            return jsonify({"error": "username required"}), 400

        file = request.files.get('file')
        if not file:
            print("❌ No file found in request")
            return jsonify({"error": "No file uploaded"}), 400

        filename = secure_filename(file.filename)
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        file.save(filepath)
        latest_uploaded_file = filepath

        print(f"✅ File saved at: {filepath}")

        # Create per-user uploads table if not exists
        uploads_tbl = f"{username}_uploads"
        ddl = text(f"""
            CREATE TABLE IF NOT EXISTS `{uploads_tbl}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255) NOT NULL,
                upload_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_data LONGBLOB
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        db.session.execute(ddl)

        with open(filepath, 'rb') as f:
            blob = f.read()
        insert = text(f"""
            INSERT INTO `{uploads_tbl}` (filename, file_data)
            VALUES (:fn, :blob)
        """)
        db.session.execute(insert, {'fn': filename, 'blob': blob})
        db.session.commit()
        print(f"✅ File inserted into table {uploads_tbl}")

        # Read and profile dataset
        try:
            if filename.lower().endswith('.csv'):
                df = pd.read_csv(filepath, nrows=5)
                full_df = pd.read_csv(filepath)
            elif filename.lower().endswith(('.xls', '.xlsx')):
                df = pd.read_excel(filepath, nrows=5)
                full_df = pd.read_excel(filepath)
            else:
                print("❌ Unsupported file type")
                return jsonify({"error": "Unsupported file type"}), 400
        except Exception as e:
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500

        metadata = []
        groups_dict = {}
        for col in df.columns:
            series = df[col]
            full_series = full_df[col]
            if pd.api.types.is_datetime64_any_dtype(series):
                col_type = "datetime"
            elif pd.api.types.is_numeric_dtype(series):
                col_type = "numeric"
            elif series.nunique() / len(series) < 0.05:
                col_type = "categorical"
            else:
                col_type = "text"

            sem = None
            name_lower = col.lower()
            if "date" in name_lower or "time" in name_lower:
                sem = "date"
            elif "id" in name_lower:
                sem = "identifier"

            entry = {
                "name": col,
                "type": col_type,
                "semantic": sem
            }

            if col_type in ("categorical", "text"):
                unique_vals = full_series.dropna().astype(str).unique().tolist()
                groups_dict[col] = unique_vals

            metadata.append(entry)

        print("✅ Metadata profiling complete")
        return jsonify({
            "columns": df.columns.tolist(),
            "column_metadata": metadata,
            "filename": filename,
            "groups": groups_dict
        }), 200

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

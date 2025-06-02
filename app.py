from flask import Flask, request, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from flask_cors import CORS
import mysql.connector
import os
import pandas as pd

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.after_request
def add_cors_headers(response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type,Authorization")
    response.headers.add("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
    return response

# MySQL config
db_config = {
    'host': 'enqhzd10cxh7hv2e.cbetxkdyhwsb.us-east-1.rds.amazonaws.com',
    'user': 'v4jbqslxdkfz0ox0',
    'password': 'fxawiuzv6nu61c70',
    'database': 'rnqxqwaljdwgx3un'
}

@app.route('/')
def home():
    return "Backend is running."

@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    username = data.get('username')
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    email = data.get('email')
    raw_password = data.get('password')
    password = generate_password_hash(raw_password)

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO users (username, first_name, last_name, email, password) VALUES (%s, %s, %s, %s, %s)",
            (username, first_name, last_name, email, password)
        )
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"message": "User registered successfully"}), 201
    except mysql.connector.IntegrityError:
        return jsonify({"error": "Username already exists"}), 409
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username_or_email = data.get('username')
    password = data.get('password')

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM users WHERE username = %s OR email = %s", (username_or_email, username_or_email))
        user = cursor.fetchone()
        cursor.close()
        conn.close()

        if user and check_password_hash(user['password'], password):
            return jsonify({
                "message": "Login successful",
                "plan": user['plan'],
                "first_name": user["first_name"],
                "last_name": user["last_name"],
                "email": user["email"]
            }), 200

        return jsonify({"error": "Invalid username/email or password"}), 401
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/update-plan', methods=['POST'])
def update_plan():
    data = request.get_json()
    email = data.get('email')
    plan = data.get('plan')

    if not email or not plan:
        return jsonify({"error": "Missing email or plan"}), 400

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET plan = %s WHERE email = %s", (plan, email))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"message": f"Plan updated to {plan}"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

from flask import request, jsonify
from flask_cors import cross_origin
from werkzeug.utils import secure_filename
from sqlalchemy import text
import pandas as pd
import io

@app.route('/upload', methods=['POST'])
@cross_origin()
def upload_file():
    try:
        # ─── Validate Input ────────────────────────────────────────────────
        username = request.form.get('username')
        if not username:
            return jsonify({"error": "username required"}), 400

        if 'file' not in request.files:
            return jsonify({"error": "No file uploaded"}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No selected file"}), 400

        filename = secure_filename(file.filename)
        file_stream = io.BytesIO(file.read())

        # ─── Store File in DB ───────────────────────────────────────────────
        uploads_tbl = f"{username}_uploads"

        db.session.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{uploads_tbl}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255) NOT NULL,
                upload_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_data LONGBLOB
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))

        db.session.execute(
            text(f"""
                INSERT INTO `{uploads_tbl}` (filename, file_data)
                VALUES (:fn, :blob)
            """), {'fn': filename, 'blob': file_stream.getvalue()}
        )
        db.session.commit()

        # ─── Read File to Extract Columns and Metadata ─────────────────────
        file_stream.seek(0)
        sample_df = pd.read_csv(file_stream, nrows=5) if filename.lower().endswith('.csv') \
            else pd.read_excel(file_stream, nrows=5)

        file_stream.seek(0)
        full_df = pd.read_csv(file_stream) if filename.lower().endswith('.csv') \
            else pd.read_excel(file_stream)

        metadata = []
        groups_dict = {}

        for col in sample_df.columns:
            series = sample_df[col]
            full_series = full_df[col]

            if pd.api.types.is_datetime64_any_dtype(series):
                col_type = "datetime"
            elif pd.api.types.is_numeric_dtype(series):
                col_type = "numeric"
            elif series.nunique() / len(series) < 0.05:
                col_type = "categorical"
            else:
                col_type = "text"

            semantic = None
            name_lower = col.lower()
            if "date" in name_lower or "time" in name_lower:
                semantic = "date"
            elif "id" in name_lower:
                semantic = "identifier"

            metadata.append({
                "name": col,
                "type": col_type,
                "semantic": semantic
            })

            if col_type in ("categorical", "text"):
                groups_dict[col] = full_series.dropna().astype(str).unique().tolist()

        return jsonify({
            "columns": sample_df.columns.tolist(),
            "column_metadata": metadata,
            "filename": filename,
            "groups": groups_dict
        }), 200

    except UnicodeDecodeError:
        file_stream.seek(0)
        df = pd.read_csv(file_stream, nrows=5, encoding='latin1')
        return jsonify({"columns": df.columns.tolist(), "note": "Read using latin1 encoding"}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Processing failed: {str(e)}"}), 500

if __name__ == '__main__':
    from waitress import serve
    serve(app, host='0.0.0.0', port=5000)

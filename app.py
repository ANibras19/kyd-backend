from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from sqlalchemy import create_engine, text
import pandas as pd
import mysql.connector
import os
import traceback

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

UPLOAD_FOLDER = "uploads"
latest_uploaded_file = None

# MySQL connection details
db_config = {
    'host': 'enqhzd10cxh7hv2e.cbetxkdyhwsb.us-east-1.rds.amazonaws.com',
    'user': 'v4jbqslxdkfz0ox0',
    'password': 'fxawiuzv6nu61c70',
    'database': 'rnqxqwaljdwgx3un'
}

db_url = f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
db = create_engine(db_url)

@app.route('/')
def home():
    return "Backend is connected to JawsDB!"

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
        query = """
            INSERT INTO users (username, first_name, last_name, email, password)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query, (username, first_name, last_name, email, password))
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

    print("Login attempt with:", username_or_email)

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT * FROM users
            WHERE username = %s OR email = %s
        """
        cursor.execute(query, (username_or_email, username_or_email))
        user = cursor.fetchone()
        cursor.close()
        conn.close()

        print("User fetched from DB:", user)

        if user:
            print("Stored hash:", user['password'])
            password_match = check_password_hash(user['password'], password)
            print("Password match result:", password_match)

            if password_match:
                return jsonify({
                    "message": "Login successful",
                    "plan": user['plan'],
                    "first_name": user["first_name"],
                    "last_name": user["last_name"],
                    "email": user["email"]
                }), 200

        return jsonify({"error": "Invalid username/email or password"}), 401

    except Exception as e:
        print("Login error:", str(e))
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
        query = "UPDATE users SET plan = %s WHERE email = %s"
        cursor.execute(query, (plan, email))
        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({"message": f"Plan updated to {plan}"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/upload', methods=['POST'])
def upload_file():
    global latest_uploaded_file

    username = request.form.get('username')
    if not username:
        return jsonify({"error": "username required"}), 400

    file = request.files.get('file')
    if not file:
        return jsonify({"error": "No file uploaded"}), 400

    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    filename = secure_filename(file.filename)
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    file.save(filepath)

    latest_uploaded_file = filepath

    uploads_tbl = f"{username}_uploads"
    ddl = text(f"""
        CREATE TABLE IF NOT EXISTS `{uploads_tbl}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            filename VARCHAR(255) NOT NULL,
            upload_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            file_data LONGBLOB
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    db.execute(ddl)

    with open(filepath, 'rb') as f:
        blob = f.read()
    insert = text(f"""
        INSERT INTO `{uploads_tbl}` (filename, file_data)
        VALUES (:fn, :blob)
    """)
    db.execute(insert, {'fn': filename, 'blob': blob})

    try:
        if filename.lower().endswith('.csv'):
            try:
                df = pd.read_csv(filepath, nrows=5)
                full_df = pd.read_csv(filepath)
            except UnicodeDecodeError:
                df = pd.read_csv(filepath, nrows=5, encoding='latin1')
                full_df = pd.read_csv(filepath, encoding='latin1')
        elif filename.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(filepath, nrows=5)
            full_df = pd.read_excel(filepath)
        else:
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

    return jsonify({
        "columns": df.columns.tolist(),
        "column_metadata": metadata,
        "filename": filename,
        "groups": groups_dict
    }), 200

if __name__ == '__main__':
    from waitress import serve
    serve(app, host="0.0.0.0", port=5000)

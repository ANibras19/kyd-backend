from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
import pandas as pd
import mysql.connector
import os
import traceback

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Upload folder
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Global to track last uploaded file
latest_uploaded_file = None

# MySQL config
db_config = {
    'host': 'enqhzd10cxh7hv2e.cbetxkdyhwsb.us-east-1.rds.amazonaws.com',
    'user': 'v4jbqslxdkfz0ox0',
    'password': 'fxawiuzv6nu61c70',
    'database': 'rnqxqwaljdwgx3un'
}

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
    password = generate_password_hash(data.get('password'))

    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO users (username, first_name, last_name, email, password)
            VALUES (%s, %s, %s, %s, %s)
        """, (username, first_name, last_name, email, password))
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
        cursor.execute("""
            SELECT * FROM users WHERE username = %s OR email = %s
        """, (username_or_email, username_or_email))
        user = cursor.fetchone()
        cursor.close()
        conn.close()

        print("User fetched from DB:", user)

        if user and check_password_hash(user['password'], password):
            print("Password match result: True")
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
        cursor.execute("UPDATE users SET plan = %s WHERE email = %s", (plan, email))
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
    uploaded_file = request.files.get('file')

    if not username or not uploaded_file:
        return jsonify({"error": "Missing username or file"}), 400

    filename = secure_filename(uploaded_file.filename)
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    uploaded_file.save(filepath)
    latest_uploaded_file = filepath

    try:
        if filename.endswith('.csv'):
            try:
                df = pd.read_csv(filepath, nrows=5)
                full_df = pd.read_csv(filepath)
            except UnicodeDecodeError:
                df = pd.read_csv(filepath, nrows=5, encoding='latin1')
                full_df = pd.read_csv(filepath, encoding='latin1')
        elif filename.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(filepath, nrows=5)
            full_df = pd.read_excel(filepath)
        else:
            return jsonify({"error": "Unsupported file type"}), 400
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

    # Profiling logic
    metadata = []
    groups = {}
    for col in df.columns:
        s = df[col]
        full = full_df[col]
        if pd.api.types.is_datetime64_any_dtype(s):
            col_type = "datetime"
        elif pd.api.types.is_numeric_dtype(s):
            col_type = "numeric"
        elif s.nunique() / len(s) < 0.05:
            col_type = "categorical"
        else:
            col_type = "text"

        sem = None
        if "date" in col.lower() or "time" in col.lower():
            sem = "date"
        elif "id" in col.lower():
            sem = "identifier"

        if col_type in ("categorical", "text"):
            groups[col] = full.dropna().astype(str).unique().tolist()

        metadata.append({
            "name": col,
            "type": col_type,
            "semantic": sem
        })

    return jsonify({
        "columns": df.columns.tolist(),
        "column_metadata": metadata,
        "filename": filename,
        "groups": groups
    }), 200

if __name__ == '__main__':
    from waitress import serve
    serve(app, host='0.0.0.0', port=5000)

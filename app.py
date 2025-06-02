from flask import Flask, request, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from flask_cors import CORS
import mysql.connector
import os
import traceback
import pandas as pd
from datetime import datetime

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

@app.after_request
def add_cors_headers(response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type,Authorization")
    response.headers.add("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
    return response

# MySQL connection details
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
    print("Upload endpoint hit")
    username = request.form.get('username')
    if not username:
        return jsonify({"error": "username required"}), 400

    file = request.files.get('file')
    print("Uploaded file:", file)
    if not file:
        return jsonify({"error": "No file uploaded"}), 400

    filename = secure_filename(file.filename)
    filepath = os.path.join("/tmp", filename)
    file.save(filepath)

    try:
        if filename.lower().endswith('.csv'):
            try:
                df = pd.read_csv(filepath)
            except UnicodeDecodeError:
                df = pd.read_csv(filepath, encoding='latin1')
        elif filename.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(filepath)
        else:
            return jsonify({"error": "Unsupported file type"}), 400
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

    col_summary = []
    for col in df.columns:
        col_data = df[col].dropna()
        data_type = str(col_data.dtype)
        summary = {
            "column_name": col,
            "data_type": data_type,
            "type": "numerical" if pd.api.types.is_numeric_dtype(col_data) else "categorical",
        }
        if pd.api.types.is_numeric_dtype(col_data):
            summary.update({
                "count": int(col_data.count()),
                "mean": round(col_data.mean(), 2),
                "median": round(col_data.median(), 2),
                "mode": col_data.mode().tolist(),
                "min": round(col_data.min(), 2),
                "max": round(col_data.max(), 2),
                "std": round(col_data.std(), 2),
                "sum": round(col_data.sum(), 2),
            })
        else:
            unique_vals = col_data.astype(str).unique().tolist()
            summary.update({
                "num_unique": len(unique_vals),
                "unique_values": unique_vals[:10]
            })

        col_summary.append(summary)

    return jsonify({
        "row_count": len(df),
        "col_count": len(df.columns),
        "columns": col_summary
    })

if __name__ == '__main__':
    from waitress import serve
    serve(app, host="0.0.0.0", port=5000)

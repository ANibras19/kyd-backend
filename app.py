from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
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

UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

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

@app.route('/upload', methods=['POST'])
@cross_origin()
def upload_file():
    # ─── Validate Input ──────────────────────────────────────────────────
    username = request.form.get('username')
    if not username:
        return jsonify({"error": "username required"}), 400

    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    # ─── In-Memory Processing (No Filesystem Dependency) ─────────────────
    try:
        # Process file directly from memory
        filename = secure_filename(file.filename)
        file_stream = io.BytesIO(file.read())  # Read into memory

        # ─── Database Storage ───────────────────────────────────────────
        uploads_tbl = f"{username}_uploads"
        
        # Create table if not exists
        db.session.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{uploads_tbl}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255) NOT NULL,
                upload_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_data LONGBLOB
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        
        # Store file
        db.session.execute(
            text(f"INSERT INTO `{uploads_tbl}` (filename, file_data) VALUES (:fn, :blob)"),
            {'fn': filename, 'blob': file_stream.getvalue()}
        )
        db.session.commit()

        # ─── Data Analysis ──────────────────────────────────────────────
        file_stream.seek(0)  # Rewind for reading
        
        # Sample first 5 rows
        sample_df = pd.read_csv(file_stream, nrows=5) if filename.lower().endswith('.csv') \
            else pd.read_excel(file_stream, nrows=5)
        
        # Full read for groups (new stream)
        file_stream.seek(0)
        full_df = pd.read_csv(file_stream) if filename.lower().endswith('.csv') \
            else pd.read_excel(file_stream)

        # ─── Generate Metadata ──────────────────────────────────────────
        metadata = []
        groups_dict = {}
        
        for col in sample_df.columns:
            col_type = (
                "datetime" if pd.api.types.is_datetime64_any_dtype(sample_df[col]) else
                "numeric" if pd.api.types.is_numeric_dtype(sample_df[col]) else
                "categorical" if sample_df[col].nunique() / len(sample_df[col]) < 0.05 else
                "text"
            )
            
            metadata.append({
                "name": col,
                "type": col_type,
                "semantic": ("date" if any(x in col.lower() for x in ["date", "time"]) 
                            or ("identifier" if "id" in col.lower() else None)
            })
            
            if col_type in ("categorical", "text"):
                groups_dict[col] = full_df[col].dropna().astype(str).unique().tolist()

        return jsonify({
            "columns": sample_df.columns.tolist(),
            "column_metadata": metadata,
            "filename": filename,
            "groups": groups_dict
        })

    except UnicodeDecodeError:
        file_stream.seek(0)
        sample_df = pd.read_csv(file_stream, nrows=5, encoding='latin1')
        # ... repeat processing with latin1 encoding
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Processing failed: {str(e)}"}), 500

@app.route('/upload', methods=['POST'])
@cross_origin()  # Explicitly allow CORS on this route
def upload_file():
    file = request.files.get('file')
    if not file:
        return jsonify({"error": "No file uploaded"}), 400

    filename = secure_filename(file.filename)
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    file.save(filepath)

    # Read and extract columns
    try:
        if filename.lower().endswith('.csv'):
            df = pd.read_csv(filepath)
        elif filename.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(filepath)
        else:
            return jsonify({"error": "Unsupported file format"}), 400
    except Exception as e:
        return jsonify({"error": f"Failed to read file: {str(e)}"}), 500

    return jsonify({
        "columns": df.columns.tolist(),
        "row_count": len(df),
        "col_count": len(df.columns)
    }), 200

if __name__ == '__main__':
    from waitress import serve
    serve(app, host='0.0.0.0', port=5000)

from flask import Flask, request, jsonify
from dotenv import load_dotenv
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from flask_cors import CORS
import mysql.connector
import pandas as pd
import json
import os
from openai import OpenAI
load_dotenv()  # Load from .env file
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# MySQL config
db_config = {
    'host': 'enqhzd10cxh7hv2e.cbetxkdyhwsb.us-east-1.rds.amazonaws.com',
    'user': 'v4jbqslxdkfz0ox0',
    'password': 'fxawiuzv6nu61c70',
    'database': 'rnqxqwaljdwgx3un'
}

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text

# Add SQLAlchemy config (use same credentials)
app.config['SQLALCHEMY_DATABASE_URI'] = (
    'mysql+mysqlconnector://v4jbqslxdkfz0ox0:fxawiuzv6nu61c70@enqhzd10cxh7hv2e.cbetxkdyhwsb.us-east-1.rds.amazonaws.com/rnqxqwaljdwgx3un'
)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


@app.route('/')
def home():
    return "Backend is running and connected to database."

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
    username = request.form.get('username')
    if not username:
        return jsonify({"error": "username required"}), 400

    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    try:
        filename = secure_filename(file.filename)
        file_stream = io.BytesIO(file.read())
        uploads_tbl = f"{username}_uploads"

        # ─── Check user plan ──────────────────────
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT plan FROM users WHERE username = %s", (username,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()

        if not user:
            return jsonify({"error": "User not found"}), 404

        plan = user['plan']

        # ─── Create user upload table if not exists ─────────────
        db.session.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{uploads_tbl}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255) NOT NULL,
                upload_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_data LONGBLOB
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        db.session.commit()

        # ─── If Free plan, check for existing upload ────────────
        if plan.lower() == "free":
            existing = db.session.execute(
                text(f"SELECT COUNT(*) AS count FROM `{uploads_tbl}`")
            ).fetchone()
            if existing['count'] > 0:
                return jsonify({"error": "Free plan allows only one dataset upload"}), 403

        # ─── Insert new file ─────────────────────────────────────
        db.session.execute(
            text(f"INSERT INTO `{uploads_tbl}` (filename, file_data) VALUES (:fn, :blob)"),
            {'fn': filename, 'blob': file_stream.getvalue()}
        )
        db.session.commit()

        # ─── Analyze file ────────────────────────────────────────
        file_stream.seek(0)
        sample_df = pd.read_csv(file_stream, nrows=5) if filename.lower().endswith('.csv') \
            else pd.read_excel(file_stream, nrows=5)

        file_stream.seek(0)
        full_df = pd.read_csv(file_stream) if filename.lower().endswith('.csv') \
            else pd.read_excel(file_stream)
        parsed_data = full_df.head(100).to_dict(orient='records')  # limit to avoid overload

        metadata = []
        groups_dict = {}
        for col in sample_df.columns:
            series_full = full_df[col].dropna()
            dtype = str(full_df[col].dtype)
            nunique = series_full.nunique()
            avg_len = series_full.astype(str).map(len).mean() if dtype == 'object' else 0

            if pd.api.types.is_datetime64_any_dtype(full_df[col]):
                col_type = "datetime"
            elif pd.api.types.is_numeric_dtype(full_df[col]):
                col_type = "numeric"
            elif dtype == 'object' or dtype == 'string':
                if nunique < 50 and avg_len <= 20:
                    col_type = "categorical"
                else:
                    col_type = "text"
            else:
                col_type = "text"

            metadata.append({
                "name": col,
                "type": col_type,
                "semantic": (
                    "date" if any(x in col.lower() for x in ["date", "time"]) else
                    "identifier" if "id" in col.lower() else None
                )
            })

            if col_type in ("categorical", "text"):
                groups_dict[col] = series_full.astype(str).unique().tolist()

        return jsonify({
            "columns": sample_df.columns.tolist(),
            "column_metadata": metadata,
            "filename": filename,
            "groups": groups_dict
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Upload failed: {str(e)}"}), 500

@app.route('/list-uploads', methods=['GET'])
@cross_origin()
def list_uploads():
    username = request.args.get('username')
    if not username:
        return jsonify({"error": "username required"}), 400

    table_name = f"{username}_uploads"
    try:
        results = db.session.execute(
            text(f"SELECT filename, upload_time FROM `{table_name}` ORDER BY upload_time DESC")
        ).mappings().all()

        uploads = [
            {"filename": row['filename'], "upload_time": row['upload_time'].strftime('%Y-%m-%d %H:%M')}
            for row in results
        ]

        return jsonify(uploads), 200

    except Exception as e:
        return jsonify({"error": f"Failed to list uploads: {str(e)}"}), 500

@app.route('/load-upload', methods=['POST'])
@cross_origin()
def load_upload():
    data = request.get_json()
    username = data.get('username')
    filename = data.get('filename')

    if not username or not filename:
        return jsonify({"error": "Username and filename required"}), 400

    table = f"{username}_uploads"

    try:
        result = db.session.execute(
            text(f"SELECT file_data FROM `{table}` WHERE filename = :filename ORDER BY upload_time DESC LIMIT 1"),
            {"filename": filename}
        ).fetchone()

        if not result:
            return jsonify({"error": "File not found"}), 404

        file_stream = io.BytesIO(result[0])

        sample_df = pd.read_csv(file_stream, nrows=5) if filename.lower().endswith('.csv') \
            else pd.read_excel(file_stream, nrows=5)

        file_stream.seek(0)
        full_df = pd.read_csv(file_stream) if filename.lower().endswith('.csv') \
            else pd.read_excel(file_stream)
        parsed_data = full_df.head(100).to_dict(orient='records')  # limit to avoid overload

        metadata = []
        groups_dict = {}
        for col in sample_df.columns:
            series_full = full_df[col].dropna()
            dtype = str(full_df[col].dtype)
            nunique = series_full.nunique()
            avg_len = series_full.astype(str).map(len).mean() if dtype == 'object' else 0

            if pd.api.types.is_datetime64_any_dtype(full_df[col]):
                col_type = "datetime"
            elif pd.api.types.is_numeric_dtype(full_df[col]):
                col_type = "numeric"
            elif dtype == 'object' or dtype == 'string':
                if nunique < 50 and avg_len <= 20:
                    col_type = "categorical"
                else:
                    col_type = "text"
            else:
                col_type = "text"

            metadata.append({
                "name": col,
                "type": col_type,
                "semantic": (
                    "date" if any(x in col.lower() for x in ["date", "time"]) else
                    "identifier" if "id" in col.lower() else None
                )
            })

            if col_type in ("categorical", "text"):
                groups_dict[col] = series_full.astype(str).unique().tolist()

        return jsonify({
            "columns": sample_df.columns.tolist(),
            "column_metadata": metadata,
            "filename": filename,
            "groups": groups_dict,
            "parsed_data": parsed_data
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Load failed: {str(e)}"}), 500

from openai import OpenAI
client = OpenAI()

@app.route('/explain-test', methods=['POST'])
def explain_test():
    try:
        data = request.get_json()
        username = data.get('username')
        filename = data.get('filename')
        selected_groups = data.get('selected_groups')
        column_metadata = data.get('column_metadata')
        test_name = data.get('test_name')
        preview_rows = data.get('previewRows')

        prompt = f"""
You are a data assistant helping a non-technical user analyze data in a file named '{filename}'.
They have selected column groups: {selected_groups}
They have metadata about columns: {column_metadata}
They are viewing a few rows of data: {preview_rows}
They want to run this test: '{test_name}'.

Please answer simply:
1. What does this test do?
2. Why is it useful?
3. What kind of result will it return?

Avoid technical jargon. Explain like you're helping a beginner.
"""

        response = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.5
        )

        explanation = response['choices'][0]['message']['content']
        return jsonify({"explanation": explanation})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/suggest-tests', methods=['POST'])
def suggest_tests():
    import ast

    try:
        data = request.get_json()
        print("📥 Incoming payload to /suggest-tests:\n", json.dumps(data, indent=2))
        username = data.get('username') or 'unknown'
        filename = data['filename']
        selected_groups = data['selected_groups']
        column_metadata = data['column_metadata']
        objective = data['objective']
        preview_rows = data.get('previewRows', [])

        if not preview_rows or len(preview_rows) == 0:
            return jsonify({'error': 'No preview rows found. Please reload dataset and try again.'}), 400

        sample = pd.DataFrame(preview_rows)

        test_categories = {
            "Mean/Median Comparison": [
                "Independent t-test", "Paired t-test", "One-way ANOVA", "Welch’s ANOVA",
                "Mann-Whitney U test", "Wilcoxon signed-rank test", "Kruskal-Wallis H test", "Friedman test"
            ],
            "Correlation and Association": [
                "Pearson correlation", "Spearman rank correlation", "Kendall’s tau",
                "Chi-square test of independence", "Fisher’s Exact Test", "Cramér’s V", "Point-Biserial correlation"
            ],
            "Normality and Distribution": [
                "Shapiro-Wilk test", "Kolmogorov–Smirnov test", "Anderson-Darling test",
                "Lilliefors test", "Jarque-Bera test"
            ],
            "Homogeneity of Variance": [
                "Levene’s test", "Bartlett’s test"
            ],
            "Proportions and Rates": [
                "Z-test for proportions", "Chi-square goodness-of-fit test", "McNemar’s test"
            ],
            "Regression and Model Fit": [
                "Simple Linear Regression", "Multiple Linear Regression", "Logistic Regression",
                "Hosmer-Lemeshow test", "ANOVA for Regression"
            ],
            "Reliability / Agreement": [
                "Cohen’s Kappa", "Intraclass Correlation Coefficient (ICC)"
            ],
            "Outlier and Sensitivity": [
                "Grubbs’ Test"
            ]
        }

        prompt = f"""
You are a data analysis assistant.

Only choose from the following list of statistical tests:
{json.dumps(test_categories, indent=2)}

User objective: {objective}

User selected these groups and columns:
{json.dumps(selected_groups, indent=2)}

Column metadata:
{json.dumps(column_metadata, indent=2)}

Here are sample rows from the dataset:
{sample.to_markdown(index=False)}

👉 Based on all of this, return a dictionary where each category maps to 0 or more suitable test names from the above list.

Do NOT invent new tests. Do NOT return explanations. ONLY return test names organized by category as a Python dictionary with standard straight quotes.
"""

        response = client.chat.completions.create(
            model='gpt-4o',
            messages=[
                {"role": "system", "content": "You are a statistical assistant that strictly returns allowed tests."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=700
        )

        reply = response.choices[0].message.content.strip()
        print("🔍 GPT reply:\n", reply)

        # Clean fancy/smart quotes and strip code block if present
        cleaned_reply = (
            reply.replace("“", '"')
                 .replace("”", '"')
                 .replace("’", "'")
                 .strip()
        )

        if cleaned_reply.startswith("```") and cleaned_reply.endswith("```"):
            lines = cleaned_reply.splitlines()
            if len(lines) >= 3:
                cleaned_reply = "\n".join(lines[1:-1])  # Remove first and last lines

        try:
            test_map = json.loads(cleaned_reply)
        except Exception:
            try:
                test_map = ast.literal_eval(cleaned_reply)
            except Exception as parse_error:
                print("❌ Failed to parse GPT response:\n", cleaned_reply)
                return jsonify({'error': 'Invalid GPT response format', 'raw': cleaned_reply}), 500

        return jsonify(test_map)

    except Exception as e:
        print("🔥 Exception in /suggest-tests:", str(e))
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    from waitress import serve
    serve(app, host='0.0.0.0', port=5000)

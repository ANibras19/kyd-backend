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
        file_bytes = file.read()
        file_stream = io.BytesIO(file_bytes)
        uploads_tbl = f"{username}_uploads"

        # ─── Analyse the file first ───────────────────────────────
        file_stream.seek(0)
        try:
            sample_df = pd.read_csv(file_stream, nrows=5)
            file_format = 'csv'
            file_stream.seek(0)
            full_df = pd.read_csv(file_stream)
        except Exception:
            file_stream.seek(0)
            sample_df = pd.read_excel(file_stream, nrows=5)
            file_format = 'excel'
            file_stream.seek(0)
            full_df = pd.read_excel(file_stream)

        if sample_df.empty or len(sample_df.columns) == 0:
            return jsonify({"error": "File appears empty or unstructured"}), 400

        missing_columns = [col for col in full_df.columns if full_df[col].isnull().any()]
        if missing_columns:
            return jsonify({
                "missing_columns": missing_columns,
                "filename": filename,
                "format": file_format
            }), 200

        # ─── Proceed with DB insert only if user accepted fills ──────
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT plan FROM users WHERE username = %s", (username,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()

        if not user:
            return jsonify({"error": "User not found"}), 404

        plan = user['plan']

        db.session.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{uploads_tbl}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename      VARCHAR(255) NOT NULL,
                upload_time   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_data     LONGBLOB
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        db.session.commit()

        if plan.lower() == "free":
            existing = db.session.execute(
                text(f"SELECT COUNT(*) AS count FROM `{uploads_tbl}`")
            ).fetchone()
            if existing['count'] > 0:
                return jsonify({"error": "Free plan allows only one dataset upload"}), 403

        # Insert after clean
        db.session.execute(
            text(f"INSERT INTO `{uploads_tbl}` (filename, file_data) VALUES (:fn, :blob)"),
            {'fn': filename, 'blob': file_bytes}
        )
        db.session.commit()

        # ─── Metadata generation ────────────────────────────
        row_count, col_count = map(int, full_df.shape)
        parsed_data = full_df.head(100).to_dict(orient='records')

        metadata = []
        groups_dict = {}

        for col in sample_df.columns:
            series_full = full_df[col].dropna()
            dtype = str(full_df[col].dtype)
            nunique = series_full.nunique()
            avg_len = series_full.astype(str).map(len).mean() if not series_full.empty and dtype == 'object' else 0

            if series_full.empty:
                col_type = "empty"
            elif pd.api.types.is_datetime64_any_dtype(full_df[col]):
                col_type = "datetime"
            elif pd.api.types.is_numeric_dtype(full_df[col]):
                col_type = "numeric"
            elif dtype in ('object', 'string'):
                col_type = "categorical" if (nunique < 50 and avg_len <= 20) else "text"
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
            "parsed_data": parsed_data,
            "row_count": row_count,
            "col_count": col_count,
            "format": file_format
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

        file_bytes = result[0]
        file_stream = io.BytesIO(file_bytes)

        try:
            sample_df = pd.read_csv(file_stream, nrows=5)
            file_format = 'csv'
        except Exception:
            file_stream.seek(0)
            sample_df = pd.read_excel(file_stream, nrows=5)
            file_format = 'excel'

        if sample_df.empty or len(sample_df.columns) == 0:
            return jsonify({"error": "File appears empty or unstructured"}), 400

        file_stream.seek(0)
        full_df = pd.read_csv(file_stream) if file_format == 'csv' else pd.read_excel(file_stream)

        row_count, col_count = map(int, full_df.shape)
        parsed_data = full_df.head(100).to_dict(orient='records')

        metadata = []
        groups_dict = {}

        for col in sample_df.columns:
            series_full = full_df[col].dropna()
            dtype = str(full_df[col].dtype)
            nunique = series_full.nunique()
            avg_len = series_full.astype(str).map(len).mean() if not series_full.empty and dtype == 'object' else 0

            if series_full.empty:
                col_type = "empty"
            elif pd.api.types.is_datetime64_any_dtype(full_df[col]):
                col_type = "datetime"
            elif pd.api.types.is_numeric_dtype(full_df[col]):
                col_type = "numeric"
            elif dtype in ('object', 'string'):
                col_type = "categorical" if (nunique < 50 and avg_len <= 20) else "text"
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
            "parsed_data": parsed_data,
            "row_count": row_count,
            "col_count": col_count,
            "format": file_format
        })

    except Exception as e:
        db.session.rollback()
        return jsonify({"error": f"Load failed: {str(e)}"}), 500
    
@app.route('/explain-test', methods=['POST'])
def explain_test():
    try:
        data = request.get_json()
        username = data.get('username')
        filename = data.get('filename')
        selected_groups = data.get('selected_groups', {})
        column_metadata = data.get('column_metadata', [])
        test_name = data.get('test_name')
        preview_rows = data.get('previewRows', [])

        selected_columns = list(selected_groups.keys())
        metadata_info = json.dumps(column_metadata, indent=2)
        preview_info = json.dumps(preview_rows, indent=2)
        selected_info = json.dumps(selected_groups, indent=2)

        if not selected_columns:
            prompt = f"""
You are a statistical assistant helping a beginner understand how to use the test: '{test_name}' on their dataset named '{filename}'.

They have NOT selected any columns yet.

You are provided with:
• Column metadata (types and names):  
{metadata_info}

• A few preview rows from the dataset:  
{preview_info}

Your task is to explain the test contextually based on this actual dataset — referencing relevant column names and example values whenever useful. Write in a friendly tone for a beginner user.

Respond using exactly 5 numbered questions with detailed answers:

Q1. What does this test do?  
Q2. Why is it useful for this dataset?  
   → Use real column names and sample values to explain **what this test can reveal**, and **why those findings matter**. For example, how it could help identify trends, inform decisions, or uncover problems hidden in the data.  
Q3. What column types and combinations are needed to run this test?  
   → Clearly describe the required types (e.g., 1 numeric, 2 categorical).  
   → If the test requires only one column, say: "This test requires one [type] column. You may choose from: col1, col2, col3."  
   → If the test needs multiple, include 2–3 valid combinations using real column names like [Gender, Score].  
   → Explain briefly what each combination would help the user understand in this specific dataset.  
Q4. What chart or visualization can be shown after this test and why?  
   → Suggest a visual that suits the data and test type (e.g., histogram, boxplot, bar chart, mosaic plot).  
   → Explain what the visual will show and **how it helps interpret** the test result — such as patterns, distributions, or outliers the user should look for.  
Q5. Is this selection valid?  
   → Since the user has not selected columns, say: "The selection is incomplete. Please choose the required columns to proceed."

Avoid greetings and markdown. Output each Q exactly as shown.
"""
        else:
            prompt = f"""
You are a statistical assistant helping a beginner apply the test: '{test_name}' on their dataset named '{filename}'.

The user selected these columns for analysis:
{selected_info}

You are also provided with:
• Column metadata (types and names):  
{metadata_info}

• A few preview rows from the dataset:  
{preview_info}

Your job is to explain how this test works using the actual dataset, the selected columns, and relevant values.

Respond using exactly 5 numbered questions with detailed answers:

Q1. What does this test do?  
Q2. Why is it useful for this dataset?  
   → Use real column names and sample values to explain **what this test can reveal**, and **why those findings matter**. For example, how it could help identify trends, inform decisions, or uncover problems hidden in the data.  
Q3. What will the test reveal based on selected data?  
   → Clearly describe the required types (e.g., 2 categorical or 1 numeric).  
   → Include 2–3 valid combinations using real column names like [Gender, Score].  
   → Explain briefly what each combination would help the user understand in this specific dataset.  
Q4. What chart or visualization will help and why?  
   → Suggest one or more visuals suited for this test and dataset.  
   → Explain what it will show and how it supports understanding of the result (e.g., distribution, patterns, or relationships).  
Q5. Is this selection valid?  
   → If valid, say only: This selection is valid to run the test.  
   → If invalid, explain clearly what to add or remove.

Avoid greetings and markdown. Output each Q exactly as shown.
"""

        # GPT call
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.5
        )

        full_text = response.choices[0].message.content.strip()

        # Normalize and split by Q1–Q5
        qa_map = {}
        current_q = None
        for line in full_text.splitlines():
            line = line.strip()
            if line.startswith("Q1.") or line.startswith("Q2.") or line.startswith("Q3.") or line.startswith("Q4.") or line.startswith("Q5."):
                current_q = line[:2]
                qa_map[current_q] = line[3:].strip()
            elif current_q:
                qa_map[current_q] += '\n' + line

        # Combine to send back clean structured string for frontend
        cleaned_sections = []
        for i in range(1, 6):
            q_key = f"Q{i}"
            if q_key in qa_map:
                cleaned_sections.append(f"{q_key}. {qa_map[q_key].strip()}")

        clean_text = "\n\n".join(cleaned_sections).strip()

        # Determine proceed permission
        can_proceed = "Q5" in qa_map and "valid to run the test" in qa_map["Q5"].lower()

        # Extract valid column combinations from Q3 if invalid
        combinations = []
        if not can_proceed and "Q3" in qa_map:
            q3_lines = qa_map["Q3"].split('\n')
            for line in q3_lines:
                if '[' in line and ']' in line:
                    inner = line[line.find('[')+1 : line.find(']')]
                    cols = [x.strip().strip('"') for x in inner.split(',') if x.strip()]
                    if len(cols) > 1:
                        combinations.append(cols)

        return jsonify({
            "explanation": clean_text,
            "required_columns": combinations,
            "can_proceed": can_proceed
        })

    except Exception as e:
        print("\U0001f534 /explain-test ERROR:", e)
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
    serve(app, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))

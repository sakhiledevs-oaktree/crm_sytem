import os
import re
import pandas as pd
import psycopg2
import psycopg2.extras
from flask import Flask, render_template, request, redirect, jsonify, url_for, flash, send_from_directory
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from psycopg2.extras import execute_values
from io import StringIO
from psycopg2.pool import ThreadedConnectionPool



app = Flask(__name__)
app.secret_key = "dev-secret-key"

NEON_DATABASE_URL = 'postgresql://neondb_owner:npg_J0LaKIwNbX3o@ep-frosty-hat-ahg0tukc-pooler.c-3.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'

from psycopg2 import pool

# Create a global pool (min 1 connection, max 10)
db_pool = psycopg2.pool.SimpleConnectionPool(1, 10, NEON_DATABASE_URL)


db_pool = ThreadedConnectionPool(1, 10, NEON_DATABASE_URL)

def get_db_connection():
    return db_pool.getconn()

def put_db_connection(conn):
    db_pool.putconn(conn)

# 1. MAPPING LOGIC: Converts "Mentorship X" to the correct Day
MENTORSHIP_MAP = {
    "1": "monday",
    "2": "tuesday",
    "3": "wednesday",
    "4": "thursday"
}

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login" # Redirects here if user isn't logged in

def init_db():
    conn = psycopg2.connect(NEON_DATABASE_URL)
    cur = conn.cursor()

    # 1. Cohort Candidates Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cohort_candidates (
            id SERIAL PRIMARY KEY,
            cohort TEXT NOT NULL,
            client TEXT,
            primary_contact TEXT,
            email TEXT,
            phone TEXT,
            id_number TEXT NOT NULL,
            cipc_number TEXT,
            tier TEXT,
            source TEXT,
            comment TEXT,
            company TEXT,
            position TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            CONSTRAINT uniq_cohort_id UNIQUE (cohort, id_number)
        );
    """)

    # 2. Survey Submissions Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS survey_submissions (
            id SERIAL PRIMARY KEY,
            respondent_email TEXT,
            survey_number INTEGER,
            cohort_tag TEXT,
            q1 INTEGER, q2 INTEGER, q3 INTEGER, q4 INTEGER,
            apply_plan TEXT,
            key_learnings TEXT,
            submitted_at TIMESTAMP DEFAULT NOW()
        );
    """)

    # 3. USERS Table (For Login)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL
        );
    """)

    # 4. Insert default admin if table is empty
    cur.execute("""
        INSERT INTO users (username, password) 
        VALUES ('0aktree', '@dm!n0aktree') 
        ON CONFLICT (username) DO NOTHING;
    """)

    conn.commit()
    cur.close()  # Only close after EVERYTHING is done
    conn.close()
    print("Database initialized successfully.")
# ==========================
# FILE LOADER (CSV + EXCEL)#
# ==========================

class User(UserMixin):
    def __init__(self, id, username):
        self.id = id
        self.username = username


def get_db():
    return db_pool.getconn()

def release_db(conn):
    db_pool.putconn(conn)

@login_manager.user_loader
def load_user(user_id):
    conn = psycopg2.connect(NEON_DATABASE_URL)
    cur = conn.cursor()
    cur.execute("SELECT id, username FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    if row:
        return User(row[0], row[1])
    return None

# ==========================
# LOGIN ROUTES             #
# ==========================

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        
        conn = psycopg2.connect(NEON_DATABASE_URL)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM users WHERE username = %s", (username,))
        user_row = cur.fetchone()
        cur.close(); conn.close()

        # Simple check (For better security, use check_password_hash)
        if user_row and user_row['password'] == password:
            user_obj = User(user_row['id'], user_row['username'])
            login_user(user_obj)
            return redirect(url_for("dashboard"))
        else:
            flash("Invalid username or password")
            
    return render_template("login.html")

@app.route("/logout")
def logout():
    logout_user()
    return redirect(url_for("login"))

def load_tabular_file(file):
    filename = file.filename.lower()

    if filename.endswith(".csv"):
        return pd.read_csv(file, encoding="utf-8-sig")

    elif filename.endswith(".xlsx") or filename.endswith(".xls"):
        return pd.read_excel(file)

    else:
        raise ValueError("Unsupported file format")

@app.route("/api/surveys/<path:email>/<int:num>")
def get_survey_details(email, num):
    conn = psycopg2.connect(NEON_DATABASE_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    clean_email = email.lower().strip()
    
    try:
        # Only select the 2 columns we are now using
        cur.execute("""
            SELECT q1, q2, apply_plan, key_learnings 
            FROM survey_submissions 
            WHERE LOWER(respondent_email) = %s 
            AND survey_number = %s
        """, (clean_email, num))
        
        res = cur.fetchone()
        
        if res:
            return jsonify({
                "scores": [res['q1'], res['q2']], # Only 2 values sent to JS
                "feedback": {
                    "apply_plan": res['apply_plan'],
                    "key_learnings": res['key_learnings']
                }
            })
        
        return jsonify({"scores": [0,0], "feedback": None}), 200
            
    except Exception as e:
        print(f"API Error: {e}")
        return jsonify({"error": "Server error"}), 500
    finally:
        cur.close(); conn.close()

@app.route("/api/cohort_analysis/<cohort_name>")
def cohort_analysis(cohort_name):
    conn = psycopg2.connect(NEON_DATABASE_URL)
    cur = conn.cursor()
    
    # Calculate the average for each of the 6 questions for the whole cohort
    cur.execute("""
        SELECT 
            AVG(q1), AVG(q2), AVG(q3), AVG(q4), AVG(q5), AVG(q6)
        FROM survey_submissions s
        JOIN cohort_candidates c ON s.respondent_email = c.email
        WHERE c.cohort = %s
    """, (cohort_name,))
    
    stats = cur.fetchone()
    cur.close(); conn.close()
    
    # Returns the 'Pulse' of the cohort
    return jsonify({
        "labels": ["Leadership", "Strategy", "Finance", "Marketing", "Operations", "Impact"],
        "averages": [float(x) if x else 0 for x in stats]
    })
@app.route("/")
@login_required
def dashboard():
    cohort = request.args.get("cohort", "All")
    conn = psycopg2.connect(NEON_DATABASE_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # 1. UNIFIED QUERY
    query = """
        SELECT 
            c.*, 
            COALESCE(ARRAY_AGG(s.survey_number) FILTER (WHERE s.survey_number IS NOT NULL), '{}') as completed_surveys
        FROM cohort_candidates c
        LEFT JOIN survey_submissions s ON LOWER(TRIM(c.email)) = LOWER(TRIM(s.respondent_email))
        WHERE (%s = 'All' OR c.cohort = %s)
        GROUP BY c.id 
        ORDER BY c.client ASC
    """
    cur.execute(query, (cohort, cohort))
    rows = cur.fetchall()

    # 2. CALCULATE ANALYTICS
    attendance_trend = [0] * 6
    # FIXED: Only q1 and q2 initialized
    avg_stats = {"q1": 0, "q2": 0} 
    recent_comments = []

    if cohort != "All":
        # Get counts for Attendance Trend
        cur.execute("""
            SELECT survey_number, COUNT(*) 
            FROM survey_submissions 
            WHERE cohort_tag = %s GROUP BY survey_number
        """, (cohort,))
        for count_row in cur.fetchall():
            if 1 <= count_row[0] <= 6:
                attendance_trend[count_row[0]-1] = count_row[1]

        # FIXED: Only SELECT q1 and q2 averages
        cur.execute("""
            SELECT AVG(q1), AVG(q2) 
            FROM survey_submissions WHERE cohort_tag = %s
        """, (cohort,))
        
        stat_row = cur.fetchone()
        
        # FIXED: Safe rounding check to prevent NoneType errors
        if stat_row and stat_row[0] is not None:
            avg_stats = {
                "q1": round(stat_row[0], 1) if stat_row[0] else 0, 
                "q2": round(stat_row[1], 1) if stat_row[1] else 0
            }

        # Get Recent Key Learnings
        cur.execute("""
            SELECT key_learnings, apply_plan FROM survey_submissions 
            WHERE cohort_tag = %s ORDER BY submitted_at DESC LIMIT 5
        """, (cohort,))
        recent_comments = cur.fetchall()
    
    cur.close(); conn.close()
    return render_template("dashboard.html", rows=rows, cohort=cohort, 
                           attendance_trend=attendance_trend, stats=avg_stats, comments=recent_comments)
# ==========================
# UPLOAD COHORT FILE
# ==========================



def get_clean_row_val(row_dict, possible_names):
    """Fuzzy-matches columns and preserves long ID/CIPC numbers from scientific notation."""
    clean_row = {str(k).strip().lower(): v for k, v in row_dict.items()}
    for name in possible_names:
        val = clean_row.get(name.lower().strip())
        if pd.notna(val) and str(val).lower() != "nan":
            s = str(val).strip()
            
            # 1. REMOVE TRAILING .0 (Common in Excel floats)
            if s.endswith('.0'): 
                s = s[:-2]
                
            # 2. CONVERT SCIENTIFIC NOTATION (e.g., 1.23E+12)
            if "e+" in s.lower():
                try:
                    # float conversion followed by formatting removes scientific shorthand
                    s = "{:.0f}".format(float(s))
                except: 
                    pass
            return s
    return ""
@app.route("/upload/<cohort_context>", methods=["POST"])
def upload(cohort_context):
    file = request.files.get("file")
    if not file:
        return redirect(url_for("dashboard", cohort=cohort_context))

    conn = psycopg2.connect(NEON_DATABASE_URL)
    cur = conn.cursor()

    try:
        # 1. LOAD DATA
        if file.filename.lower().endswith(".csv"):
            raw = file.read().decode("utf-8", errors="ignore")
            df = pd.read_csv(StringIO(raw), sep=None, engine='python', dtype=str)
        else:
            df = pd.read_excel(file, engine="openpyxl", dtype=str)

        # 2. CLEAN COLUMN NAMES
        df.columns = [str(c).strip().lower() for c in df.columns]
        
        # 3. FIX SCIENTIFIC NOTATION & FLOAT FORMATTING FOR ID NUMBERS
        # Identify the ID column dynamically
        id_col_name = next((c for c in df.columns if 'id' in c), None)
        
        if id_col_name:
            def clean_id(val):
                val = str(val).strip()
                if not val or val.lower() == 'nan':
                    return None
                # Convert scientific '1.23e+12' to full string '1234567890123'
                if 'e+' in val.lower():
                    try:
                        val = '{:.0f}'.format(float(val))
                    except:
                        pass
                # Remove trailing '.0' (common when reading Excel integers)
                if val.endswith('.0'):
                    val = val[:-2]
                return val

            df[id_col_name] = df[id_col_name].apply(clean_id)

        df = df.dropna(how='all')
        data_to_insert = []

        for _, row in df.iterrows():
            row_dict = row.to_dict()

            # A. COHORT MAPPING
            raw_source = get_clean_row_val(row_dict, ["source"])
            digit_match = re.search(r'\d', str(raw_source))
            m_digit = digit_match.group(0) if digit_match else None
            assigned_day = MENTORSHIP_MAP.get(m_digit, "Unassigned")

            # B. GET CLEANED ID
            id_num = row_dict.get(id_col_name)
            if not id_num or str(id_num).lower() == "id number":
                continue

            data_to_insert.append((
                assigned_day,
                get_clean_row_val(row_dict, ["client"]),
                get_clean_row_val(row_dict, ["primary contact", "primary cont", "contact person"]),
                get_clean_row_val(row_dict, ["email"]),
                get_clean_row_val(row_dict, ["phone", "cell", "contact number"]),
                id_num,
                get_clean_row_val(row_dict, ["cipc number", "cipc"]),
                get_clean_row_val(row_dict, ["tier"]),
                raw_source,
                get_clean_row_val(row_dict, ["contactability", "comment", "comments"])
            ))

        if data_to_insert:
            execute_values(cur, """
                INSERT INTO cohort_candidates (
                    cohort, client, primary_contact, email, phone, 
                    id_number, cipc_number, tier, source, comment
                ) VALUES %s
                ON CONFLICT (id_number) DO UPDATE SET
                    client = COALESCE(NULLIF(EXCLUDED.client, ''), cohort_candidates.client),
                    primary_contact = COALESCE(NULLIF(EXCLUDED.primary_contact, ''), cohort_candidates.primary_contact),
                    email = COALESCE(NULLIF(EXCLUDED.email, ''), cohort_candidates.email),
                    phone = COALESCE(NULLIF(EXCLUDED.phone, ''), cohort_candidates.phone),
                    tier = EXCLUDED.tier,
                    comment = COALESCE(cohort_candidates.comment, EXCLUDED.comment)
            """, data_to_insert)

        conn.commit()
        flash(f"Sync Complete: {len(data_to_insert)} records processed.")

    except Exception as e:
        conn.rollback()
        flash(f"Error: {str(e)}")
    finally:
        cur.close(); conn.close()

    return redirect(url_for("dashboard", cohort=cohort_context))

@app.route('/survey/<cohort>/<session_id>', methods=['GET', 'POST'])
def handle_survey(cohort, session_id):
    conn = db_pool.getconn()
    try:
        if request.method == 'POST':
            step = request.form.get('step')

            if step == '1':
                # ... (Step 1 remains the same) ...
                email = request.form.get('email', '').strip().lower()
                name = request.form.get('name', '').strip()
                phone = request.form.get('phone', '').strip()
                
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute("SELECT client, email FROM cohort_candidates WHERE TRIM(LOWER(email)) = %s", (email,))
                    candidate = cur.fetchone()
                
                user_data = {
                    "client": candidate['client'] if candidate else name,
                    "email": email,
                    "phone": phone
                }
                is_guest = False if candidate else True
                
                return render_template('survey_entry.html', cohort=cohort, session=session_id, 
                                     step=2, user=user_data, is_guest=is_guest)

            elif step == '2':
                email = request.form.get('verified_email', '').strip().lower()
                client_name = request.form.get('verified_name', '').strip()
                phone = request.form.get('verified_phone', '').strip()

                with conn:
                    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                        # A. Check for existing candidate
                        cur.execute("SELECT cohort, client, phone FROM cohort_candidates WHERE TRIM(LOWER(email)) = %s", (email,))
                        candidate = cur.fetchone()
                        
                        assigned_tag = candidate['cohort'] if candidate else "Guest"
                        
                        # B. Registration/Enrichment Logic
                        if not candidate:
                            # Register a completely new guest
                            cur.execute("""
                                INSERT INTO cohort_candidates (cohort, client, email, phone, id_number, comment)
                                VALUES (%s, %s, %s, %s, %s, 'Self-registered Guest')
                            """, (assigned_tag, client_name, email, phone, f"GUEST-{email}"))
                        else:
                            # ENRICHMENT: Only update fields if they are currently empty/null
                            # We mark it as 'Enriched' in the comment for the purple badge
                            cur.execute("""
                                UPDATE cohort_candidates 
                                SET 
                                    phone = COALESCE(NULLIF(phone, ''), %s),
                                    client = COALESCE(NULLIF(client, ''), %s),
                                    comment = CASE 
                                        WHEN (phone IS NULL OR phone = '') OR (client IS NULL OR client = '') 
                                        THEN 'Enriched' 
                                        ELSE comment 
                                    END
                                WHERE TRIM(LOWER(email)) = %s
                            """, (phone, client_name, email))

                        # C. Save Survey
                        cur.execute("""
                            INSERT INTO survey_submissions (
                                respondent_email, survey_number, cohort_tag,
                                q1, q2, apply_plan, key_learnings
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            email, 
                            session_id.lower().replace('s', ''), 
                            assigned_tag,
                            request.form.get('quality'),   
                            request.form.get('relevance'), 
                            request.form.get('apply_plan'), 
                            request.form.get('key_learnings')
                        ))
                
                return render_template('download_deck.html', cohort=cohort, session=session_id)

    except Exception as e:
        print(f"Database Error: {e}")
        flash("An error occurred. Please try again.")
    finally:
        db_pool.putconn(conn)

    return render_template('survey_entry.html', cohort=cohort, session=session_id, step=1, user=None, is_guest=True)
    
@app.route('/download/<cohort>/<session_id>')
def download_deck(cohort, session_id):
    # Matches your folder names (e.g., 'monday')
    cohort_folder = cohort.lower().strip()
    
    # Matches your filenames (e.g., 'S1.pptx')
    filename = f"{session_id.upper()}.pptx"
    
    # Construct the path to: static/decks/monday/
    # Using os.path.join is the modern standard
    directory = os.path.join(app.root_path, 'static', 'decks', cohort_folder)
    
    try:
        return send_from_directory(
            directory, 
            filename, 
            as_attachment=True
        )
    except FileNotFoundError:
        flash(f"Resource {filename} not found for {cohort}.")
        return redirect(url_for('dashboard', cohort=cohort))
    
@app.route("/update_comment", methods=["POST"])
def update_comment():
    data = request.json
    id_number = data.get("id_number")
    comment_text = data.get("comment")

    conn = psycopg2.connect(NEON_DATABASE_URL)
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE cohort_candidates 
            SET admin_notes = %s 
            WHERE id_number = %s
        """, (comment_text, id_number))
        conn.commit()
        return {"status": "success"}, 200
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500
    finally:
        cur.close(); conn.close()
        
if __name__ == "__main__":    
    init_db()
    app.run(debug=True)
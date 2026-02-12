import os
import re
import pandas as pd
import io
import psycopg2
import psycopg2.extras
from flask import Flask, render_template, request, redirect, jsonify, url_for, flash, send_from_directory
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from psycopg2.extras import execute_values
from io import StringIO
from psycopg2.pool import ThreadedConnectionPool
from io import BytesIO


app = Flask(__name__)
app.secret_key = "dev-secret-key"

NEON_DATABASE_URL = 'postgresql://neondb_owner:npg_J0LaKIwNbX3o@ep-frosty-hat-ahg0tukc-pooler.c-3.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'



db_pool = ThreadedConnectionPool(
    minconn=1,
    maxconn=10,
    dsn=NEON_DATABASE_URL,
    sslmode="require",
    connect_timeout=10
)

def get_conn():
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
    except psycopg2.OperationalError:
        db_pool.putconn(conn, close=True)
        conn = db_pool.getconn()
    return conn

def release_conn(conn, *, close=False):
    if close:
        db_pool.putconn(conn, close=True)
    else:
        db_pool.putconn(conn)

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

    conn = get_conn()

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
    cur.close() 
    release_db(conn)
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
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, username FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    cur.close()
    release_db(conn)

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
        
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * FROM users WHERE username = %s", (username,))
        user_row = cur.fetchone()
        cur.close()
        release_db(conn)


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
    import pandas as pd
    import io

    filename = file.filename.lower()
    file_bytes = file.read()
    
    if filename.endswith(".csv"):
        try:
            # Teams CSV fallback (Tab separated, UTF-16, skipping the 9-line summary)
            return pd.read_csv(io.BytesIO(file_bytes), encoding="utf-16", sep='\t', skiprows=9)
        except:
            # Standard CSV fallback
            return pd.read_csv(io.BytesIO(file_bytes), encoding="utf-8-sig")

    elif filename.endswith((".xlsx", ".xls")):
        # Excel Teams fallback (skipping the 9-line summary)
        return pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl', skiprows=9)
    
    raise ValueError("Unsupported format.")

@app.route("/api/surveys/<path:email>/<int:num>")
def get_survey_details(email, num):
    conn = get_conn()
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
        cur.close()
        release_db(conn)



@app.route("/api/cohort_analysis/<cohort_name>")
def cohort_analysis(cohort_name):
    conn = get_conn()
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
    cur.close()
    release_db(conn)

    
    # Returns the 'Pulse' of the cohort
    return jsonify({
        "labels": ["Leadership", "Strategy", "Finance", "Marketing", "Operations", "Impact"],
        "averages": [float(x) if x else 0 for x in stats]
    })


@app.route("/")
@login_required
def dashboard():
    conn = None
    cur = None

    try:
        cohort = request.args.get("cohort", "All")
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # Updated JOIN logic inside your dashboard() function
        # Replace the query variable in your dashboard() function with this:
        query = """
            SELECT 
                c.*, 
                COALESCE(
                    ARRAY_AGG(DISTINCT s.survey_number) 
                    FILTER (WHERE s.survey_number IS NOT NULL), 
                    '{}'
                ) AS completed_surveys,
                COALESCE(
                    ARRAY_AGG(DISTINCT a.session_number) 
                    FILTER (WHERE a.session_number IS NOT NULL), 
                    '{}'
                ) AS attended_sessions
            FROM cohort_candidates c
            
            -- Match Surveys by Email OR Name OR Phone
            LEFT JOIN survey_submissions s
            ON (LOWER(TRIM(c.email)) = LOWER(TRIM(s.respondent_email)) AND c.email <> '')
            OR (LOWER(TRIM(c.client)) = LOWER(TRIM(s.respondent_name)))
            OR (TRIM(c.phone) = TRIM(s.respondent_phone) AND c.phone <> '')
            
            -- Match Attendance by Email OR Name OR Phone
            LEFT JOIN attendance_records a
            ON (LOWER(TRIM(c.email)) = LOWER(TRIM(a.email)) AND c.email <> '')
            OR (LOWER(TRIM(c.client)) = LOWER(TRIM(a.name)))
            OR (TRIM(c.phone) = TRIM(a.phone) AND c.phone <> '')
            
            WHERE (%s = 'All' OR c.cohort = %s)
            GROUP BY c.id
            ORDER BY c.client ASC
        """

        # We explicitly join c.email with a.email
        cur.execute(query, (cohort, cohort))
        rows = cur.fetchall()

        attendance_trend = [0] * 6
        attendance_totals = [0] * 6
        avg_stats = {"q1": 0, "q2": 0}
        recent_comments = []

        if cohort != "All":
            # 1. Survey Data (Red Bars)
            cur.execute("""
                SELECT survey_number, COUNT(*) FROM survey_submissions
                WHERE cohort_tag = %s GROUP BY survey_number
            """, (cohort,))
            for survey_num, count in cur.fetchall():
                if 1 <= survey_num <= 6:
                    attendance_trend[survey_num - 1] = count

            # 2. Attendance Data (Blue Bars)
            cur.execute("""
                SELECT session_number, COUNT(*) FROM attendance_records
                WHERE cohort = %s GROUP BY session_number
            """, (cohort,))
            for sess_num, count in cur.fetchall():
                if 1 <= sess_num <= 6:
                    attendance_totals[sess_num - 1] = count

            # 3. Sentiment Stats
            cur.execute("SELECT AVG(q1), AVG(q2) FROM survey_submissions WHERE cohort_tag = %s", (cohort,))
            stat_row = cur.fetchone()
            if stat_row and stat_row[0] is not None:
                avg_stats = {"q1": round(stat_row[0], 1), "q2": round(stat_row[1], 1)}

            # 4. Key Learnings Feed
            cur.execute("""
                SELECT key_learnings, apply_plan FROM survey_submissions
                WHERE cohort_tag = %s ORDER BY submitted_at DESC LIMIT 5
            """, (cohort,))
            recent_comments = cur.fetchall()

        return render_template(
            "dashboard.html",
            rows=rows,
            cohort=cohort,
            attendance_trend=attendance_trend,
            attendance_totals=attendance_totals,
            stats=avg_stats,
            comments=recent_comments
        )

    finally:
        if cur: cur.close()
        if conn: release_db(conn)

def load_tabular_file(file):
    import pandas as pd
    import io

    filename = file.filename.lower()
    file_bytes = file.read()
    
    if filename.endswith(".csv"):
        # We try skipping the first 9 rows to get past the Teams Summary metadata
        try:
            # Try reading with skiproms=9 for Teams formats
            return pd.read_csv(io.BytesIO(file_bytes), encoding="utf-16", sep='\t', skiprows=9)
        except Exception:
            try:
                # Fallback for standard CSVs if skiprows fails
                return pd.read_csv(io.BytesIO(file_bytes), encoding="utf-8-sig")
            except Exception as e:
                raise ValueError(f"Could not parse CSV: {e}")

    elif filename.endswith((".xlsx", ".xls")):
        # For Excel files, we also skip the first 9 rows of metadata
        return pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl', skiprows=9)
    
    raise ValueError("Unsupported file format.")

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

def ultimate_id_fix(val):
    """Handles scientific notation with both dots and commas."""
    if not val or str(val).lower() in ['nan', 'none', '']:
        return None
    
    # Convert to string and clean spaces
    val = str(val).strip()
    
    # THE FIX: Replace comma with dot for scientific notation (8,70E+12 -> 8.70E+12)
    val = val.replace(',', '.')
    
    try:
        num_val = float(val)
        clean_val = '{:.0f}'.format(num_val)
        return clean_val if len(clean_val) > 5 else val
    except:
        return val


@app.route("/upload_attendance/<int:session_num>", methods=["POST"])
def upload_attendance(session_num):
    cohort = request.args.get('cohort')
    file = request.files.get('attendance_file')
    
    if not file:
        flash("No file selected.")
        return redirect(url_for('dashboard', cohort=cohort))

    try:
        # 1. Load file with Teams-specific logic (skipping metadata)
        df = load_tabular_file(file) 
        df.columns = [str(c).strip().lower() for c in df.columns]
        
        name_col = next((c for c in df.columns if 'name' in c), None)
        email_col = next((c for c in df.columns if 'email' in c or 'principal' in c), None)
        phone_col = next((c for c in df.columns if any(p in c for p in ['phone', 'contact', 'mobile'])), None)

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("SELECT client, email, phone FROM cohort_candidates WHERE cohort = %s", (cohort,))
        db_candidates = cur.fetchall()

        matched_count = 0
        
        for _, row in df.iterrows():
            up_name = str(row.get(name_col, '')).strip().lower()
            up_email = str(row.get(email_col, '')).strip().lower() if email_col else ""
            up_phone = ''.join(filter(str.isdigit, str(row.get(phone_col, ''))))

            if not up_name and not up_email and not up_phone:
                continue

            for cand in db_candidates:
                db_name = str(cand['client']).strip().lower()
                db_email = str(cand['email']).strip().lower() if cand['email'] else ""
                db_phone = ''.join(filter(str.isdigit, str(cand['phone']))) if cand['phone'] else ""
                
                is_match = False
                if up_email and up_email == db_email: is_match = True
                elif up_name and up_name == db_name: is_match = True
                elif up_phone and up_phone == db_phone: is_match = True

                if is_match:
                    # CORRECT TABLE: attendance_records
                    # We save name/phone/email so the Dashboard JOIN can find it
                    cur.execute("""
                        INSERT INTO attendance_records 
                        (email, name, phone, session_number, cohort)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (cand['email'], cand['client'], cand['phone'], session_num, cohort))
                    matched_count += 1
                    break 

        conn.commit()
        cur.close()
        release_db(conn)
        
        flash(f"Attendance recorded for {matched_count} participants.")

    except Exception as e:
        flash(f"Error: {str(e)}")
    
    return redirect(url_for('dashboard', cohort=cohort))

@app.route('/survey/<cohort>/<session_id>', methods=['GET', 'POST'])
def handle_survey(cohort, session_id):
    conn = db_pool.getconn()
    try:
        if request.method == 'POST':
            step = request.form.get('step')

            if step == '1':
                
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

    conn = get_conn()
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
        cur.close()
        release_db(conn)


if __name__ == "__main__":    
    app.run(debug=False)

from flask import Flask, render_template, redirect, jsonify, url_for, flash, send_from_directory, session, request
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool
from io import StringIO, BytesIO
import os
import re
import uuid
import pandas as pd
import io
import psycopg2
import psycopg2.extras


import datetime


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

        # 1. Main Table Query (Kept exactly as is to preserve Dots logic)
        # 1. Main Table Query (Updated for Case-Insensitivity and Admin Notes)
        query = """
            SELECT 
                c.*, 
                COALESCE(ARRAY_AGG(DISTINCT s.survey_number) FILTER (WHERE s.survey_number IS NOT NULL), '{}') AS completed_surveys,
                COALESCE(ARRAY_AGG(DISTINCT a.session_number) FILTER (WHERE a.session_number IS NOT NULL), '{}') AS attended_sessions
            FROM cohort_candidates c
            LEFT JOIN survey_submissions s ON 
                (LOWER(TRIM(c.email)) = LOWER(TRIM(s.respondent_email)) AND c.email <> '') OR 
                (LOWER(TRIM(c.client)) = LOWER(TRIM(s.respondent_name))) OR 
                (TRIM(c.phone) = TRIM(s.respondent_phone) AND c.phone <> '')
            LEFT JOIN attendance_records a ON 
                (LOWER(TRIM(c.email)) = LOWER(TRIM(a.email)) AND c.email <> '') OR 
                (LOWER(TRIM(c.client)) = LOWER(TRIM(a.name))) OR 
                (TRIM(c.phone) = TRIM(a.phone) AND c.phone <> '')
            WHERE (%s = 'All' OR LOWER(TRIM(c.cohort)) = LOWER(TRIM(%s)))
            GROUP BY c.id ORDER BY c.client ASC
        """
        cur.execute(query, (cohort, cohort))
        rows = cur.fetchall()

        # 2. Get Chart Totals and Summary Count (Fixed the Unpacking Error here)
        attendance_totals, total_external_attendees = get_attendance_stats(cohort)

        # Initialize defaults for visuals
        attendance_trend = [0] * 6
        avg_stats = {"q1": 0, "q2": 0}
        recent_comments = []

        if cohort != "All":
            # 3. Survey Data (Red Bars)
            cur.execute("""
                SELECT survey_number, COUNT(DISTINCT respondent_email) FROM survey_submissions
                WHERE cohort_tag = %s GROUP BY survey_number
            """, (cohort,))
            for survey_num, count in cur.fetchall():
                if 1 <= survey_num <= 6:
                    attendance_trend[survey_num - 1] = count

            # 4. Sentiment & Comments
            cur.execute("SELECT AVG(q1), AVG(q2) FROM survey_submissions WHERE cohort_tag = %s", (cohort,))
            stat_row = cur.fetchone()
            if stat_row and stat_row[0] is not None:
                avg_stats = {"q1": round(stat_row[0], 1), "q2": round(stat_row[1], 1)}

            cur.execute("""
                SELECT key_learnings, apply_plan FROM survey_submissions
                WHERE cohort_tag = %s ORDER BY submitted_at DESC LIMIT 5
            """, (cohort,))
            recent_comments = cur.fetchall()

        return render_template(
            "dashboard.html",
            rows=rows,
            cohort=cohort,
            total_attendees=total_external_attendees, 
            attendance_trend=attendance_trend,
            attendance_totals=attendance_totals, # This now contains your 30+ headcount
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



@app.route("/upload/<cohort_context>", methods=["POST"])
@login_required
def upload(cohort_context):
    conn = None 
    file = request.files.get("file")
    if not file:
        return redirect(url_for("dashboard", cohort=cohort_context))

    # Create a unique ID for this specific upload session (e.g., 20260216_1130)
    batch_id = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

    try:
        df = load_tabular_file(file)
        conn = get_conn()
        cur = conn.cursor()

        for _, row in df.iterrows():
            row_dict = row.to_dict()
            
            # --- DEFINE THESE VARIABLES FIRST ---
            name_val = get_clean_row_val(row_dict, ["Client Name", "Name", "Full Name"])
            phone_val = get_clean_row_val(row_dict, ["Phone", "Mobile"])
            tier_val = get_clean_row_val(row_dict, ["Tier"])
            id_val = ultimate_id_fix(get_clean_row_val(row_dict, ["ID Number", "Identity", "ID"]))
            email_val = get_clean_row_val(row_dict, ["Email", "Email Address"]).lower().strip()
            
            if not id_val: continue

            raw_source = get_clean_row_val(row_dict, ["source"])
            m_digit = re.search(r"\d", str(raw_source))
            assigned_day = MENTORSHIP_MAP.get(m_digit.group(0) if m_digit else None, cohort_context)

            cur.execute("""
                INSERT INTO cohort_candidates (cohort, client, email, phone, id_number, tier, source, last_upload_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (cohort, id_number) DO UPDATE SET
                    -- 1. Always update the Name if provided (to keep it fresh)
                    client = COALESCE(NULLIF(EXCLUDED.client, ''), cohort_candidates.client),
                    
                    -- 2. "Smart Fill" Email: Only update if current is empty AND new is NOT empty
                    email = CASE 
                        WHEN (cohort_candidates.email IS NULL OR cohort_candidates.email = '' OR cohort_candidates.email = 'pending@oaktree.co.za') 
                        THEN COALESCE(NULLIF(EXCLUDED.email, ''), cohort_candidates.email)
                        ELSE cohort_candidates.email 
                    END,

                    -- 3. "Smart Fill" Phone: Only update if current is empty/NA AND new is NOT empty
                    phone = CASE 
                        WHEN (cohort_candidates.phone IS NULL OR cohort_candidates.phone = '' OR cohort_candidates.phone = 'N/A') 
                        THEN COALESCE(NULLIF(EXCLUDED.phone, ''), cohort_candidates.phone)
                        ELSE cohort_candidates.phone 
                    END,

                    -- 4. Track this batch for the Revert button
                    prev_data = CASE 
                        WHEN cohort_candidates.last_upload_id = EXCLUDED.last_upload_id THEN cohort_candidates.prev_data
                        ELSE jsonb_build_object(
                            'client', cohort_candidates.client,
                            'email', cohort_candidates.email,
                            'phone', cohort_candidates.phone
                        )
                    END,
                    last_upload_id = EXCLUDED.last_upload_id
            """, (assigned_day, name_val, email_val, phone_val, id_val, tier_val, raw_source, batch_id))

        conn.commit()
        flash(f"Successfully imported. | Batch ID: {batch_id}", "success")
        # Store batch_id in session so the Revert button knows which one to undo
        session['last_batch_id'] = batch_id 

    except Exception as e:
        if conn: conn.rollback()
        flash(f"Upload error: {str(e)}", "danger")
    finally:
        if conn: release_db(conn)
        
    return redirect(url_for("dashboard", cohort=cohort_context))

@app.route("/revert_last_upload")
@login_required
def revert_last_upload():
    batch_id = session.get('last_batch_id')
    if not batch_id:
        flash("No recent upload found to revert.", "warning")
        return redirect(url_for("dashboard", cohort='All'))

    conn = get_conn()
    cur = conn.cursor()
    try:
        # 1. Restore rows that were overwritten (Restores client, email, phone from JSON)
        cur.execute("""
            UPDATE cohort_candidates 
            SET 
                client = prev_data->>'client',
                email = prev_data->>'email',
                phone = prev_data->>'phone',
                prev_data = NULL,
                last_upload_id = NULL
            WHERE last_upload_id = %s AND prev_data IS NOT NULL
        """, (batch_id,))

        # 2. Delete rows that were newly created in this batch
        cur.execute("""
            DELETE FROM cohort_candidates 
            WHERE last_upload_id = %s AND prev_data IS NULL
        """, (batch_id,))

        conn.commit()
        session.pop('last_batch_id', None) # Clear from session
        flash("Revert Successful: Database restored to previous state.", "success")
    except Exception as e:
        if conn: conn.rollback()
        flash(f"Revert failed: {str(e)}", "danger")
    finally:
        release_db(conn)
    return redirect(url_for("dashboard", cohort='All'))

@app.route("/dismiss_revert")
@login_required
def dismiss_revert():
    session.pop('last_batch_id', None)
    # Redirect back to the cohort they were viewing
    return redirect(request.referrer or url_for("dashboard", cohort='All'))

@app.route("/upload_attendance/<int:session_num>", methods=["POST"])
def upload_attendance(session_num):
    cohort = request.args.get('cohort')
    file = request.files.get('attendance_file')
    
    if not file: return redirect(url_for('dashboard', cohort=cohort))

    try:
        df = load_tabular_file(file) 
        df.columns = [str(c).strip().lower() for c in df.columns]
        
        name_col = next((c for c in df.columns if 'name' in c), None)
        email_col = next((c for c in df.columns if 'email' in c), None)

        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("SELECT client, email, phone FROM cohort_candidates WHERE cohort = %s", (cohort,))
        db_candidates = cur.fetchall()

        raw_headcount = 0
        matched_count = 0
        # NEW: Track unique names seen in this specific file
        seen_names = set()
        
        for _, row in df.iterrows():
            up_name = str(row.get(name_col, '')).strip()
            up_email = str(row.get(email_col, '')).strip().lower() if email_col else ""

            # 1. Standard Filters
            if not up_name or up_name.lower() == 'nan': continue
            if "@oaktreepeople.co.za" in up_email: continue
            if any(bot in up_name.lower() for bot in ['notetaker', 'read.ai', 'ai meeting']): continue

            # 2. UNIQUE CHECK: If we've already counted this name in this upload, skip it
            if up_name.lower() in seen_names:
                continue
            
            seen_names.add(up_name.lower())
            raw_headcount += 1

            # 3. Match logic for the Blue Dots (attendance_records)
            for cand in db_candidates:
                if (up_email and up_email == cand['email']) or (up_name.lower() == cand['client'].lower()):
                    cur.execute("""
                        INSERT INTO attendance_records (email, name, phone, session_number, cohort)
                        VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                    """, (cand['email'], cand['client'], cand['phone'], session_num, cohort))
                    matched_count += 1
                    break 

        # Save the deduplicated raw headcount
        cur.execute("""
            INSERT INTO session_metadata (cohort, session_number, raw_headcount)
            VALUES (%s, %s, %s)
            ON CONFLICT (cohort, session_number) DO UPDATE SET raw_headcount = EXCLUDED.raw_headcount
        """, (cohort, session_num, raw_headcount))

        conn.commit()
        cur.close()
        release_db(conn)
        flash(f"De-duplicated: {raw_headcount} unique external attendees found.")

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

def get_attendance_stats(cohort_name):
    conn = get_conn()
    cur = conn.cursor()
    
    # Chart Data (The Blue Bars - looking at the 30+ headcount)
    chart_series = [0] * 6
    cur.execute("""
        SELECT session_number, raw_headcount 
        FROM session_metadata 
        WHERE (%s = 'All' OR cohort = %s)
    """, (cohort_name, cohort_name))
    
    for sess_num, count in cur.fetchall():
        if 1 <= sess_num <= 6:
            chart_series[sess_num - 1] = count

    # Stat Card (The Summary Number - only counting matched people)
    cur.execute("""
        SELECT COUNT(DISTINCT a.email) 
        FROM attendance_records a
        JOIN cohort_candidates c ON (
            (LOWER(TRIM(a.email)) = LOWER(TRIM(c.email)) AND a.email <> '') OR 
            (LOWER(TRIM(a.name)) = LOWER(TRIM(c.client)))
        )
        WHERE (%s = 'All' OR a.cohort = %s)
    """, (cohort_name, cohort_name))
    total_matched = cur.fetchone()[0] or 0
    
    cur.close()
    release_db(conn)
    
    # Returns exactly 2 items to match the "unpacking" in the dashboard route
    return chart_series, total_matched

@app.route("/update_comment", methods=["POST"])
def update_comment():
    data = request.json
    id_number = data.get("id_number")
    comment_text = data.get("comment")

    conn = get_conn()
    cur = conn.cursor()
    try:
        # Saving to admin_notes to match your current table structure
        cur.execute("""
            UPDATE cohort_candidates 
            SET admin_notes = %s 
            WHERE id_number = %s
        """, (comment_text, id_number))
        conn.commit()
        print(f"Comment saved for ID {id_number}") 
        return {"status": "success"}, 200
    except Exception as e:
        print(f"Database Error: {str(e)}")
        return {"status": "error", "message": str(e)}, 500
    finally:
        cur.close()
        release_db(conn)


@app.route("/webinar")
def webinar_landing():
    """Step 1: The Landing Page"""
    return render_template("webinar_landing.html")

@app.route("/webinar/register-form")
def webinar_form():
    """Step 2: The Registration Form"""
    return render_template("webinar_form.html")


WHATSAPP_LINKS = {
    "Guest":"https://chat.whatsapp.com/FHlfIbAkwaQ8nNnlhYzPUB?mode=gi_c"
    }

@app.route("/webinar/process", methods=["POST"])
def webinar_process():
    # 1. Capture Form Data
    first_name = request.form.get("name")
    surname = request.form.get("surname")
    email = request.form.get("email").lower().strip()
    company_name = request.form.get("company") # This will go into 'client'
    phone = request.form.get("phone")
    
    # Format how you want it to look in your dashboard
    # Example: "John Doe (Oaktree Labs)"
    display_name = f"{first_name} {surname} ({company_name})"

    # 2. Determine Cohort based on today
    import datetime
    current_day = datetime.datetime.now().strftime('%A')
    
    assigned_cohort = current_day if current_day in WHATSAPP_LINKS else "Guest"
    
    # 3. Generate Temp ID for Primary Key
    temp_id = f"GUEST-{uuid.uuid4().hex[:8].upper()}"

    conn = get_conn()
    cur = conn.cursor()
    try:
        # We mapped company_name to 'client' here to fix the SQL Error
        cur.execute("""
            INSERT INTO cohort_candidates (cohort, client, email, phone, id_number, source)
            VALUES (%s, %s, %s, %s, %s, 'Webinar Registration')
            ON CONFLICT (email) DO UPDATE SET
                client = COALESCE(NULLIF(EXCLUDED.client, ''), cohort_candidates.client),
                phone = COALESCE(NULLIF(EXCLUDED.phone, ''), cohort_candidates.phone),
                cohort = EXCLUDED.cohort
        """, (assigned_cohort, display_name, email, phone, temp_id))
        
        conn.commit()
        session['reg_cohort'] = assigned_cohort
        session['reg_wa'] = WHATSAPP_LINKS.get(assigned_cohort)
        
        return redirect(url_for('webinar_success'))

    except Exception as e:
        if conn: conn.rollback()
        print(f"SQL Error: {e}") # This helps you debug in the terminal
        flash(f"Registration Error: {str(e)}", "danger")
        return redirect(url_for('webinar_landing'))
    finally:
        cur.close()
        release_db(conn)

@app.route("/webinar/success")
def webinar_success():
    cohort = session.get('reg_cohort', 'Guest')
    wa_url = session.get('reg_wa', WHATSAPP_LINKS["Guest"])

    from datetime import datetime

    # 🔹 Webinar Details
    event_title = "Oaktree Business Growth Webinar"
    event_description = "Join us live on Microsoft Teams."
    teams_link = "https://teams.microsoft.com/l/meetup-join/19%3ameeting_ODBhNDEzZDMtZjc1MC00OTA3LWJjODItZGY1MTdjMjg1NmZh%40thread.v2/0?context=%7b%22Tid%22%3a%22784c8a5b-84c1-4c3e-904d-effa70174769%22%2c%22Oid%22%3a%221741f59d-5bba-4ccc-9f76-bb9eff953c95%22%7d"

    # 🔹 SA Time 17:30 → Convert to UTC (15:30)
    start = datetime(2026, 2, 18, 15, 30)
    end = datetime(2026, 2, 18, 17, 0)

    start_str = start.strftime("%Y%m%dT%H%M%SZ")
    end_str = end.strftime("%Y%m%dT%H%M%SZ")

    google_calendar_link = (
        "https://calendar.google.com/calendar/render?action=TEMPLATE"
        f"&text={event_title}"
        f"&dates={start_str}/{end_str}"
        f"&details={event_description}%0A%0AJoin%20here:%20{teams_link}"
        f"&location={teams_link}"
        "&sf=true&output=xml"
    )

    return render_template(
        "registration_success.html",
        cohort=cohort,
        whatsapp_url=wa_url,
        calendar_link=google_calendar_link
    )

if __name__ == "__main__":    
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
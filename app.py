
from flask import Flask, render_template, request, redirect, jsonify, url_for, flash
import pandas as pd
import psycopg2
import camelot
import os
import re
from psycopg2.extras import execute_values
from io import StringIO

app = Flask(__name__)
app.secret_key = "dev-secret-key"

NEON_DATABASE_URL = 'postgresql://neondb_owner:npg_J0LaKIwNbX3o@ep-frosty-hat-ahg0tukc-pooler.c-3.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'

# 1. MAPPING LOGIC: Converts "Mentorship X" to the correct Day
MENTORSHIP_MAP = {
    "1": "Monday",
    "2": "Tuesday",
    "3": "Wednesday",
    "4": "Thursday"
}
def init_db():
    conn = psycopg2.connect(NEON_DATABASE_URL)
    cur = conn.cursor()

    # 1. Recreate the table with ALL necessary columns
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

    # 2. Create the Survey Submissions table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS survey_submissions (
            id SERIAL PRIMARY KEY,
            respondent_email TEXT,
            survey_number INTEGER,
            cohort_tag TEXT,  -- <--- Make sure this matches
            q1 INTEGER, q2 INTEGER, q3 INTEGER, q4 INTEGER,
            apply_plan TEXT,
            key_learnings TEXT,
            submitted_at TIMESTAMP DEFAULT NOW()
        );
    """)

    conn.commit()
    cur.close()
    conn.close()

# ==========================
# FILE LOADER (CSV + EXCEL)
# ==========================

def load_tabular_file(file):
    filename = file.filename.lower()

    if filename.endswith(".csv"):
        return pd.read_csv(file, encoding="utf-8-sig")

    elif filename.endswith(".xlsx") or filename.endswith(".xls"):
        return pd.read_excel(file)

    else:
        raise ValueError("Unsupported file format")

#     return render_template("dashboard.html", cohort=cohort, rows=rows)
@app.route("/api/surveys/<path:email>/<int:num>")
def get_survey_details(email, num):
    conn = psycopg2.connect(NEON_DATABASE_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    # Force lowercase on the email parameter
    clean_email = email.lower().strip()
    
    try:
        # Use LOWER() in the SQL to ensure a perfect match
        cur.execute("""
            SELECT q1, q2, q3, q4, apply_plan, key_learnings 
            FROM survey_submissions 
            WHERE LOWER(respondent_email) = %s 
            AND survey_number = %s
        """, (clean_email, num))
        
        res = cur.fetchone()
        
        if res:
            return jsonify({
                "scores": [res['q1'], res['q2'], res['q3'], res['q4']],
                "feedback": {
                    "apply_plan": res['apply_plan'],
                    "key_learnings": res['key_learnings']
                }
            })
        
        # Return empty data if not found, but with a 200 status so the JS doesn't error
        return jsonify({"scores": [0,0,0,0], "feedback": None}), 200
            
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
def dashboard():
    cohort = request.args.get("cohort", "All")
    conn = psycopg2.connect(NEON_DATABASE_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # 1. UNIFIED QUERY
    # This now handles 'All', 'Monday', 'Tuesday', etc., AND 'Guest' 
    # because guests are now saved in cohort_candidates table.
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
    avg_stats = {"q1": 0, "q2": 0, "q3": 0, "q4": 0}
    recent_comments = []

    if cohort != "All":
        # Get counts for Attendance Trend (Works for Monday, Guest, etc.)
        cur.execute("""
            SELECT survey_number, COUNT(*) 
            FROM survey_submissions 
            WHERE cohort_tag = %s GROUP BY survey_number
        """, (cohort,))
        for count_row in cur.fetchall():
            if 1 <= count_row[0] <= 6:
                attendance_trend[count_row[0]-1] = count_row[1]

        # Get Averages for the 4 Metrics
        cur.execute("""
            SELECT AVG(q1), AVG(q2), AVG(q3), AVG(q4) 
            FROM survey_submissions WHERE cohort_tag = %s
        """, (cohort,))
        stat_row = cur.fetchone()
        if stat_row and stat_row[0] is not None:
            avg_stats = {"q1": round(stat_row[0], 1), "q2": round(stat_row[1], 1), 
                         "q3": round(stat_row[2], 1), "q4": round(stat_row[3], 1)}

        # Get Recent Key Learnings
        cur.execute("""
            SELECT key_learnings, apply_plan FROM survey_submissions 
            WHERE cohort_tag = %s ORDER BY submitted_at DESC LIMIT 5
        """, (cohort,))
        recent_comments = cur.fetchall()
    
    cur.close(); conn.close()
    return render_template("dashboard.html", rows=rows, cohort=cohort, 
                           attendance_trend=attendance_trend, stats=avg_stats, comments=recent_comments)# ==========================
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
        # LOAD AS STRINGS: This is the primary fix for Scientific Numbers
        if file.filename.lower().endswith(".csv"):
            raw = file.read().decode("utf-8", errors="ignore")
            df = pd.read_csv(StringIO(raw), sep=None, engine='python', dtype=str)
        else:
            # Using dtype=str prevents Pandas from guessing 'number' types
            df = pd.read_excel(file, engine="openpyxl", dtype=str)

        df.columns = [str(c).strip().lower() for c in df.columns]
        df = df.dropna(how='all')

        data_to_insert = []

        for _, row in df.iterrows():
            row_dict = row.to_dict()

            # A. COHORT MAPPING
            raw_source = get_clean_row_val(row_dict, ["source"])
            digit_match = re.search(r'\d', str(raw_source))
            m_digit = digit_match.group(0) if digit_match else None
            assigned_day = MENTORSHIP_MAP.get(m_digit, "Unassigned")

            # B. ID NUMBER HANDLING
            id_num = get_clean_row_val(row_dict, ["id number", "id_number", "id"])
            if not id_num or id_num.lower() == "id number":
                continue

            data_to_insert.append((
                assigned_day,
                get_clean_row_val(row_dict, ["client"]),
                get_clean_row_val(row_dict, ["primary contact", "primary cont"]),
                get_clean_row_val(row_dict, ["email"]),
                get_clean_row_val(row_dict, ["phone", "cell"]),
                id_num, # Now cleaned of scientific notation
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
                ON CONFLICT (cohort, id_number) DO UPDATE SET
                    client = EXCLUDED.client,
                    primary_contact = EXCLUDED.primary_contact,
                    email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    cipc_number = EXCLUDED.cipc_number,
                    tier = EXCLUDED.tier,
                    source = EXCLUDED.source,
                    comment = EXCLUDED.comment
            """, data_to_insert)

        conn.commit()
        flash(f"Sync Complete: {len(data_to_insert)} candidates sorted.")

    except Exception as e:
        conn.rollback()
        flash(f"Error: {str(e)}")
    finally:
        cur.close(); conn.close()

    return redirect(url_for("dashboard", cohort=cohort_context))

@app.route('/survey/<cohort>/<session_id>', methods=['GET', 'POST'])
def handle_survey(cohort, session_id):
    conn = psycopg2.connect(NEON_DATABASE_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    if request.method == 'POST':
        step = request.form.get('step')

        if step == '1':
            email = request.form.get('email', '').strip().lower()
            name = request.form.get('name', '').strip()
            phone = request.form.get('phone', '').strip()
            
            cur.execute("SELECT client, email FROM cohort_candidates WHERE TRIM(LOWER(email)) = %s", (email,))
            candidate = cur.fetchone()
            
            # If guest, prepare data to be inserted later
            user_data = {
                "client": candidate['client'] if candidate else name,
                "email": email,
                "phone": phone
            }
            is_guest = False if candidate else True

            cur.close(); conn.close()
            return render_template('survey_entry.html', cohort=cohort, session=session_id, 
                                 step=2, user=user_data, is_guest=is_guest)

        elif step == '2':
            email = request.form.get('verified_email', '').strip().lower()
            client_name = request.form.get('verified_name', '').strip()
            phone = request.form.get('verified_phone', '').strip()

            # 1. Check if we need to 'Register' this guest in the candidates table
            cur.execute("SELECT cohort FROM cohort_candidates WHERE TRIM(LOWER(email)) = %s", (email,))
            candidate = cur.fetchone()
            
            if not candidate:
                assigned_tag = "Guest"
                cur.execute("""
                    INSERT INTO cohort_candidates (cohort, client, email, phone, id_number)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (assigned_tag, client_name, email, phone, f"GUEST-{email}"))
            else:
                assigned_tag = candidate['cohort']

            # 2. Save Survey
            try:
                cur.execute("""
                    INSERT INTO survey_submissions (
                        respondent_email, survey_number, cohort_tag,
                        q1, q2, q3, q4, apply_plan, key_learnings
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    email, session_id.lower().replace('s', ''), assigned_tag,
                    request.form.get('quality'), request.form.get('relevance'), 5, 5,
                    request.form.get('apply_plan'), request.form.get('key_learnings')
                ))
                conn.commit()
            finally:
                cur.close(); conn.close()
            
            return render_template('download_deck.html', cohort=cohort, session=session_id)

    cur.close(); conn.close()
    return render_template('survey_entry.html', cohort=cohort, session=session_id, step=1, user=None, is_guest=True)

 
@app.route("/clear/<cohort>")
def clear_cohort(cohort):
    conn = psycopg2.connect(NEON_DATABASE_URL)
    cur = conn.cursor()
    cur.execute("DELETE FROM cohort_candidates WHERE cohort = %s", (cohort,))
    conn.commit()
    cur.close()
    conn.close()
    flash(f"Cleared all records for {cohort}")
    return redirect(url_for("dashboard", cohort=cohort))

def reset_db_once():
    conn = psycopg2.connect(NEON_DATABASE_URL)
    cur = conn.cursor()
    print("Dropping old tables to refresh schema...")
    # Add the survey table to the drop list
    cur.execute("DROP TABLE IF EXISTS cohort_candidates CASCADE;")
    cur.execute("DROP TABLE IF EXISTS survey_submissions CASCADE;") 
    conn.commit()
    cur.close()
    conn.close()
    print("Tables dropped. Recreating with 'cohort_tag'...")
import psycopg2

def seed_dummy_data():
    emails = [
    "bshoba@medsac.co.za"
    "Info.blackstone@outlookl.co.za"

    "dibategladys@gmail.com"
    "hlamalani@sidaconsulting.com"
    "Henterpriseptyltd@gmail.com"
    "jackson@khula.co.za"
    "Enymasilela@gmail.com"
    "katlegomatabane55@gmail.com"
    "kelebogileramosunya@gmail.com"
    "kgalalelo@bbud.co.za"
    "dimezaprojects@gmail.com"
    "selo.setsha@gmail.com"
    "mapsrams1@gmail.com"
    "smart1bontle@gmail.com"
    "ndoumasindi@gmail.com"
    "modisaotsile@Carngocleaning.co.za"

    "Zarfuwi@gmail.com",
    "nana@Nkamathebula.co.za",
    "neal@kgb.co.za",
    "naidoonirvasha@gmail.com"]
    
    conn = psycopg2.connect(NEON_DATABASE_URL)
    cur = conn.cursor()
    
    # Generate data for Sessions 1 through 3 for each email
    for email in emails:
        for s_num in range(1, 4):  # S1, S2, S3
            cur.execute("""
                INSERT INTO survey_submissions (
                    respondent_email, survey_number, cohort_tag,
                    q1, q2, q3, q4, apply_plan, key_learnings
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                email, s_num, "Monday",
                4, 5, 4, 3, # Dummy scores
                f"Implementing the S{s_num} strategy into my daily operations.",
                f"Learned about high-level scaling and S{s_num} efficiency."
            ))
    
    conn.commit()
    cur.close(); conn.close()
    print("Dummy data seeded for Monday cohort!")

if __name__ == "__main__":
    # 1. UNCOMMENT the line below. 
    # 2. RUN the app (it will delete the table and start). 
    # 3. STOP the app and COMMENT the line out again.
    
    # reset_db_once() 
    
    init_db()
    seed_dummy_data()
    app.run(debug=True)
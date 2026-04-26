from flask import Flask, request, render_template_string, send_file, redirect, session
import duckdb
import pandas as pd
import os
import uuid
from datetime import datetime

app = Flask(__name__)
app.secret_key = "rahasia_ff_backtester_2024"

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

CURRENT_PRICE_FILE = {"path": None, "name": None}
CURRENT_FF_FILE = {"path": None, "name": None}

# Default parameter (hanya untuk inisialisasi pertama kali)
DEFAULT_PARAMS = {
    "anchor_date": datetime.today().strftime("%Y-%m-%d"),
    "my_budget": "1000000",
    "alpha": "30",
    "x": "-99.8047",
    "unit_size": "100"
}

# ============================================================
# HTML
# ============================================================

HTML = """
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
body { font-family: Arial; margin: 20px; }
h2 { text-align: center; }
.form-box { display: flex; flex-direction: column; align-items: center; }

/* file status */
.file-status {
    display: flex;
    justify-content: center;
    gap: 30px;
    margin-bottom: 20px;
    flex-wrap: wrap;
}
.file-card {
    border: 1px solid #ccc;
    border-radius: 8px;
    padding: 10px 20px;
    background-color: #f9f9f9;
    min-width: 250px;
}
.file-card b { color: #2c3e50; }
.file-card .active { color: green; font-weight: bold; }
.file-card .inactive { color: red; }

/* upload box */
.upload-box {
    border: 2px dashed #aaa;
    padding: 30px;
    text-align: center;
    width: 320px;
    margin: 10px;
}
.upload-container {
    display: flex;
    justify-content: center;
    gap: 30px;
    flex-wrap: wrap;
}

/* parameter grid */
.param-grid {
    display: grid;
    grid-template-columns: 200px 200px;
    gap: 10px 20px;
    margin-bottom: 20px;
}
.big-button {
    font-size: 18px;
    padding: 10px 30px;
    cursor: pointer;
}
.upload-button {
    font-size: 16px;
    padding: 10px 30px;
    cursor: pointer;
}
.section-title {
    text-align: center;
    margin: 20px 0 10px;
    font-weight: bold;
}

/* table */
.table-container {
    width: 100%;
    overflow-x: auto;
}
table {
    border-collapse: collapse;
    width: 100%;
    min-width: 1400px;
}
th, td {
    border: 1px solid #ccc;
    padding: 6px;
    text-align: right;
    white-space: nowrap;
}
th {
    background-color: #f2f2f2;
    text-align: center;
    position: sticky;
    top: 0;
}
td:nth-child(2) {
    text-align: left;
}
tr:hover { background-color: #f5f5f5; }
.total-row td {
    background-color: #d9edf7 !important;
    font-weight: bold;
}
.cagr-row td {
    background-color: #dff0d8 !important;
    font-weight: bold;
}
.action-buttons {
    display: flex;
    justify-content: center;
    gap: 20px;
    margin-top: 20px;
}
.remove-button {
    background-color: #dc3545;
    color: white;
    border: none;
    padding: 5px 15px;
    cursor: pointer;
    border-radius: 4px;
}
.run-button {
    background-color: #28a745;
    color: white;
    border: none;
    padding: 10px 30px;
    cursor: pointer;
    border-radius: 4px;
    font-size: 18px;
}
.reset-params-link {
    margin-left: 20px;
}
.reset-params-link a {
    color: #f0ad4e;
    text-decoration: none;
    font-size: 12px;
}
</style>
</head>
<body>

<h2>📉 Free Float Backtester</h2>

<!-- File Status -->
<div class="file-status">
    <div class="file-card">
        <b>📊 Price History:</b> 
        {% if price_file %}
            <span class="active">✓ {{ price_file }}</span>
            <form method="post" style="display:inline;">
                <input type="hidden" name="action" value="remove_price">
                <button type="submit" class="remove-button" style="margin-left:10px;">✗</button>
            </form>
        {% else %}
            <span class="inactive">❌ belum upload</span>
        {% endif %}
    </div>
    <div class="file-card">
        <b>💾 Free Float DB:</b> 
        {% if ff_file %}
            <span class="active">✓ {{ ff_file }}</span>
            <form method="post" style="display:inline;">
                <input type="hidden" name="action" value="remove_ff">
                <button type="submit" class="remove-button" style="margin-left:10px;">✗</button>
            </form>
        {% else %}
            <span class="inactive">❌ belum upload</span>
        {% endif %}
    </div>
</div>

{% if not price_file or not ff_file %}
<div class="section-title">📂 Upload File</div>
<div class="upload-container">
    <form method="post" enctype="multipart/form-data" class="upload-box">
        <input type="hidden" name="action" value="upload_price">
        <p><b>Price History (.parquet)</b></p>
        <input type="file" name="file" accept=".parquet" required><br><br>
        <button type="submit" class="upload-button">📂 Upload Price</button>
    </form>

    <form method="post" enctype="multipart/form-data" class="upload-box">
        <input type="hidden" name="action" value="upload_ff">
        <p><b>Free Float (.db)</b></p>
        <input type="file" name="file" accept=".db" required><br><br>
        <button type="submit" class="upload-button">📂 Upload Free Float</button>
    </form>
</div>
{% endif %}

{% if price_file and ff_file %}
<hr>
<form method="post" class="form-box">
<input type="hidden" name="action" value="run">

<div class="param-grid">
    <div>📅 Anchor Date:</div>
    <input name="anchor_date" value="{{ params.anchor_date }}" type="date">

    <div>💰 Budget (Rp):</div>
    <input name="my_budget" value="{{ params.my_budget }}" step="100000" type="number">

    <div>🔢 Alpha (1-30):</div>
    <input name="alpha" value="{{ params.alpha }}" min="1" max="30" type="number">

    <div>📉 Threshold X (%):</div>
    <select name="x">
        <option value="-75.00" {{ 'selected' if params.x == '-75.00' else '' }}>-75.00%</option>
        <option value="-87.50" {{ 'selected' if params.x == '-87.50' else '' }}>-87.50%</option>
        <option value="-93.75" {{ 'selected' if params.x == '-93.75' else '' }}>-93.75%</option>
        <option value="-96.875" {{ 'selected' if params.x == '-96.875' else '' }}>-96.875%</option>
        <option value="-98.4375" {{ 'selected' if params.x == '-98.4375' else '' }}>-98.4375%</option>
        <option value="-99.2188" {{ 'selected' if params.x == '-99.2188' else '' }}>-99.2188%</option>
        <option value="-99.6094" {{ 'selected' if params.x == '-99.6094' else '' }}>-99.6094%</option>
        <option value="-99.8047" {{ 'selected' if params.x == '-99.8047' else '' }}>-99.8047%</option>
    </select>

    <div>📦 Unit Size (lembar/lot):</div>
    <input name="unit_size" value="{{ params.unit_size }}" type="number">
</div>

<div class="action-buttons">
    <button type="submit" class="run-button">🚀 Run Backtest</button>
    <div class="reset-params-link">
        <a href="#" onclick="document.getElementById('resetForm').submit(); return false;">↺ Reset ke default</a>
        <form id="resetForm" method="post" style="display:none;">
            <input type="hidden" name="action" value="reset_params">
        </form>
    </div>
    <form method="post" style="margin:0;">
        <input type="hidden" name="action" value="reset_all">
        <button type="submit" class="remove-button">🗑️ Reset All Files</button>
    </form>
</div>
</form>
{% endif %}

<hr>

{% if error %}
<p style="color:red; text-align:center;"><b>{{ error }}</b></p>
{% endif %}

{% if table %}
<div style="text-align:center; margin-bottom:10px;">
    <a href="/download">⬇️ Download CSV</a>
</div>
<div class="table-container">
{{ table | safe }}
</div>
{% endif %}

</body>
</html>
"""

# ============================================================
# FORMAT TABLE
# ============================================================

def format_table(df):
    df = df.copy()
    
    # Nomor urut
    numbers = []
    counter = 1
    for val in df["Ticker"]:
        if val in ["TOTAL", "CAGR (%)"]:
            numbers.append("")
        else:
            numbers.append(counter)
            counter += 1
    df.insert(0, "No", numbers)
    
    # Rename columns
    df.columns = [col.replace("_", " ").title() for col in df.columns]
    
    # Format angka
    for col in df.columns:
        if df[col].dtype == "object" or "date" in col.lower() or "purchase" in col.lower():
            continue
        if "Price" in col:
            df[col] = df[col].map(lambda x: f"{x:.4f}" if pd.notnull(x) else "")
        elif "Return" in col or col == "Cagr (%)":
            df[col] = df[col].map(lambda x: f"{x:.2f}%" if pd.notnull(x) else "")
        elif "Qty" in col:
            df[col] = df[col].map(lambda x: f"{int(x):,}" if pd.notnull(x) else "")
        else:
            df[col] = df[col].map(lambda x: f"{x:,.2f}" if pd.notnull(x) else "")
    
    # Color profit/return
    def color(val):
        try:
            if isinstance(val, str):
                val = val.replace('%', '').replace(',', '')
            v = float(val)
            if v > 0:
                return "color: green; font-weight: bold;"
            elif v < 0:
                return "color: red; font-weight: bold;"
        except:
            pass
        return ""
    
    styled = df.style
    profit_cols = ["Profit", "Return", "Cagr (%)"]
    for col in profit_cols:
        if col in df.columns:
            styled = styled.applymap(color, subset=[col])
    
    html = styled.hide(axis="index").to_html()
    html = html.replace('>TOTAL<', ' class="total-row">TOTAL<')
    html = html.replace('>CAGR (%)<', ' class="cagr-row">CAGR (%)<')
    
    return html

# ============================================================
# MAIN QUERY BUILDER
# ============================================================

def build_query(price_path, ff_path, anchor_date, my_budget, alpha, x, unit_size):
    return f"""
    CREATE OR REPLACE VIEW price_history AS 
    SELECT * FROM read_parquet('{price_path}');
    
    ATTACH '{ff_path}' AS db_ff;
    
    WITH 
    params AS (SELECT {unit_size} AS unit_size),
    
    settings AS (
        SELECT 
            DATE '{anchor_date}' AS anchor_date,
            {my_budget} AS my_budget,
            {alpha} AS alpha,
            {x} AS x
    ),
    
    all_time_highs AS (
        SELECT 
            Ticker,
            MAX(High) AS ath_price
        FROM price_history
        WHERE Date <= (SELECT anchor_date FROM settings)
        GROUP BY Ticker
    ),
    
    buy_price_candidates AS (
        SELECT 
            Ticker, 
            Close AS buy_price,
            Date AS actual_buy_date,
            ABS(DATEDIFF('day', Date, (SELECT anchor_date FROM settings))) AS diff
        FROM price_history
        WHERE Date BETWEEN (SELECT anchor_date FROM settings) - INTERVAL 15 DAY
                       AND (SELECT anchor_date FROM settings) + INTERVAL 15 DAY
    ),
    
    latest_buy_price AS (
        SELECT *
        FROM buy_price_candidates
        WHERE buy_price > 0  
        QUALIFY ROW_NUMBER() OVER(PARTITION BY Ticker ORDER BY diff) = 1
    ),
    
    threshold_calc AS (
        SELECT 
            b.Ticker,
            b.buy_price,
            b.actual_buy_date,
            a.ath_price,
            ((b.buy_price - a.ath_price) * 100.0 / NULLIF(a.ath_price,0)) AS perf_pct,
            (b.buy_price * ff."Float Shares") AS ff_value,
            (AVG(b.buy_price * ff."Float Shares") OVER() / s.alpha) AS ff_threshold,
            s.my_budget,
            s.anchor_date,
            p.unit_size
        FROM latest_buy_price b
        JOIN all_time_highs a ON b.Ticker = a.Ticker
        JOIN db_ff.free_float ff ON b.Ticker = ff.Ticker
        CROSS JOIN settings s
        CROSS JOIN params p
    ),
    
    filtered AS (
        SELECT 
            *,
            FLOOR(LEAST(ff_threshold,my_budget) / NULLIF(buy_price*unit_size,0)) AS qty
        FROM threshold_calc
        WHERE perf_pct <= (SELECT x FROM settings)
          AND ff_value < ff_threshold
          AND (buy_price*unit_size) <= my_budget
          AND buy_price > 0  
    ),
    
    price_lookup AS (
        SELECT 
            f.*,
            (SELECT Close FROM price_history p
             WHERE p.Ticker = f.Ticker AND p.Date >= f.anchor_date + INTERVAL 1 YEAR
             ORDER BY p.Date ASC LIMIT 1) AS p1,
            (SELECT Close FROM price_history p
             WHERE p.Ticker = f.Ticker AND p.Date >= f.anchor_date + INTERVAL 2 YEAR
             ORDER BY p.Date ASC LIMIT 1) AS p2,
            (SELECT Close FROM price_history p
             WHERE p.Ticker = f.Ticker AND p.Date >= f.anchor_date + INTERVAL 3 YEAR
             ORDER BY p.Date ASC LIMIT 1) AS p3,
            (SELECT Close FROM price_history p
             WHERE p.Ticker = f.Ticker AND p.Date >= f.anchor_date + INTERVAL 4 YEAR
             ORDER BY p.Date ASC LIMIT 1) AS p4
        FROM filtered f
    ),
    
    final_calculations AS (
        SELECT
            Ticker,
            actual_buy_date,
            buy_price,
            qty,
            unit_size,
            ROUND(qty * buy_price * unit_size, 2) AS allocation,
            CASE WHEN p1 IS NOT NULL THEN ROUND((p1 - buy_price) * qty * unit_size, 2) ELSE NULL END AS prof1,
            CASE WHEN p2 IS NOT NULL THEN ROUND((p2 - buy_price) * qty * unit_size, 2) ELSE NULL END AS prof2,
            CASE WHEN p3 IS NOT NULL THEN ROUND((p3 - buy_price) * qty * unit_size, 2) ELSE NULL END AS prof3,
            CASE WHEN p4 IS NOT NULL THEN ROUND((p4 - buy_price) * qty * unit_size, 2) ELSE NULL END AS prof4
        FROM price_lookup
    )
    
    SELECT *
    FROM (
        SELECT
            Ticker,
            actual_buy_date AS purchase_date,
            CAST(ROUND(buy_price, 4) AS DECIMAL(18,4)) AS price_per_share,
            qty,
            CAST(allocation AS DECIMAL(18,2)) AS allocation,
            CAST(prof1 AS DECIMAL(18,2)) AS profit_year1,
            CAST(prof2 AS DECIMAL(18,2)) AS profit_year2,
            CAST(prof3 AS DECIMAL(18,2)) AS profit_year3,
            CAST(prof4 AS DECIMAL(18,2)) AS profit_year4,
            CAST(ROUND(prof1 * 100.0 / NULLIF(allocation, 0), 2) AS DECIMAL(10,2)) AS return_year1,
            CAST(ROUND(prof2 * 100.0 / NULLIF(allocation, 0), 2) AS DECIMAL(10,2)) AS return_year2,
            CAST(ROUND(prof3 * 100.0 / NULLIF(allocation, 0), 2) AS DECIMAL(10,2)) AS return_year3,
            CAST(ROUND(prof4 * 100.0 / NULLIF(allocation, 0), 2) AS DECIMAL(10,2)) AS return_year4,
            1 AS sort_order
        FROM final_calculations
    
        UNION ALL
    
        SELECT
            'TOTAL',
            NULL, NULL, NULL,
            CAST(SUM(allocation) AS DECIMAL(18,2)),
            CAST(SUM(prof1) AS DECIMAL(18,2)),
            CAST(SUM(prof2) AS DECIMAL(18,2)),
            CAST(SUM(prof3) AS DECIMAL(18,2)),
            CAST(SUM(prof4) AS DECIMAL(18,2)),
            CAST(ROUND(SUM(prof1) * 100.0 / NULLIF(SUM(allocation), 0), 2) AS DECIMAL(10,2)),
            CAST(ROUND(SUM(prof2) * 100.0 / NULLIF(SUM(allocation), 0), 2) AS DECIMAL(10,2)),
            CAST(ROUND(SUM(prof3) * 100.0 / NULLIF(SUM(allocation), 0), 2) AS DECIMAL(10,2)),
            CAST(ROUND(SUM(prof4) * 100.0 / NULLIF(SUM(allocation), 0), 2) AS DECIMAL(10,2)),
            2
        FROM final_calculations
    
        UNION ALL
    
        SELECT
            'CAGR (%)',
            NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            CAST(ROUND((POWER((SUM(allocation) + SUM(prof1)) / NULLIF(SUM(allocation), 0), 1.0 / 1) - 1) * 100, 2) AS DECIMAL(10,2)),
            CAST(ROUND((POWER((SUM(allocation) + SUM(prof2)) / NULLIF(SUM(allocation), 0), 1.0 / 2) - 1) * 100, 2) AS DECIMAL(10,2)),
            CAST(ROUND((POWER((SUM(allocation) + SUM(prof3)) / NULLIF(SUM(allocation), 0), 1.0 / 3) - 1) * 100, 2) AS DECIMAL(10,2)),
            CAST(ROUND((POWER((SUM(allocation) + SUM(prof4)) / NULLIF(SUM(allocation), 0), 1.0 / 4) - 1) * 100, 2) AS DECIMAL(10,2)),
            3
        FROM final_calculations
    ) t
    ORDER BY sort_order, ticker ASC
    """

# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/", methods=["GET", "POST"])
def index():
    global CURRENT_PRICE_FILE, CURRENT_FF_FILE
    
    table = None
    error = None
    
    # Inisialisasi session parameter jika belum ada
    if "ff_params" not in session:
        session["ff_params"] = DEFAULT_PARAMS.copy()
    
    if request.method == "POST":
        action = request.form.get("action")
        
        # Upload Price File (reset ke default)
        if action == "upload_price":
            file = request.files.get("file")
            if file and file.filename.endswith('.parquet'):
                file_id = str(uuid.uuid4())
                path = os.path.join(UPLOAD_FOLDER, f"price_{file_id}.parquet")
                file.save(path)
                CURRENT_PRICE_FILE["path"] = path
                CURRENT_PRICE_FILE["name"] = file.filename
                # Reset params ke default saat upload file baru
                session["ff_params"] = DEFAULT_PARAMS.copy()
            else:
                error = "File harus berekstensi .parquet"
            return redirect("/")
        
        # Upload Free Float DB (reset ke default)
        elif action == "upload_ff":
            file = request.files.get("file")
            if file and file.filename.endswith('.db'):
                file_id = str(uuid.uuid4())
                path = os.path.join(UPLOAD_FOLDER, f"ff_{file_id}.db")
                file.save(path)
                CURRENT_FF_FILE["path"] = path
                CURRENT_FF_FILE["name"] = file.filename
                # Reset params ke default saat upload file baru
                session["ff_params"] = DEFAULT_PARAMS.copy()
            else:
                error = "File harus berekstensi .db"
            return redirect("/")
        
        # Remove Price (reset ke default)
        elif action == "remove_price":
            if CURRENT_PRICE_FILE["path"] and os.path.exists(CURRENT_PRICE_FILE["path"]):
                os.remove(CURRENT_PRICE_FILE["path"])
            CURRENT_PRICE_FILE = {"path": None, "name": None}
            session["ff_params"] = DEFAULT_PARAMS.copy()
            return redirect("/")
        
        # Remove FF (reset ke default)
        elif action == "remove_ff":
            if CURRENT_FF_FILE["path"] and os.path.exists(CURRENT_FF_FILE["path"]):
                os.remove(CURRENT_FF_FILE["path"])
            CURRENT_FF_FILE = {"path": None, "name": None}
            session["ff_params"] = DEFAULT_PARAMS.copy()
            return redirect("/")
        
        # Reset All Files (reset ke default)
        elif action == "reset_all":
            if CURRENT_PRICE_FILE["path"] and os.path.exists(CURRENT_PRICE_FILE["path"]):
                os.remove(CURRENT_PRICE_FILE["path"])
            if CURRENT_FF_FILE["path"] and os.path.exists(CURRENT_FF_FILE["path"]):
                os.remove(CURRENT_FF_FILE["path"])
            CURRENT_PRICE_FILE = {"path": None, "name": None}
            CURRENT_FF_FILE = {"path": None, "name": None}
            session["ff_params"] = DEFAULT_PARAMS.copy()
            return redirect("/")
        
        # Reset Params ke default (tetap pakai file yang sama)
        elif action == "reset_params":
            session["ff_params"] = DEFAULT_PARAMS.copy()
            return redirect("/")
        
        # Run Backtest (simpan parameter terbaru ke session)
        elif action == "run":
            # Simpan parameter dari form ke session
            session["ff_params"] = {
                "anchor_date": request.form.get("anchor_date", DEFAULT_PARAMS["anchor_date"]),
                "my_budget": request.form.get("my_budget", DEFAULT_PARAMS["my_budget"]),
                "alpha": request.form.get("alpha", DEFAULT_PARAMS["alpha"]),
                "x": request.form.get("x", DEFAULT_PARAMS["x"]),
                "unit_size": request.form.get("unit_size", DEFAULT_PARAMS["unit_size"])
            }
            
            if not CURRENT_PRICE_FILE["path"] or not CURRENT_FF_FILE["path"]:
                error = "Price history dan Free float DB harus diupload terlebih dahulu"
            else:
                try:
                    anchor_date = session["ff_params"]["anchor_date"]
                    my_budget = int(session["ff_params"]["my_budget"])
                    alpha = int(session["ff_params"]["alpha"])
                    x = float(session["ff_params"]["x"])
                    unit_size = int(session["ff_params"]["unit_size"])
                    
                    query = build_query(
                        CURRENT_PRICE_FILE["path"].replace("\\", "/"),
                        CURRENT_FF_FILE["path"].replace("\\", "/"),
                        anchor_date, my_budget, alpha, x, unit_size
                    )
                    
                    con = duckdb.connect()
                    df = con.execute(query).df()
                    con.close()
                    
                    if df.empty:
                        error = "Tidak ada data yang memenuhi kriteria"
                    else:
                        df.to_csv("ff_result.csv", index=False)
                        table = format_table(df)
                        
                except Exception as e:
                    error = f"Error: {str(e)}"
    
    # GET request
    return render_template_string(
        HTML,
        table=table,
        error=error,
        price_file=CURRENT_PRICE_FILE["name"],
        ff_file=CURRENT_FF_FILE["name"],
        params=session["ff_params"]
    )

@app.route("/download")
def download():
    if os.path.exists("ff_result.csv"):
        return send_file("ff_result.csv", as_attachment=True, download_name="ff_backtest_result.csv")
    else:
        return "No result file", 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
from flask import Flask, request, render_template_string, send_file, redirect, session
import duckdb
import pandas as pd
import os
import uuid

app = Flask(__name__)
app.secret_key = "rahasia_default_params_2024"  # Wajib untuk session

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

CURRENT_FILE = {"path": None, "name": None}

# Default parameter
DEFAULT_PARAMS = {
    "date": "2025-01-01",
    "ma_short": "50",
    "ma_long": "200",
    "budget": "1000000",
    "unit_size": "100"
}

# ============================================================
# HTML (DIPERBAIKI - nilai parameter dari session)
# ============================================================

HTML = """
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">

<style>
body { font-family: Arial; margin: 20px; }

h2 { text-align: center; }

.form-box {
    display: flex;
    flex-direction: column;
    align-items: center;
}

.upload-box {
    border: 2px dashed #aaa;
    padding: 30px;
    text-align: center;
    width: 320px;
}

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
    font-size: 20px;
    padding: 14px 50px;
    cursor: pointer;
}

.table-container {
    width: 100%;
    overflow-x: auto;
}

table {
    border-collapse: collapse;
    width: 100%;
    min-width: 1200px;
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

td:nth-child(2) { text-align: left; }

.total-row td {
    background-color: #d9edf7 !important;
    font-weight: bold;
}

.cagr-row td {
    background-color: #dff0d8 !important;
    font-weight: bold;
}

.file-box {
    display: flex;
    justify-content: center;
    gap: 10px;
    align-items: center;
}

.reset-link {
    margin-left: 15px;
    font-size: 12px;
}

.reset-link a {
    color: #f0ad4e;
    text-decoration: none;
}

.reset-link a:hover {
    text-decoration: underline;
}
</style>
</head>

<body>

<h2>📈 Multiverse Golden Cross</h2>

{% if current_file %}
<div class="file-box">
    <b>File aktif:</b> {{current_file}}

    <form method="post" style="margin:0;">
        <input type="hidden" name="action" value="remove">
        <button type="submit">❌ Remove</button>
    </form>

    <div class="reset-link">
        <a href="#" onclick="document.getElementById('resetForm').submit(); return false;">↺ Reset ke default</a>
        <form id="resetForm" method="post" style="display:none;">
            <input type="hidden" name="action" value="reset_params">
        </form>
    </div>
</div>

<br>

<form method="post" class="form-box">
<input type="hidden" name="action" value="run">

<div class="param-grid">
    <div>Tanggal:</div>
    <input name="date" value="{{ params.date }}">

    <div>MA Short:</div>
    <input name="ma_short" value="{{ params.ma_short }}">

    <div>MA Long:</div>
    <input name="ma_long" value="{{ params.ma_long }}">

    <div>Budget:</div>
    <input name="budget" value="{{ params.budget }}">

    <div>Unit Size (lot):</div>
    <input name="unit_size" value="{{ params.unit_size }}">
</div>

<button type="submit" class="big-button">🚀 Load Results</button>

</form>

{% else %}
<form method="post" enctype="multipart/form-data" class="form-box">
<input type="hidden" name="action" value="upload">

<div class="upload-box">
    <p><b>Upload Parquet File</b></p>
    <input type="file" name="file" required><br><br>
    <button type="submit" class="upload-button">📂 Upload File</button>
</div>
</form>
{% endif %}

<hr>

{% if table %}
<div style="text-align:center;">
    <a href="/download">⬇️ Download CSV</a>
</div>

<div class="table-container">
{{table | safe}}
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

    # nomor
    nums = []
    c = 1
    for v in df["Ticker"]:
        if v in ["TOTAL", "CAGR (%)"]:
            nums.append("")
        else:
            nums.append(c)
            c += 1

    df.insert(0, "No", nums)

    # header rapih
    df.columns = [c.replace("_", " ").title() for c in df.columns]

    # format angka
    for col in df.columns:
        if df[col].dtype == "object" or "Date" in col:
            continue

        if col == "Price":
            df[col] = df[col].map(lambda x: f"{x:.4f}" if pd.notnull(x) else "")
        else:
            df[col] = df[col].map(lambda x: f"{x:.2f}" if pd.notnull(x) else "")

    styled = df.style.hide(axis="index")

    html = styled.to_html()
    html = html.replace(">TOTAL<", ' class="total-row">TOTAL<')
    html = html.replace(">CAGR (%)<", ' class="cagr-row">CAGR (%)<')

    return html

# ============================================================
# MAIN (DENGAN SESSION UNTUK PARAMETER)
# ============================================================

@app.route("/", methods=["GET", "POST"])
def index():
    global CURRENT_FILE

    table = None

    # Inisialisasi session parameter jika belum ada
    if "params" not in session:
        session["params"] = DEFAULT_PARAMS.copy()

    if request.method == "POST":
        action = request.form.get("action")

        # ---------------- UPLOAD (reset ke default) ----------------
        if action == "upload":
            file = request.files.get("file")

            if not file or file.filename == "":
                return render_template_string(HTML, current_file=None, params=session["params"])

            file_id = str(uuid.uuid4())
            path = os.path.join(UPLOAD_FOLDER, file_id + ".parquet")
            file.save(path)

            CURRENT_FILE["path"] = path
            CURRENT_FILE["name"] = file.filename

            # Reset parameter ke default saat upload file baru
            session["params"] = DEFAULT_PARAMS.copy()

            return redirect("/")

        # ---------------- REMOVE (reset ke default) ----------------
        if action == "remove":
            CURRENT_FILE = {"path": None, "name": None}
            # Reset parameter ke default saat remove file
            session["params"] = DEFAULT_PARAMS.copy()
            return redirect("/")

        # ---------------- RESET PARAMS (kembali ke default) ----------------
        if action == "reset_params":
            session["params"] = DEFAULT_PARAMS.copy()
            return redirect("/")

        # ---------------- RUN (simpan parameter terbaru) ----------------
        if action == "run":
            # Simpan parameter dari form ke session
            session["params"] = {
                "date": request.form.get("date", DEFAULT_PARAMS["date"]),
                "ma_short": request.form.get("ma_short", DEFAULT_PARAMS["ma_short"]),
                "ma_long": request.form.get("ma_long", DEFAULT_PARAMS["ma_long"]),
                "budget": request.form.get("budget", DEFAULT_PARAMS["budget"]),
                "unit_size": request.form.get("unit_size", DEFAULT_PARAMS["unit_size"])
            }

            if not CURRENT_FILE["path"]:
                return render_template_string(HTML, current_file=None, params=session["params"])

            con = duckdb.connect()

            # Ambil parameter dari session (yang sudah disimpan)
            scan_start_date = session["params"]["date"]
            budget = float(session["params"]["budget"])
            unit_size = int(session["params"]["unit_size"])
            ma_short = int(session["params"]["ma_short"])
            ma_long = int(session["params"]["ma_long"])

            # Query utama dengan filter: harga * unit_size <= budget
            query = f"""
            -- Data harga
            CREATE OR REPLACE VIEW price_history AS 
            SELECT * FROM read_parquet('{CURRENT_FILE["path"]}');

            -- Parameter
            WITH settings AS (
              SELECT 
                DATE '{scan_start_date}' AS scan_start_date,
                {budget} AS my_budget,
                {unit_size} AS unit_size,
                {ma_short} AS ma_short,
                {ma_long} AS ma_long
            ),

            -- Hitung moving average
            ma AS (
                SELECT *,
                    AVG(Close) OVER (
                        PARTITION BY Ticker ORDER BY Date
                        ROWS BETWEEN (SELECT ma_short-1 FROM settings) PRECEDING AND CURRENT ROW
                    ) AS ma_s,

                    AVG(Close) OVER (
                        PARTITION BY Ticker ORDER BY Date
                        ROWS BETWEEN (SELECT ma_long-1 FROM settings) PRECEDING AND CURRENT ROW
                    ) AS ma_l
                FROM price_history
            ),

            -- Signal golden cross
            sig AS (
                SELECT *,
                    LAG(ma_s) OVER (PARTITION BY Ticker ORDER BY Date) ps,
                    LAG(ma_l) OVER (PARTITION BY Ticker ORDER BY Date) pl
                FROM ma
                WHERE Date >= (SELECT scan_start_date FROM settings)
            ),

            -- Entry point pertama tiap ticker (BELUM FILTER BUDGET)
            entry_raw AS (
                SELECT *
                FROM sig
                WHERE ps <= pl AND ma_s > ma_l
                QUALIFY ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY Date)=1
            ),

            -- Filter: harga * unit_size <= budget (bisa beli minimal 1 lot)
            entry AS (
                SELECT *
                FROM entry_raw
                WHERE Close * (SELECT unit_size FROM settings) <= (SELECT my_budget FROM settings)
            ),

            -- Harga terakhir di seluruh data
            latest AS (
                SELECT Ticker, Close AS latest_price
                FROM price_history
                QUALIFY ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY Date DESC)=1
            ),

            -- Tanggal terakhir di seluruh dataset (untuk CAGR)
            last_date_all AS (
                SELECT MAX(Date) AS last_date FROM price_history
            ),

            -- Perhitungan per ticker
            calc AS (
                SELECT
                    e.Ticker,
                    e.Date AS purchase_date,
                    e.Close AS price,

                    -- Quantity (dalam lot) = budget / (harga * unit_size)
                    FLOOR((SELECT my_budget FROM settings) / (e.Close * (SELECT unit_size FROM settings))) AS qty,

                    -- Alokasi dana = qty * harga * unit_size
                    FLOOR((SELECT my_budget FROM settings) / (e.Close * (SELECT unit_size FROM settings)))
                    * e.Close * (SELECT unit_size FROM settings) AS allocation,

                    l.latest_price,

                    -- Profit unrealized
                    (l.latest_price - e.Close) *
                    FLOOR((SELECT my_budget FROM settings) / (e.Close * (SELECT unit_size FROM settings)))
                    * (SELECT unit_size FROM settings) AS profit,

                    -- Return persentase per ticker
                    (l.latest_price - e.Close) / NULLIF(e.Close,0) * 100 AS return
                FROM entry e
                JOIN latest l USING(Ticker)
            ),

            -- Agregasi untuk TOTAL dan CAGR (hanya dari ticker yang terfilter)
            aggregated AS (
                SELECT
                    SUM(allocation) AS total_allocation,
                    SUM(profit) AS total_profit,
                    MIN(purchase_date) AS first_purchase_date,
                    (SELECT last_date FROM last_date_all) AS last_date
                FROM calc
            )

            -- Final output: detail per ticker + TOTAL + CAGR
            SELECT * FROM (

                -- Detail per ticker (hanya yang memenuhi syarat budget)
                SELECT
                    Ticker,
                    purchase_date,
                    ROUND(price,4) AS price,
                    qty,
                    allocation,
                    latest_price,
                    profit,
                    return,
                    1 AS sort_order
                FROM calc

                UNION ALL

                -- Baris TOTAL (hanya dari ticker yang dibeli)
                SELECT
                    'TOTAL',
                    NULL,
                    NULL,
                    NULL,
                    total_allocation,
                    NULL,
                    total_profit,
                    CASE WHEN total_allocation > 0 THEN (total_profit / total_allocation) * 100 ELSE 0 END,
                    2
                FROM aggregated

                UNION ALL

                -- Baris CAGR (%) (hanya dari ticker yang dibeli)
                SELECT
                    'CAGR (%)',
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    CASE 
                        WHEN total_allocation > 0 AND first_purchase_date IS NOT NULL AND last_date IS NOT NULL 
                             AND last_date > first_purchase_date
                        THEN 
                            ROUND(
                                (POWER(
                                    (total_allocation + total_profit) / total_allocation,
                                    1.0 / ((EXTRACT(EPOCH FROM (last_date - first_purchase_date)) / 86400.0) / 365.25)
                                ) - 1) * 100
                            , 2)
                        ELSE NULL
                    END,
                    3
                FROM aggregated

            ) t
            ORDER BY sort_order, purchase_date NULLS LAST
            """

            df = con.execute(query).df()
            
            # Jika tidak ada data (semua saham tidak memenuhi budget), kasih pesan
            if df.empty or (len(df) == 2 and df.iloc[0]['Ticker'] == 'TOTAL' and df.iloc[1]['Ticker'] == 'CAGR (%)'):
                df = pd.DataFrame({
                    'Ticker': ['TOTAL', 'CAGR (%)'],
                    'Allocation': [0, None],
                    'Profit': [0, None],
                    'Return': [0, None]
                })
                df['Return'] = df['Return'].fillna(0)
            
            df.to_csv("result.csv", index=False)
            table = format_table(df)

    return render_template_string(
        HTML,
        table=table,
        current_file=CURRENT_FILE["name"],
        params=session["params"]
    )

@app.route("/download")
def download():
    return send_file("result.csv", as_attachment=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
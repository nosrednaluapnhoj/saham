from flask import Flask, request, render_template_string, send_file, redirect
import duckdb
import pandas as pd
import os
import uuid

app = Flask(__name__)
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

CURRENT_FILE = {"path": None, "name": None}

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

.form-box {
    display: flex;
    flex-direction: column;
    align-items: center;
}

/* upload box */
.upload-box {
    border: 2px dashed #aaa;
    padding: 30px;
    text-align: center;
    width: 320px;
}

/* parameter */
.param-grid {
    display: grid;
    grid-template-columns: 200px 200px;
    gap: 10px 20px;
    margin-bottom: 20px;
}

/* buttons */
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

/* ticker kiri */
td:nth-child(2) {
    text-align: left;
}

tr:hover { background-color: #f5f5f5; }

/* highlight */
.total-row td {
    background-color: #d9edf7 !important;
    font-weight: bold;
}

.cagr-row td {
    background-color: #dff0d8 !important;
    font-weight: bold;
}

/* file active */
.file-box {
    display: flex;
    justify-content: center;
    gap: 10px;
    align-items: center;
}
</style>
</head>

<body>

<h2>📈 Golden Cross Backtester</h2>

{% if current_file %}
<div class="file-box">
    <b>File aktif:</b> {{current_file}}

    <form method="post" style="margin:0;">
        <input type="hidden" name="action" value="remove">
        <button type="submit">❌ Remove</button>
    </form>
</div>

<br>

<form method="post" class="form-box">
<input type="hidden" name="action" value="run">

<div class="param-grid">
    <div>Tanggal:</div>
    <input name="date" value="2020-01-02">

    <div>MA Short:</div>
    <input name="ma_short" value="50">

    <div>MA Long:</div>
    <input name="ma_long" value="200">

    <div>Budget:</div>
    <input name="budget" value="300000">

    <div>Unit Size:</div>
    <input name="unit_size" value="100">
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

{% if error %}
<p style="color:red; text-align:center;"><b>{{error}}</b></p>
{% endif %}

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
# FORMAT TABLE (DATE SAFE)
# ============================================================

def format_table(df):
    df = df.copy()

    # =========================
    # NOMOR
    # =========================
    numbers = []
    counter = 1
    for val in df["Ticker"]:
        if val in ["TOTAL", "CAGR (%)"]:
            numbers.append("")
        else:
            numbers.append(counter)
            counter += 1

    df.insert(0, "No", numbers)

    # =========================
    # HEADER
    # =========================
    df.columns = [col.replace("_", " ").title() for col in df.columns]

    # =========================
    # FORMAT ANGKA (SAFE DATE)
    # =========================
    for col in df.columns:

        # skip object & datetime (INI FIX UTAMA)
        if df[col].dtype == "object" or "date" in col.lower():
            continue

        if col == "Price":
            df[col] = df[col].map(lambda x: f"{x:.4f}" if pd.notnull(x) else "")
        else:
            df[col] = df[col].map(lambda x: f"{x:.2f}" if pd.notnull(x) else "")

    # =========================
    # COLOR PROFIT / RETURN
    # =========================
    def color(val):
        try:
            v = float(val)
            if v > 0:
                return "color: green; font-weight: bold;"
            elif v < 0:
                return "color: red; font-weight: bold;"
        except:
            pass
        return ""

    styled = df.style

    for col in ["Prof1","Prof2","Prof3","Prof4",
                "Ret Y1","Ret Y2","Ret Y3","Ret Y4"]:
        if col in df.columns:
            styled = styled.applymap(color, subset=[col])

    html = styled.hide(axis="index").to_html()

    # highlight row
    html = html.replace('>TOTAL<', ' class="total-row">TOTAL<')
    html = html.replace('>CAGR (%)<', ' class="cagr-row">CAGR (%)<')

    return html

# ============================================================
# MAIN
# ============================================================

@app.route("/", methods=["GET", "POST"])
def index():
    global CURRENT_FILE

    table = None
    error = None

    if request.method == "POST":

        action = request.form.get("action")

        if action == "upload":
            file = request.files.get("file")

            if not file or file.filename == "":
                error = "File belum dipilih"
            else:
                file_id = str(uuid.uuid4())
                path = os.path.join(UPLOAD_FOLDER, file_id + ".parquet")
                file.save(path)

                CURRENT_FILE["path"] = path
                CURRENT_FILE["name"] = file.filename

                return redirect("/")

        elif action == "remove":
            CURRENT_FILE = {"path": None, "name": None}
            return redirect("/")

        elif action == "run":

            if not CURRENT_FILE["path"]:
                return render_template_string(HTML, error="Belum ada file")

            scan_start_date = request.form.get("date")
            ma_short = int(request.form.get("ma_short"))
            ma_long = int(request.form.get("ma_long"))
            budget = float(request.form.get("budget"))
            unit_size = int(request.form.get("unit_size"))

            try:
                con = duckdb.connect()

                query = f"""
                CREATE OR REPLACE VIEW price_history AS 
                SELECT * FROM read_parquet('{CURRENT_FILE["path"]}');

                WITH settings AS (
                  SELECT 
                    DATE '{scan_start_date}' AS scan_start_date,
                    {budget} AS my_budget,
                    {unit_size} AS unit_size,
                    {ma_short} AS ma_short,
                    {ma_long} AS ma_long
                ),

                ma_calculated AS (
                    SELECT 
                        p.Ticker,
                        p.Date,
                        p.Close,

                        AVG(p.Close) OVER (
                            PARTITION BY p.Ticker 
                            ORDER BY p.Date 
                            ROWS BETWEEN (SELECT ma_short - 1 FROM settings) PRECEDING AND CURRENT ROW
                        ) AS ma_short,

                        AVG(p.Close) OVER (
                            PARTITION BY p.Ticker 
                            ORDER BY p.Date 
                            ROWS BETWEEN (SELECT ma_long - 1 FROM settings) PRECEDING AND CURRENT ROW
                        ) AS ma_long

                    FROM price_history p
                ),

                golden_cross_events AS (
                    SELECT 
                        Ticker,
                        Date AS cross_date,
                        Close AS buy_price,
                        ma_short,
                        ma_long,

                        LAG(ma_short) OVER (PARTITION BY Ticker ORDER BY Date) AS prev_ma_short,
                        LAG(ma_long) OVER (PARTITION BY Ticker ORDER BY Date) AS prev_ma_long

                    FROM ma_calculated
                    WHERE Date BETWEEN (SELECT scan_start_date FROM settings) 
                                   AND (SELECT scan_start_date FROM settings) + INTERVAL 1 YEAR
                ),

                filtered_signals AS (
                    SELECT 
                        f.Ticker,
                        f.cross_date,
                        f.buy_price,
                        s.my_budget,
                        s.unit_size,
                        FLOOR(s.my_budget / NULLIF(f.buy_price * s.unit_size, 0)) AS qty

                    FROM golden_cross_events f
                    CROSS JOIN settings s

                    WHERE f.prev_ma_short <= f.prev_ma_long 
                      AND f.ma_short > f.ma_long
                      AND (f.buy_price * s.unit_size) <= s.my_budget
                      AND f.buy_price > 0

                    QUALIFY ROW_NUMBER() OVER(
                        PARTITION BY f.Ticker 
                        ORDER BY f.cross_date ASC
                    ) = 1
                ),

                price_lookup AS (
                    SELECT 
                        f.*,

                        (SELECT p.Close FROM price_history p 
                         WHERE p.Ticker = f.Ticker 
                         AND p.Date >= f.cross_date + INTERVAL 1 YEAR 
                         ORDER BY p.Date ASC LIMIT 1) AS p1,

                        (SELECT p.Close FROM price_history p 
                         WHERE p.Ticker = f.Ticker 
                         AND p.Date >= f.cross_date + INTERVAL 2 YEAR 
                         ORDER BY p.Date ASC LIMIT 1) AS p2,

                        (SELECT p.Close FROM price_history p 
                         WHERE p.Ticker = f.Ticker 
                         AND p.Date >= f.cross_date + INTERVAL 3 YEAR 
                         ORDER BY p.Date ASC LIMIT 1) AS p3,

                        (SELECT p.Close FROM price_history p 
                         WHERE p.Ticker = f.Ticker 
                         AND p.Date >= f.cross_date + INTERVAL 4 YEAR 
                         ORDER BY p.Date ASC LIMIT 1) AS p4

                    FROM filtered_signals f
                ),

                final_calculations AS (
                    SELECT
                        Ticker,
                        cross_date AS purchase_date,
                        buy_price,
                        qty,
                        unit_size,

                        ROUND(qty * buy_price * unit_size, 2) AS allocation,

                        ROUND((p1 - buy_price) * qty * unit_size, 2) AS prof1,
                        ROUND((p2 - buy_price) * qty * unit_size, 2) AS prof2,
                        ROUND((p3 - buy_price) * qty * unit_size, 2) AS prof3,
                        ROUND((p4 - buy_price) * qty * unit_size, 2) AS prof4

                    FROM price_lookup
                    WHERE qty > 0
                )

                SELECT * FROM (

                    SELECT
                        Ticker,
                        purchase_date,
                        ROUND(buy_price,2) AS price,
                        qty,
                        allocation,
                        prof1, prof2, prof3, prof4,

                        ROUND(prof1 * 100.0 / NULLIF(allocation, 0), 2) AS ret_y1,
                        ROUND(prof2 * 100.0 / NULLIF(allocation, 0), 2) AS ret_y2,
                        ROUND(prof3 * 100.0 / NULLIF(allocation, 0), 2) AS ret_y3,
                        ROUND(prof4 * 100.0 / NULLIF(allocation, 0), 2) AS ret_y4,

                        1 AS sort_order

                    FROM final_calculations

                    UNION ALL

                    SELECT
                        'TOTAL', NULL, NULL, NULL,
                        SUM(allocation),
                        SUM(prof1), SUM(prof2), SUM(prof3), SUM(prof4),

                        ROUND(SUM(prof1)*100.0/NULLIF(SUM(allocation),0),2),
                        ROUND(SUM(prof2)*100.0/NULLIF(SUM(allocation),0),2),
                        ROUND(SUM(prof3)*100.0/NULLIF(SUM(allocation),0),2),
                        ROUND(SUM(prof4)*100.0/NULLIF(SUM(allocation),0),2),

                        2

                    FROM final_calculations

                    UNION ALL

                    SELECT
                        'CAGR (%)', NULL, NULL, NULL, NULL,
                        NULL, NULL, NULL, NULL,

                        ROUND((POWER((SUM(allocation)+SUM(prof1))/NULLIF(SUM(allocation),0),1.0/1)-1)*100,2),
                        ROUND((POWER((SUM(allocation)+SUM(prof2))/NULLIF(SUM(allocation),0),1.0/2)-1)*100,2),
                        ROUND((POWER((SUM(allocation)+SUM(prof3))/NULLIF(SUM(allocation),0),1.0/3)-1)*100,2),
                        ROUND((POWER((SUM(allocation)+SUM(prof4))/NULLIF(SUM(allocation),0),1.0/4)-1)*100,2),

                        3

                    FROM final_calculations

                ) t
                ORDER BY sort_order, purchase_date
                """

                df = con.execute(query).df()

            except Exception as e:
                return render_template_string(HTML, error=str(e))

            df.to_csv("result.csv", index=False)
            table = format_table(df)

    return render_template_string(
        HTML,
        table=table,
        error=error,
        current_file=CURRENT_FILE["name"]
    )

@app.route("/download")
def download():
    return send_file("result.csv", as_attachment=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
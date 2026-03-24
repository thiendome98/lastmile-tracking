import json, sqlite3, threading, io, csv
from datetime import datetime
from flask import Flask, render_template, jsonify, make_response, request
import paho.mqtt.client as mqtt
from waitress import serve

app = Flask(__name__)
DB_PATH = 'data.db'

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS records 
                       (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                        timestamp REAL, readable_time TEXT, 
                        device_id TEXT, lat REAL, lon REAL, 
                        hdop REAL, satellites INTEGER)''')
        conn.commit()

# --- MQTT Logic ---
def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        now = datetime.now()
        with get_db() as conn:
            conn.execute("INSERT INTO records (timestamp, readable_time, device_id, lat, lon, hdop, satellites) VALUES (?, ?, ?, ?, ?, ?, ?)",
                         (now.timestamp(), now.strftime('%Y-%m-%d %H:%M:%S'), 
                          data.get("device_id", "Unknown"), data.get("gps",{}).get("lat",0), 
                          data.get("gps",{}).get("lon",0), data.get("hdop",99), data.get("satellites",0)))
            conn.commit()
    except Exception as e: print(f"MQTT Error: {e}")

def start_mqtt():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    client.connect("broker.hivemq.com", 1883, 60)
    client.subscribe("phuoc/pi/realtime")
    client.loop_forever()

# --- Routes ---
@app.route('/')
def index(): return render_template('table.html')

@app.route('/api/table')
def get_table_data():
    f = request.args.get('filter', 'all')
    with get_db() as conn:
        if f == 'all':
            rows = conn.execute("SELECT * FROM records ORDER BY id DESC LIMIT 100").fetchall()
        else:
            rows = conn.execute("SELECT * FROM records WHERE device_id = ? ORDER BY id DESC LIMIT 100", (f,)).fetchall()
        return jsonify([dict(row) for row in rows])

@app.route('/api/export')
def export_csv():
    dev = request.args.get('device', 'all')
    with get_db() as conn:
        q = "SELECT * FROM records ORDER BY id DESC" if dev == 'all' else "SELECT * FROM records WHERE device_id = ? ORDER BY id DESC"
        p = () if dev == 'all' else (dev,)
        rows = conn.execute(q, p).fetchall()
    
    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(['ID', 'Time', 'Device', 'Lat', 'Lon', 'HDOP', 'Sats', 'Status'])
    for r in rows:
        cw.writerow([r['id'], r['readable_time'], r['device_id'], r['lat'], r['lon'], r['hdop'], r['satellites'], "FIXED" if r['lat']!=0 else "NO FIX"])
    
    resp = make_response(si.getvalue().encode('utf-8-sig'))
    resp.headers["Content-Disposition"] = f"attachment; filename=LASTMILE_Log_{dev}.csv"
    resp.headers["Content-type"] = "text/csv"
    return resp



if __name__ == "__main__":
    init_db()
    threading.Thread(target=start_mqtt, daemon=True).start()
    print("LASTMILE Server running on http://localhost:5000")
    serve(app, host="0.0.0.0", port=5000, threads=6)





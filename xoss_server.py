"""
XOSS Heart Rate Logger + Dashboard en vivo (Listo para PyInstaller .exe).
BLE (Frecuencia Cardíaca + Batería) -> SQLite (automático al conectar) -> WebSocket + Exportación JSON por sensor.
Requisitos: pip install bleak aiohttp pyinstaller
Uso normal: python xoss_server.py
Empaquetar: pyinstaller --noconfirm --onefile --name "XOSS_Monitor" --add-data "dashboard.html;." --collect-all bleak xoss_server.py
"""
import asyncio, json, re, sqlite3, sys, threading, time, webbrowser
from datetime import datetime, timezone
from pathlib import Path
from aiohttp import web
from bleak import BleakClient, BleakScanner

# Rutas compatibles tanto con Python normal como con el ejecutable (.exe) de PyInstaller
if getattr(sys, "frozen", False):
    BASE = Path(sys.executable).parent
    HTML_PATH = Path(sys._MEIPASS) / "dashboard.html"
else:
    BASE = Path(__file__).parent
    HTML_PATH = BASE / "dashboard.html"

CONFIG_PATH = BASE / "config.json"
DB_PATH = BASE / "xoss_readings.sqlite3"
USERS_PATH = BASE / "users.json"

TAIL = "-0000-1000-8000-00805f9b34fb"
U = lambda n: f"0000{n}{TAIL}"

HR_SERVICE, HR_CHAR = U("180d"), U("2a37")
BATTERY_SERVICE, BATTERY_LEVEL = U("180f"), U("2a19")

DEFAULTS = {
    "save_interval_seconds": 1.0,
    "dashboard_refresh_seconds": 1.0,
    "scan_timeout_seconds": 8,
    "max_sensors": 5,
    "stale_after_seconds": 5,
    "auto_json_export_every_seconds": 3600,  # 3600 s = 1 hora
    "user_weight_kg": 70.0,
    "user_age": 25,
    "user_gender": "M",
    "export_dir": "exports",
    "host": "0.0.0.0",
    "port": 8080
}

INSERT = """INSERT INTO readings (
    recorded_at_utc, user_name, device_name, device_address,
    heart_rate_bpm, avg_bpm, max_bpm, calories_kcal, battery_percent
) VALUES (?,?,?,?,?,?,?,?,?)"""

now_iso = lambda unit: datetime.now(timezone.utc).isoformat(timespec=unit)
clean_name = lambda s: re.sub(r"[^a-zA-Z0-9_-]", "_", s.strip()) if s and s.strip() else "Sensor"


def load_config():
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(json.dumps(DEFAULTS, indent=2), "utf-8")
    raw = json.loads(CONFIG_PATH.read_text("utf-8"))
    if raw.get("auto_json_export_every_seconds") == 0:
        raw["auto_json_export_every_seconds"] = 3600
    cfg = {**DEFAULTS, **raw}
    cfg["save_interval_seconds"] = max(0.2, float(cfg["save_interval_seconds"]))
    cfg["dashboard_refresh_seconds"] = max(0.2, float(cfg["dashboard_refresh_seconds"]))
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2), "utf-8")
    return cfg


# ---------------------------------------------------------------- Base de datos
def query(sql, params=(), many=False):
    with sqlite3.connect(DB_PATH) as conn:
        return (conn.executemany if many else conn.execute)(sql, params).fetchall()


def init_db():
    query("""CREATE TABLE IF NOT EXISTS readings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        recorded_at_utc TEXT NOT NULL,
        user_name TEXT,
        device_name TEXT,
        device_address TEXT,
        heart_rate_bpm INTEGER,
        avg_bpm REAL,
        max_bpm INTEGER,
        calories_kcal REAL,
        battery_percent INTEGER
    )""")
    query("CREATE INDEX IF NOT EXISTS idx_readings_time ON readings(recorded_at_utc)")
    for col in ("avg_bpm REAL", "max_bpm INTEGER", "calories_kcal REAL"):
        try:
            query(f"ALTER TABLE readings ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass


def hourly_bpm(since_id=0):
    """Promedio de BPM por hora (hora local de esta computadora) por usuario y dispositivo."""
    groups = {}
    for user, dev, addr, hour, avg, lo, hi, n in query("""
            SELECT user_name, device_name, device_address,
                   strftime('%Y-%m-%d %H:00', recorded_at_utc, 'localtime') AS hour,
                   ROUND(AVG(heart_rate_bpm), 1), MIN(heart_rate_bpm), MAX(heart_rate_bpm), COUNT(*)
            FROM readings WHERE heart_rate_bpm IS NOT NULL AND id > ?
            GROUP BY user_name, device_address, hour ORDER BY device_address, hour""", (since_id,)):
        g = groups.setdefault((user, addr), {"user_name": user, "device_name": dev,
                                             "device_address": addr, "hourly_avg_bpm": []})
        g["hourly_avg_bpm"].append({"hour": hour, "avg_bpm": avg, "min_bpm": lo, "max_bpm": hi, "samples": n})
    return groups


def build_export_by_sensor(since_id=0):
    """Agrupa las lecturas por sensor para exportar un archivo JSON individual por cada uno."""
    rows = query("""SELECT id, recorded_at_utc, user_name, device_name, device_address,
                           heart_rate_bpm, avg_bpm, max_bpm, calories_kcal, battery_percent
                    FROM readings WHERE id > ? ORDER BY id""", (since_id,))
    sensors = {}
    for _, ts, user, dev, addr, bpm, avg_b, max_b, cal, bat in rows:
        s = sensors.setdefault(addr, {
            "exported_at_utc": now_iso("seconds"),
            "user_name": user,
            "device_name": dev,
            "device_address": addr,
            "total_readings": 0,
            "hourly_avg_bpm": [],
            "readings": []
        })
        if user:
            s["user_name"] = user
        s["readings"].append({
            "t": ts,
            "bpm": bpm,
            "avg_bpm": avg_b,
            "max_bpm": max_b,
            "calories_kcal": cal,
            "battery": bat
        })
        s["total_readings"] += 1

    for (_, addr), h in hourly_bpm(since_id).items():
        if addr in sensors:
            sensors[addr]["hourly_avg_bpm"] = h["hourly_avg_bpm"]

    last_id = rows[-1][0] if rows else since_id
    return sensors, last_id


# ---------------------------------------------------------------- Parser BLE y Calorías
def parse_hr(d):
    return int.from_bytes(d[1:3] if d[0] & 1 else d[1:2], "little") or None


def calc_kcal_per_sec(bpm, weight_kg=70.0, age=25, gender="M"):
    """Fórmula de Keytel para estimar gasto calórico (kcal/s) mediante frecuencia cardíaca."""
    if not bpm or bpm < 40:
        return 0.0
    if str(gender).upper().startswith("F"):
        kcal_min = (-20.4022 + 0.4472 * bpm - 0.1263 * weight_kg + 0.074 * age) / 4.184
    else:
        kcal_min = (-55.0969 + 0.6309 * bpm + 0.1988 * weight_kg + 0.2017 * age) / 4.184
    return max(0.0, kcal_min / 60.0)


# ---------------------------------------------------------------- Clase Sensor
class Sensor:
    def __init__(self, device, cfg):
        self.address = device.address
        self.name = device.name or "XOSS"
        self.cfg = cfg
        self.client = None
        self.connected = False
        self.user_name = ""
        self.battery = None
        self.last_update = 0.0
        self.bpm = None
        self.bpm_sum = 0
        self.bpm_count = 0
        self.max_bpm = None
        self.calories_kcal = 0.0

    def on_hr(self, data):
        bpm = parse_hr(data)
        if bpm:
            now = time.time()
            if self.last_update > 0:
                dt = min(now - self.last_update, 10.0)
                if dt > 0:
                    self.calories_kcal += calc_kcal_per_sec(
                        bpm, self.cfg["user_weight_kg"], self.cfg["user_age"], self.cfg["user_gender"]
                    ) * dt
            self.last_update = now
            self.bpm = bpm
            self.bpm_sum += bpm
            self.bpm_count += 1
            if self.max_bpm is None or bpm > self.max_bpm:
                self.max_bpm = bpm

    @property
    def avg_bpm(self):
        return round(self.bpm_sum / self.bpm_count, 1) if self.bpm_count > 0 else None

    def sample(self, now, stale):
        if not self.connected or (now - self.last_update >= stale) or self.bpm is None:
            return None
        return {
            "bpm": self.bpm,
            "avg_bpm": self.avg_bpm,
            "max_bpm": self.max_bpm,
            "calories_kcal": round(self.calories_kcal, 2),
            "battery": self.battery
        }

    def to_dict(self, now, stale):
        receiving = self.connected and (now - self.last_update < stale)
        return {
            "address": self.address,
            "name": self.name,
            "user_name": self.user_name,
            "connected": self.connected,
            "receiving": receiving,
            "bpm": self.bpm if receiving else None,
            "avg_bpm": self.avg_bpm,
            "max_bpm": self.max_bpm,
            "calories_kcal": round(self.calories_kcal, 1),
            "battery": self.battery
        }


class State:
    def __init__(self, cfg):
        self.cfg = cfg
        self.known = {}
        self.sensors = {}
        self.ws_clients = set()
        self.saved = 0
        self.last_export_id = 0
        self.export_counts = {}  # address -> consecutivo (#) de exportación
        self.total_rows = query("SELECT COUNT(*) FROM readings")[0][0]
        self.names = json.loads(USERS_PATH.read_text("utf-8")) if USERS_PATH.exists() else {}

    def snapshot(self):
        c, now = self.cfg, time.time()
        return {
            "saved_this_session": self.saved,
            "total_rows": self.total_rows + self.saved,
            "max_sensors": c["max_sensors"],
            "save_interval_seconds": c["save_interval_seconds"],
            "auto_export_seconds": c["auto_json_export_every_seconds"],
            "sensors": [s.to_dict(now, c["stale_after_seconds"]) for s in self.sensors.values()]
        }


async def connect_sensor(st, address):
    if address in st.sensors and st.sensors[address].connected:
        return
    if sum(s.connected for s in st.sensors.values()) >= st.cfg["max_sensors"]:
        raise RuntimeError(f"Máximo de {st.cfg['max_sensors']} sensores conectados.")
    if address not in st.known:
        raise RuntimeError("Dispositivo desconocido. Busca sensores primero.")

    device = st.known[address]
    sensor = st.sensors[address] = Sensor(device, st.cfg)
    sensor.user_name = st.names.get(address, "")
    sensor.client = BleakClient(device, disconnected_callback=lambda _c: setattr(sensor, "connected", False))
    try:
        await sensor.client.connect()
        services = sensor.client.services
        if not services.get_service(HR_SERVICE):
            raise RuntimeError("El dispositivo no expone el servicio de Frecuencia Cardíaca (180D).")

        await sensor.client.start_notify(HR_CHAR, lambda _s, d: sensor.on_hr(d))
        sensor.connected = True

        if services.get_service(BATTERY_SERVICE):
            try:
                sensor.battery = (await sensor.client.read_gatt_char(BATTERY_LEVEL))[0]
            except Exception:
                pass
    except Exception:
        st.sensors.pop(address, None)
        await disconnect_client(sensor)
        raise


async def disconnect_client(sensor):
    try:
        await sensor.client.disconnect()
    except Exception:
        pass


# ---------------------------------------------------------------- Tareas en segundo plano
async def saver_loop(st):
    """Guarda automáticamente en SQLite desde el momento en que un sensor está conectado y enviando BPM."""
    while True:
        await asyncio.sleep(st.cfg["save_interval_seconds"])
        c, now, stamp = st.cfg, time.time(), now_iso("milliseconds")
        rows = []
        for s in list(st.sensors.values()):
            v = s.sample(now, c["stale_after_seconds"])
            if v:
                rows.append((
                    stamp, s.user_name or None, s.name, s.address,
                    v["bpm"], v["avg_bpm"], v["max_bpm"], v["calories_kcal"], v["battery"]
                ))
        if rows:
            await asyncio.to_thread(query, INSERT, rows, True)
            st.saved += len(rows)


async def broadcast_loop(st):
    """Actualiza el dashboard en vivo por WebSocket."""
    while True:
        await asyncio.sleep(st.cfg["dashboard_refresh_seconds"])
        if not st.ws_clients:
            continue
        payload = json.dumps(st.snapshot())
        for ws in list(st.ws_clients):
            try:
                await ws.send_str(payload)
            except Exception:
                st.ws_clients.discard(ws)


async def write_sensor_exports(st, since_id=0):
    """Crea un archivo [Nombre_hora_#].json por cada sensor con datos."""
    sensors_data, last_id = await asyncio.to_thread(build_export_by_sensor, since_id)
    if not sensors_data:
        return [], since_id, 0

    folder = BASE / st.cfg["export_dir"]
    folder.mkdir(exist_ok=True)
    hora_str = datetime.now().strftime("%H-%M")
    saved_files = []
    total_count = 0

    for addr, data in sensors_data.items():
        st.export_counts[addr] = st.export_counts.get(addr, 0) + 1
        num = st.export_counts[addr]
        raw_label = data["user_name"] or (st.sensors[addr].user_name if addr in st.sensors else "") or data["device_name"] or addr[-5:]
        nombre = clean_name(raw_label)

        filename = f"{nombre}_{hora_str}_{num}.json"
        path = folder / filename
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False), "utf-8")
        saved_files.append(filename)
        total_count += data["total_readings"]

    return saved_files, last_id, total_count


async def auto_export_loop(st):
    """Exporta automáticamente cada hora (3600s por defecto) los datos nuevos de cada sensor."""
    every = float(st.cfg["auto_json_export_every_seconds"])
    while every > 0:
        await asyncio.sleep(every)
        files, st.last_export_id, n = await write_sensor_exports(st, st.last_export_id)
        if files:
            print(f"[Auto-Export] {n} lecturas guardadas en: {', '.join(files)}")


# ---------------------------------------------------------------- Rutas HTTP
def api(fn):
    async def handler(request):
        try:
            body = await request.json() if request.can_read_body else {}
            return web.json_response({"ok": True, **(await fn(request.app["state"], body) or {})})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=400)
    return handler


@api
async def connect(st, b):
    await connect_sensor(st, b["address"])


@api
async def disconnect(st, b):
    sensor = st.sensors.pop(b["address"], None)
    if sensor:
        await disconnect_client(sensor)


@api
async def set_name(st, b):
    addr, name = b["address"], str(b.get("name", "")).strip()[:60]
    st.names.pop(addr, None)
    if name:
        st.names[addr] = name
    USERS_PATH.write_text(json.dumps(st.names, indent=2, ensure_ascii=False), "utf-8")
    if addr in st.sensors:
        st.sensors[addr].user_name = name


@api
async def export_save(st, b):
    files, _, n = await write_sensor_exports(st, 0)
    if not files:
        raise RuntimeError("No hay lecturas guardadas todavía.")
    return {"files": files, "readings": n}


async def index(request):
    return web.FileResponse(HTML_PATH)


async def scan(request):
    st = request.app["state"]
    found = await BleakScanner.discover(timeout=st.cfg["scan_timeout_seconds"])
    found.sort(key=lambda d: (not any(w in (d.name or "").lower() for w in ("xoss", "heart")), (d.name or "").lower()))
    st.known = {d.address: d for d in found}
    return web.json_response([{"address": d.address, "name": d.name or "Sin nombre"} for d in found])


async def hourly(request):
    return web.json_response(list((await asyncio.to_thread(hourly_bpm)).values()))


async def export_download(request):
    sensors_data, _ = await asyncio.to_thread(build_export_by_sensor, 0)
    hora_str = datetime.now().strftime("%H-%M")
    payload = {"exported_at_utc": now_iso("seconds"), "sensors": list(sensors_data.values())}
    return web.Response(
        text=json.dumps(payload, indent=2, ensure_ascii=False),
        content_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="Sensores_{hora_str}_1.json"'}
    )


async def ws_handler(request):
    st, ws = request.app["state"], web.WebSocketResponse()
    await ws.prepare(request)
    st.ws_clients.add(ws)
    await ws.send_str(json.dumps(st.snapshot()))
    try:
        async for _ in ws:
            pass
    finally:
        st.ws_clients.discard(ws)
    return ws


async def on_startup(app):
    app["tasks"] = [asyncio.create_task(f(app["state"])) for f in (saver_loop, broadcast_loop, auto_export_loop)]


async def on_cleanup(app):
    for t in app["tasks"]:
        t.cancel()
    for s in list(app["state"].sensors.values()):
        await disconnect_client(s)


def make_app(cfg):
    app = web.Application()
    app["state"] = State(cfg)
    app.add_routes([
        web.get("/", index),
        web.get("/ws", ws_handler),
        web.get("/api/scan", scan),
        web.get("/api/export.json", export_download),
        web.get("/api/hourly_bpm", hourly),
        web.post("/api/connect", connect),
        web.post("/api/disconnect", disconnect),
        web.post("/api/name", set_name),
        web.post("/api/export/save", export_save)
    ])
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    cfg = load_config()
    init_db()
    url = f"http://localhost:{cfg['port']}"
    print(f"Servidor XOSS activo en: {url}")
    print("No cierres esta ventana mientras estes registrando datos.")
    # Abre el navegador automáticamente 1.5 segundos después de iniciar el programa
    threading.Timer(1.5, lambda: webbrowser.open(url)).start()
    web.run_app(make_app(cfg), host=cfg["host"], port=cfg["port"])
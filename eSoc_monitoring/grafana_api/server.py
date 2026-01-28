from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request

app = FastAPI(title="eSoc Grafana API")

# ----------------------------
# Paths del proyecto
# ----------------------------
# Estructura esperada:
# eSoc_monitoring/
#   config.py
#   output/snapshots/
#   grafana_api/server.py (este archivo)

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# 1) Si existe variable de entorno ESOC_SNAPSHOT_DIR, la usamos.
# 2) Si no, usamos una ruta fija (tu caso actual) para evitar que tome otro repo/copia.
# 3) Si esa ruta no existe, caemos al default por PROJECT_ROOT.
ENV_SNAP = os.environ.get("ESOC_SNAPSHOT_DIR", "").strip()

HARDCODED_SNAP_DIR = Path(
    r"C:\Users\mgallegi\OneDrive - Nokia\Nokia\05 - Softwares\02 - Automatizacion\eSoc_monitoring\output\snapshots"
)

if ENV_SNAP:
    SNAP_DIR = Path(ENV_SNAP).expanduser()
elif HARDCODED_SNAP_DIR.exists():
    SNAP_DIR = HARDCODED_SNAP_DIR
else:
    SNAP_DIR = PROJECT_ROOT / "output" / "snapshots"

# Para poder importar config.py desde grafana_api/
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config import SERVERS
except Exception as e:
    SERVERS = {}
    print(f"[WARN] No pude importar SERVERS desde config.py: {e}")


# ----------------------------
# Helpers
# ----------------------------
def _latest_snapshot_file() -> Path:
    if not SNAP_DIR.exists():
        raise HTTPException(status_code=500, detail=f"Snapshot dir no existe: {SNAP_DIR}")
    latest = SNAP_DIR / "snapshot_latest.json"
    if not latest.exists():
        raise HTTPException(status_code=404, detail=f"No existe {latest}")
    return latest


def _load_latest_snapshot() -> Dict[str, Any]:
    f = _latest_snapshot_file()
    return json.loads(f.read_text(encoding="utf-8"))


def _status_to_num(s: str) -> int:
    s = (s or "").upper()
    if s == "OK":
        return 0
    if s == "WARN":
        return 1
    return 2  # FAIL o unknown


def _find_result(snapshot: Dict[str, Any], check_name: str) -> Optional[Dict[str, Any]]:
    for r in snapshot.get("results", []):
        if r.get("name") == check_name:
            return r
    return None


def _server_label(server_key: str) -> str:
    """
    Devuelve etiqueta amigable (Nombre + IP) usando SERVERS del config.py.
    Si no existe, devuelve el server_key tal cual.
    """
    if not server_key:
        return "unknown"
    s = SERVERS.get(server_key, {})
    return s.get("label") or server_key


def _snapshot_time_iso(snapshot: Dict[str, Any]) -> str:
    return snapshot.get("timestamp") or snapshot.get("timestamp_local") or ""


def _snapshot_epoch_ms(snapshot: Dict[str, Any]) -> int:
    ts_iso = _snapshot_time_iso(snapshot)
    if not ts_iso:
        return 0
    try:
        import datetime as dt

        t = dt.datetime.fromisoformat(ts_iso)
        return int(t.timestamp() * 1000)
    except Exception:
        return 0


def _parse_dt(s: str):
    """
    Soporta:
    - '2026-01-28 15:00:14'
    - '2026-01-28T12:00:14.464385'
    """
    if not s:
        return None
    s = s.strip()
    try:
        import datetime as dt

        return dt.datetime.fromisoformat(s)
    except Exception:
        pass

    try:
        import datetime as dt

        return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _age_minutes(now_s: str, maxvalue_s: str) -> Optional[float]:
    import datetime as dt

    now_dt = _parse_dt(now_s)
    mv_dt = _parse_dt(maxvalue_s)
    if not now_dt or not mv_dt:
        return None
    delta = now_dt - mv_dt
    return round(delta.total_seconds() / 60.0, 2)


# ----------------------------
# Endpoints requeridos por simpod-json-datasource
# ----------------------------
@app.get("/")
def root():
    # útil para debug rápido
    try:
        snap_file = _latest_snapshot_file()
        snap = _load_latest_snapshot()
        boundary = [x for x in snap.get("results", []) if x.get("name") == "boundary"]
        rows_len = 0
        if boundary:
            rows_len = len(((boundary[0].get("details") or {}).get("rows") or []))
        return {
            "status": "ok",
            "snap_file": str(snap_file),
            "snap_timestamp": snap.get("timestamp"),
            "boundary_rows_len": rows_len,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/search")
def search(_: Any = None):
    # llena el dropdown "Metric"
    return [
        "checks_table",                 # tabla simple: time/server/status
        "check_status",                 # status_num global o por check (serie 1 punto)
        "nelmon_boot_use_percent",      # % /boot (requiere payload.check)
        "boundary_newest_age_minutes",  # serie 1 punto (requiere payload.check)
        "boundary_table",               # tabla: Job/Last Data/Region + age_min
    ]


@app.post("/query")
async def query(request: Request):
    """
    Endpoint principal que consulta Grafana.

    Grafana envía un JSON con "targets" y otros campos (range, interval, etc).
    Nos interesa "targets": [{ "target": "...", "payload": {...}}]
    """
    body = await request.json()
    targets = body.get("targets", [])

    snapshot = _load_latest_snapshot()
    ts_iso = _snapshot_time_iso(snapshot)
    epoch_ms = _snapshot_epoch_ms(snapshot)

    out: List[Dict[str, Any]] = []

    for tgt in targets:
        metric = tgt.get("target")
        payload = tgt.get("payload") or {}
        check_name = payload.get("check")

        # ----------------------------
        # checks_table -> TABLE simple
        # ----------------------------
        if metric == "checks_table":
            columns = [
                {"text": "time"},
                {"text": "server"},
                {"text": "status"},
            ]

            rows = []
            for r in snapshot.get("results", []):
                status = r.get("status", "FAIL")
                server_key = r.get("server")
                rows.append([
                    ts_iso,
                    _server_label(server_key),
                    status,
                ])

            out.append({
                "type": "table",
                "columns": columns,
                "rows": rows,
            })

        # ----------------------------
        # check_status -> serie 1 punto (0/1/2)
        # - sin payload.check -> global
        # - con payload.check -> ese check
        # ----------------------------
        elif metric == "check_status":
            if check_name:
                r = _find_result(snapshot, check_name)
                if not r:
                    out.append({"target": f"{check_name}.status_num", "datapoints": []})
                else:
                    s = r.get("status", "FAIL")
                    out.append({
                        "target": f"{check_name}.status_num",
                        "datapoints": [[_status_to_num(s), epoch_ms]],
                    })
            else:
                gs = snapshot.get("global_status", "FAIL")
                out.append({
                    "target": "global.status_num",
                    "datapoints": [[_status_to_num(gs), epoch_ms]],
                })

        # ----------------------------
        # nelmon_boot_use_percent -> serie 1 punto
        # ----------------------------
        elif metric == "nelmon_boot_use_percent":
            if not check_name:
                raise HTTPException(status_code=400, detail='Payload requerido: {"check":"nelmon_check_1"}')
            r = _find_result(snapshot, check_name)
            val = (r.get("metrics") or {}).get("boot_use_percent") if r else None
            if val is None:
                out.append({"target": f"{check_name}.boot_use_percent", "datapoints": []})
            else:
                out.append({"target": f"{check_name}.boot_use_percent", "datapoints": [[val, epoch_ms]]})

        # ----------------------------
        # boundary_newest_age_minutes -> serie 1 punto (si existe)
        # ----------------------------
        elif metric == "boundary_newest_age_minutes":
            if not check_name:
                raise HTTPException(status_code=400, detail='Payload requerido: {"check":"boundary"}')
            r = _find_result(snapshot, check_name)
            val = (r.get("metrics") or {}).get("newest_age_minutes") if r else None
            if val is None:
                out.append({"target": f"{check_name}.newest_age_minutes", "datapoints": []})
            else:
                out.append({"target": f"{check_name}.newest_age_minutes", "datapoints": [[val, epoch_ms]]})

        # ----------------------------
        # boundary_table -> TABLE detalle
        # Devuelve además age_min (para colorear por thresholds)
        # ----------------------------
        elif metric == "boundary_table":
            r = _find_result(snapshot, "boundary")
            details = (r.get("details") or {}) if r else {}
            rows_src = details.get("rows") or []
            now_local = details.get("now_local") or _snapshot_time_iso(snapshot)

            columns = [
                {"text": "Job", "type": "string"},
                {"text": "Last Data", "type": "string"},
                {"text": "Region", "type": "string"},
                {"text": "age_min", "type": "number"},  # <- usar para colores
            ]

            rows: List[List[Any]] = []
            for rr in rows_src:
                jobid = (rr.get("jobid") or "").strip()
                maxvalue = (rr.get("maxvalue") or "").strip()
                region_id = (rr.get("region_id") or "").strip()
                age = _age_minutes(now_local, maxvalue)

                rows.append([jobid, maxvalue, region_id, age])

            out.append({
                "type": "table",
                "columns": columns,
                "rows": rows,
            })

        else:
            # métrica desconocida -> vacía
            out.append({"target": str(metric), "datapoints": []})

    return out

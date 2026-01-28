"""
main.py

Runner principal del proyecto eSoc_monitoring.

- Lee SERVERS y CHECKS desde config.py
- Ejecuta checks
- Guarda snapshot_latest.json
- Loggea cada ejecución a output/logs/esoc_monitoring.log (rotativo)
- Exit code:
    0 = OK
    1 = WARN
    2 = FAIL
"""

import json
import datetime as dt
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import traceback
import time

from config import SERVERS, CHECKS

from checks.k8s_dis_nci import run as run_k8s_dis_nci
from checks.nelmon_check import run as run_nelmon_check
from checks.boundary import run as run_boundary


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def ensure_output_dirs(output_dirs: dict) -> None:
    Path(output_dirs["raw"]).mkdir(parents=True, exist_ok=True)
    Path(output_dirs["snapshots"]).mkdir(parents=True, exist_ok=True)
    Path(output_dirs["logs"]).mkdir(parents=True, exist_ok=True)


def status_rank(status: str) -> int:
    return {"OK": 0, "WARN": 1, "FAIL": 2}.get(status, 2)


def setup_logging(log_dir: str) -> logging.Logger:
    logger = logging.getLogger("esoc_monitoring")
    logger.setLevel(logging.INFO)

    # Evitar duplicar handlers si corrés main varias veces en la misma sesión
    if logger.handlers:
        return logger

    log_path = Path(log_dir) / "esoc_monitoring.log"

    fh = RotatingFileHandler(
        log_path,
        maxBytes=5 * 1024 * 1024,  # 5MB
        backupCount=5,
        encoding="utf-8",
    )
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():
    output_dirs = {
        "raw": "output/raw",
        "snapshots": "output/snapshots",
        "logs": "output/logs",
    }
    ensure_output_dirs(output_dirs)
    log = setup_logging(output_dirs["logs"])

    run_ts = dt.datetime.now().isoformat()
    log.info(f"RUN_START | ts={run_ts}")

    results = []
    worst_status = "OK"

    for check_name, cfg in CHECKS.items():
        check_type = cfg["type"]
        server_name = cfg["server"]
        ssh_cfg = SERVERS.get(server_name)

        if not ssh_cfg:
            msg = f"Servidor '{server_name}' no definido en SERVERS"
            log.error(f"CHECK_FAIL | name={check_name} type={check_type} server={server_name} err={msg}")
            results.append({
                "name": check_name,
                "type": check_type,
                "server": server_name,
                "status": "FAIL",
                "metrics": {},
                "details": {"error": msg},
                "raw_file": None,
            })
            worst_status = "FAIL"
            continue

        start = time.time()
        try:
            if check_type == "k8s_dis_nci":
                k8s_cfg = {"namespace": cfg["namespace"], "grep_patterns": cfg["grep_patterns"]}
                result = run_k8s_dis_nci(ssh_cfg, k8s_cfg, output_dirs)

            elif check_type == "nelmon_check":
                if not ssh_cfg.get("password"):
                    raise RuntimeError(
                        f"Password vacío para {server_name}. "
                        "Definí la variable de entorno NELMON_PASS o config."
                    )
                result = run_nelmon_check(ssh_cfg, output_dirs)

            elif check_type == "boundary":
                result = run_boundary(ssh_cfg, output_dirs)

            else:
                raise RuntimeError(f"Tipo de check no soportado: {check_type}")

            elapsed = round(time.time() - start, 2)

            # Adjuntamos duración al details para diagnóstico
            try:
                result.details["duration_sec"] = elapsed
            except Exception:
                pass

            results.append({
                "name": check_name,
                "type": check_type,
                "server": server_name,
                "status": result.status,
                "metrics": result.metrics,
                "details": result.details,
                "raw_file": result.raw_file,
            })

            log.info(
                f"CHECK_DONE | name={check_name} type={check_type} server={server_name} "
                f"status={result.status} dur_sec={elapsed} raw={result.raw_file}"
            )

            if status_rank(result.status) > status_rank(worst_status):
                worst_status = result.status

        except Exception as e:
            elapsed = round(time.time() - start, 2)
            log.error(
                f"CHECK_EXC | name={check_name} type={check_type} server={server_name} "
                f"dur_sec={elapsed} err={e}"
            )
            log.debug(traceback.format_exc())

            results.append({
                "name": check_name,
                "type": check_type,
                "server": server_name,
                "status": "FAIL",
                "metrics": {},
                "details": {"error": str(e), "duration_sec": elapsed},
                "raw_file": None,
            })
            worst_status = "FAIL"

    snapshot = {
        "timestamp": dt.datetime.now().isoformat(),
        "global_status": worst_status,
        "results": results,
    }

    snap_file = Path(output_dirs["snapshots"]) / "snapshot_latest.json"
    snap_file.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding="utf-8")

    log.info(f"RUN_END | global_status={worst_status} snapshot={snap_file}")

    print(f"[+] Snapshot guardado: {snap_file}")
    print(f"[+] Global status    : {worst_status}\n")
    for r in results:
        print(f"- {r['name']} [{r['server']}] -> {r['status']}  metrics={r['metrics']}")

    raise SystemExit(status_rank(worst_status))


if __name__ == "__main__":
    main()

"""
checks/boundary.py

Check "boundary":
- Conecta a host (ej: 10.92.180.98) por SSH (private key)
- Salta a ciap01 (nested ssh)
- Ejecuta psql con query boundary
- Guarda SIEMPRE output/raw/boundary_latest.txt

Además:
- Filtra banners/warnings del login para que el raw muestre SOLO lo útil
- Calcula newest_age_minutes
- IMPORTANTE: guarda details["rows"] con columnas jobid/maxvalue/region_id
"""

from __future__ import annotations

import datetime as dt
import re
import shlex
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import paramiko


# ==========================
# Result type
# ==========================
@dataclass
class CheckResult:
    status: str                 # "OK" | "WARN" | "FAIL"
    metrics: Dict[str, Any]
    details: Dict[str, Any]
    raw_file: str | None


# ==========================
# Config: SQL
# ==========================
BOUNDARY_SQL = (
    "SELECT jobid, maxvalue, region_id "
    "FROM boundary "
    "WHERE jobid LIKE '%Usage%' "
    "  AND maxvalue IS NOT NULL "
    "  AND region_id <> 'Unknown' "
    "ORDER BY maxvalue;"
)

# Umbral para WARN (minutos de atraso)
THRESHOLD_MINUTES = 15


# ==========================
# SSH helpers
# ==========================
def load_private_key(key_path: str) -> paramiko.PKey:
    last_err = None
    for cls in (paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey):
        try:
            return cls.from_private_key_file(key_path)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"No pude cargar la private key: {last_err}")


def _read_channel(stdout, stderr, *, channel_timeout=20, read_timeout=120) -> Tuple[str, str, int]:
    ch = stdout.channel
    ch.settimeout(channel_timeout)

    out_chunks: List[bytes] = []
    err_chunks: List[bytes] = []
    start = time.time()

    while True:
        if time.time() - start > read_timeout:
            try:
                ch.close()
            except Exception:
                pass
            raise TimeoutError(f"Timeout ejecutando boundary (>{read_timeout}s)")

        try:
            if ch.recv_ready():
                out_chunks.append(ch.recv(4096))
            if ch.recv_stderr_ready():
                err_chunks.append(ch.recv_stderr(4096))
            if ch.exit_status_ready():
                break
            time.sleep(0.1)
        except socket.timeout:
            pass

    exit_code = ch.recv_exit_status()
    out = b"".join(out_chunks).decode("utf-8", errors="replace")
    err = b"".join(err_chunks).decode("utf-8", errors="replace")
    return out, err, exit_code


def ssh_exec(client: paramiko.SSHClient, command: str, *, read_timeout: int = 120) -> Tuple[str, str, int]:
    _stdin, stdout, stderr = client.exec_command(command, get_pty=False)
    return _read_channel(stdout, stderr, channel_timeout=20, read_timeout=read_timeout)


# ==========================
# Output filtering / parsing
# ==========================
_BANNER_PATTERNS = [
    r"^#{10,}.*$",
    r"^WARNING\s*!.*$",
    r"^You are about to access.*$",
    r"^This system is for.*$",
    r"^authorized users only.*$",
    r"^All connections, actions.*$",
    r"^be logged and monitored.*$",
    r"^By accessing and using.*$",
    r"^Users should have no expectation.*$",
    r"^Last login:.*$",
]
_BANNER_RE = re.compile("|".join(f"(?:{p})" for p in _BANNER_PATTERNS), re.IGNORECASE)


def filter_boundary_output(text: str) -> str:
    out_lines: List[str] = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            out_lines.append("")
            continue
        if _BANNER_RE.match(s):
            continue
        if s == "##############################################################################":
            continue
        out_lines.append(line)
    return "\n".join(out_lines).lstrip("\n")


def _extract_now_local(text: str) -> dt.datetime | None:
    m = re.search(r"NOW_LOCAL=(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", text)
    if not m:
        return None
    try:
        return dt.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def parse_psql_table(text: str) -> List[Dict[str, str]]:
    """
    Parse del output estándar aligned de psql:

      jobid | maxvalue | region_id
     ------+----------+----------
      ...  | ...      | ...
     (22 rows)

    Devuelve:
      [{"jobid": "...", "maxvalue": "...", "region_id": "..."}, ...]
    """
    lines = [ln.rstrip("\n") for ln in text.splitlines()]

    header_idx = None
    sep_idx = None

    for i, ln in enumerate(lines):
        if ("jobid" in ln) and ("maxvalue" in ln) and ("region_id" in ln) and ("|" in ln):
            header_idx = i
            if i + 1 < len(lines) and ("+" in lines[i + 1]) and ("-" in lines[i + 1]):
                sep_idx = i + 1
            break

    if header_idx is None or sep_idx is None:
        return []

    def split_row(row_line: str) -> List[str]:
        return [c.strip() for c in row_line.split("|")]

    rows: List[Dict[str, str]] = []
    for ln in lines[sep_idx + 1 :]:
        s = ln.strip()
        if not s:
            continue
        if s.startswith("(") and s.endswith("rows)"):
            break
        if "|" not in ln:
            continue

        parts = split_row(ln)
        if len(parts) < 3:
            continue

        jobid, maxvalue, region_id = parts[0], parts[1], parts[2]
        if not jobid or not maxvalue or not region_id:
            continue

        rows.append({"jobid": jobid, "maxvalue": maxvalue, "region_id": region_id})

    return rows


def extract_newest_maxvalue(rows: List[Dict[str, str]]) -> dt.datetime | None:
    parsed: List[dt.datetime] = []
    for r in rows:
        mv = (r.get("maxvalue") or "").strip()
        if not mv:
            continue
        try:
            parsed.append(dt.datetime.strptime(mv, "%Y-%m-%d %H:%M:%S"))
        except Exception:
            pass
    return max(parsed) if parsed else None


def _format_raw(host: str, user: str, jump_target: str, stdout: str, stderr: str, exit_code: int) -> str:
    ts = dt.datetime.now().isoformat()
    return (
        f"TimestampLocal: {ts}\n"
        f"Check: boundary\n"
        f"Host: {host}\n"
        f"User: {user}\n"
        f"JumpTarget: {jump_target}\n"
        f"ExitCode: {exit_code}\n\n"
        f"--- stdout ---\n{stdout}\n\n"
        f"--- stderr ---\n{stderr if stderr else '(vacío)'}\n"
    )


# ==========================
# Main entrypoint
# ==========================
def run(ssh_cfg: dict, output_dirs: dict) -> CheckResult:
    raw_path = Path(output_dirs["raw"]) / "boundary_latest.txt"

    host = ssh_cfg["host"]
    port = ssh_cfg.get("port", 22)
    user = ssh_cfg["user"]
    key_path = ssh_cfg["key_path"]

    # target dentro del primer host
    jump_target = ssh_cfg.get("jump_target", "ciap01")

    # ssh opts para evitar cuelgues
    ssh_opts = (
        "-q "
        "-o StrictHostKeyChecking=no "
        "-o UserKnownHostsFile=/dev/null "
        "-o ConnectTimeout=10 "
        "-o ServerAliveInterval=10 "
        "-o ServerAliveCountMax=2 "
        "-T"
    )

    # sudo no interactivo + imprimimos NOW_LOCAL para calcular age
    cmd_inside_jump = (
        "sudo -n bash -lc "
        + shlex.quote(
            "date '+NOW_LOCAL=%Y-%m-%d %H:%M:%S'; "
            f"psql sai sairepo -c {shlex.quote(BOUNDARY_SQL)}"
        )
    )

    remote_cmd = (
        "bash -lc "
        + shlex.quote(
            f"ssh {ssh_opts} {jump_target} {shlex.quote(cmd_inside_jump)}"
        )
    )

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    stdout = ""
    stderr = ""
    exit_code = 255

    try:
        pkey = load_private_key(key_path)

        client.connect(
            hostname=host,
            port=port,
            username=user,
            pkey=pkey,
            timeout=10,
            auth_timeout=10,
            banner_timeout=10,
            look_for_keys=False,
            allow_agent=False,
        )

        stdout, stderr, exit_code = ssh_exec(client, remote_cmd, read_timeout=120)

        filtered_stdout = filter_boundary_output(stdout)
        filtered_stderr = filter_boundary_output(stderr)

        raw_path.write_text(
            _format_raw(host, user, jump_target, filtered_stdout, filtered_stderr, exit_code),
            encoding="utf-8",
        )

        if exit_code != 0:
            return CheckResult(
                status="FAIL",
                metrics={},
                details={"error": f"Comando remoto falló (exit_code={exit_code}). Ver raw_file."},
                raw_file=str(raw_path),
            )

        # ✅ Parsear filas para el panel boundary_table
        rows = parse_psql_table(filtered_stdout)

        now_local = _extract_now_local(filtered_stdout) or dt.datetime.now()
        newest = extract_newest_maxvalue(rows)

        # Si no hay datos parseables, igual devolvemos las filas (si hubiera) + WARN
        if newest is None:
            return CheckResult(
                status="WARN",
                metrics={"newest_age_minutes": None},
                details={
                    "message": "No pude calcular newest_age_minutes (maxvalue no parseable o sin filas).",
                    "rows": rows,  # <-- CLAVE para Grafana boundary_table
                    "raw_exit_code": exit_code,
                },
                raw_file=str(raw_path),
            )

        age_min = (now_local - newest).total_seconds() / 60.0
        status = "OK" if age_min <= THRESHOLD_MINUTES else "WARN"

        return CheckResult(
            status=status,
            metrics={"newest_age_minutes": round(age_min, 2)},
            details={
                "now_local": now_local.isoformat(sep=" "),
                "newest_maxvalue": newest.isoformat(sep=" "),
                "threshold_minutes": THRESHOLD_MINUTES,
                "rows": rows,  # <-- CLAVE para Grafana boundary_table
                "raw_exit_code": exit_code,
            },
            raw_file=str(raw_path),
        )

    except Exception as e:
        try:
            raw_path.write_text(
                _format_raw(
                    host,
                    user,
                    jump_target,
                    filter_boundary_output(stdout),
                    f"{filter_boundary_output(stderr)}\nEXCEPTION: {e}",
                    exit_code,
                ),
                encoding="utf-8",
            )
        except Exception:
            pass

        return CheckResult(
            status="FAIL",
            metrics={},
            details={"error": str(e), "raw_exit_code": exit_code},
            raw_file=str(raw_path),
        )

    finally:
        # SIEMPRE cerrar sesión SSH
        try:
            client.close()
        except Exception:
            pass

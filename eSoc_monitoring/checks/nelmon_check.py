"""
checks/nelmon_check.py

Check de servidores Nelmon:
- SSH user/pass
- Ejecuta: uptime ; df -h
- Parsea df -h y extrae uso de /boot
- OK/WARN/FAIL según %
- Guarda raw

Importante:
- La conexión SSH SIEMPRE se cierra
- No se cuelga: timeouts + lectura por chunks
"""

import re
import paramiko
import datetime as dt
import time
import socket
from pathlib import Path
from typing import Tuple, Dict, Optional

from .base import CheckResult

BOOT_RE = re.compile(
    r"^(?P<fs>\S+)\s+(?P<size>\S+)\s+(?P<used>\S+)\s+(?P<avail>\S+)\s+(?P<usep>\d+)%\s+(?P<mount>/boot)\s*$"
)

def quote_for_bash(cmd: str) -> str:
    return "'" + cmd.replace("'", "'\"'\"'") + "'"

def _read_channel(stdout, stderr, *, channel_timeout=20, read_timeout=60) -> Tuple[str, str, int]:
    ch = stdout.channel
    ch.settimeout(channel_timeout)

    out_chunks, err_chunks = [], []
    start = time.time()

    while True:
        if time.time() - start > read_timeout:
            try:
                ch.close()
            except Exception:
                pass
            raise TimeoutError(f"Timeout leyendo salida SSH (>{read_timeout}s)")

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

def ssh_run(host: str, port: int, user: str, password: str, cmd: str,
            *, connect_timeout=10, channel_timeout=20, read_timeout=60, tries=2) -> Tuple[str, str, int]:
    last_err: Optional[Exception] = None

    for attempt in range(1, tries + 1):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            client.connect(
                hostname=host,
                port=port,
                username=user,
                password=password,
                timeout=connect_timeout,
                banner_timeout=connect_timeout,
                auth_timeout=connect_timeout,
                look_for_keys=False,
                allow_agent=False,
            )

            remote_cmd = f"bash -lc {quote_for_bash(cmd)}"
            stdin, stdout, stderr = client.exec_command(remote_cmd, get_pty=False)
            return _read_channel(stdout, stderr, channel_timeout=channel_timeout, read_timeout=read_timeout)

        except Exception as e:
            last_err = e
            time.sleep(1.5 * attempt)

        finally:
            try:
                client.close()
            except Exception:
                pass

    raise RuntimeError(f"SSH nelmon_check falló tras {tries} intentos: {last_err}")

def write_raw_file(path: Path, host: str, stdout: str, stderr: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write(f"Host: {host}\n")
        f.write(f"TimestampLocal: {dt.datetime.now().isoformat()}\n\n")
        f.write("--- stdout ---\n")
        f.write(stdout if stdout else "(vacío)\n")
        f.write("\n--- stderr ---\n")
        f.write(stderr if stderr else "(vacío)\n")

def parse_boot_usage(df_output: str) -> Dict:
    for line in df_output.splitlines():
        m = BOOT_RE.match(line.strip())
        if m:
            return {
                "filesystem": m.group("fs"),
                "size": m.group("size"),
                "used": m.group("used"),
                "avail": m.group("avail"),
                "use_percent": int(m.group("usep")),
                "mount": m.group("mount"),
            }
    return {}

def compute_status(use_percent: int) -> str:
    if use_percent >= 100:
        return "FAIL"
    if use_percent >= 90:
        return "WARN"
    return "OK"

def run(ssh_cfg: dict, output_dirs: dict) -> CheckResult:
    raw_file = Path(output_dirs["raw"]) / f"nelmon_{ssh_cfg['host']}_latest.txt"
    cmd = "uptime ; df -h"

    stdout = ""
    stderr = ""
    exit_code = 255

    try:
        stdout, stderr, exit_code = ssh_run(
            host=ssh_cfg["host"],
            port=ssh_cfg["port"],
            user=ssh_cfg["user"],
            password=ssh_cfg["password"],
            cmd=cmd,
            connect_timeout=10,
            channel_timeout=20,
            read_timeout=60,
            tries=2,
        )

        write_raw_file(raw_file, ssh_cfg["host"], stdout, stderr)

        boot = parse_boot_usage(stdout)
        if not boot:
            return CheckResult(
                name="nelmon_check",
                status="FAIL",
                metrics={"boot_use_percent": -1.0},
                details={"error": "No se encontró /boot en df -h", "raw_exit_code": exit_code},
                raw_file=str(raw_file),
            )

        status = compute_status(boot["use_percent"])
        return CheckResult(
            name="nelmon_check",
            status=status,
            metrics={"boot_use_percent": float(boot["use_percent"])},
            details={
                "filesystem": boot["filesystem"],
                "mount": "/boot",
                "size": boot["size"],
                "used": boot["used"],
                "avail": boot["avail"],
                "raw_exit_code": exit_code,
            },
            raw_file=str(raw_file),
        )

    except Exception as e:
        try:
            write_raw_file(raw_file, ssh_cfg["host"], stdout, f"{stderr}\nEXCEPTION: {e}")
        except Exception:
            pass

        return CheckResult(
            name="nelmon_check",
            status="FAIL",
            metrics={},
            details={"error": str(e), "raw_exit_code": exit_code},
            raw_file=str(raw_file),
        )

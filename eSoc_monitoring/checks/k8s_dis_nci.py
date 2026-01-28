import re
import paramiko
import datetime as dt
import time
import socket
from pathlib import Path
from typing import List, Tuple, Dict, Optional

from .base import CheckResult

POD_LINE_RE = re.compile(
    r"^(?P<name>\S+)\s+(?P<ready>\d+/\d+)\s+(?P<status>\S+)\s+(?P<restarts>\d+)\s+(?P<age>\S+)\s*$"
)

def quote_for_bash(cmd: str) -> str:
    return "'" + cmd.replace("'", "'\"'\"'") + "'"

def load_private_key(key_path: str):
    loaders = [
        paramiko.RSAKey.from_private_key_file,
        paramiko.Ed25519Key.from_private_key_file,
        paramiko.ECDSAKey.from_private_key_file,
    ]
    last_err = None
    for loader in loaders:
        try:
            return loader(key_path)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"No pude cargar la private key: {last_err}")

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

def ssh_run_sudo_block(host, port, user, key_path, bash_block,
                       *, connect_timeout=10, channel_timeout=20, read_timeout=60,
                       tries=2) -> Tuple[str, str, int]:
    last_err: Optional[Exception] = None

    for attempt in range(1, tries + 1):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            pkey = load_private_key(key_path)
            client.connect(
                hostname=host,
                port=port,
                username=user,
                pkey=pkey,
                timeout=connect_timeout,
                banner_timeout=connect_timeout,
                auth_timeout=connect_timeout,
                look_for_keys=False,
                allow_agent=False,
            )

            # sudo -n: no interactivo (si pide password, falla rápido)
            remote_cmd = f"sudo -n bash -lc {quote_for_bash(bash_block)}"
            stdin, stdout, stderr = client.exec_command(remote_cmd, get_pty=False)

            return _read_channel(stdout, stderr, channel_timeout=channel_timeout, read_timeout=read_timeout)

        except Exception as e:
            last_err = e
            # backoff corto
            time.sleep(1.5 * attempt)

        finally:
            try:
                client.close()
            except Exception:
                pass

    raise RuntimeError(f"SSH k8s_dis_nci falló tras {tries} intentos: {last_err}")

def build_remote_block(namespace: str, grep_patterns: List[str]) -> str:
    parts = []
    parts.append("date")
    parts.append(f'echo "NAMESPACE={namespace}"')
    parts.append('echo ""')

    for pat in grep_patterns:
        parts.append(f'echo "### GET_PODS grep={pat}"')
        parts.append(f'kubectl get pods -n {namespace} | grep -i "{pat}" || echo "(none)"')
        parts.append('echo ""')

    return " ; ".join(parts)

def write_raw_file(path: Path, host: str, user: str, namespace: str, stdout: str, stderr: str, exit_code: int, exc: str = ""):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        f.write(f"Host: {host}\nUser: {user}\nTimestampLocal: {dt.datetime.now().isoformat()}\n")
        f.write(f"Namespace: {namespace}\nExitCode: {exit_code}\n")
        if exc:
            f.write(f"Exception: {exc}\n")
        f.write("\n--- stdout ---\n")
        f.write(stdout if stdout else "(vacío)\n")
        f.write("\n--- stderr ---\n")
        f.write(stderr if stderr else "(vacío)\n")
        f.write("\n")

def extract_stdout(text: str) -> str:
    m = re.search(r"--- stdout ---\s*\n(.*?)\n--- stderr ---", text, flags=re.S)
    return m.group(1) if m else text

def ready_ok_xx(ready: str) -> bool:
    try:
        a, b = ready.split("/")
        return int(a) == int(b)
    except Exception:
        return False

def analyze_raw(raw_file: Path) -> Dict:
    text = raw_file.read_text(encoding="utf-8", errors="replace")
    stdout = extract_stdout(text)

    rows = []
    for line in stdout.splitlines():
        line = line.strip()
        m = POD_LINE_RE.match(line)
        if not m:
            continue

        rows.append({
            "pod": m.group("name"),
            "ready": m.group("ready"),
            "status": m.group("status"),
            "restarts": int(m.group("restarts")),
            "age": m.group("age"),
            "short": f"{m.group('ready')} {m.group('status')} {m.group('restarts')} {m.group('age')}",
        })

    not_running = [r for r in rows if r["status"] != "Running"]
    not_ready = [r for r in rows if not ready_ok_xx(r["ready"])]

    return {
        "rows": rows,
        "pods_total": len(rows),
        "pods_not_running": len(not_running),
        "pods_not_ready": len(not_ready),
        "not_running": not_running,
        "not_ready": not_ready,
    }

def compute_status(a: Dict) -> str:
    if a["pods_total"] == 0:
        return "FAIL"
    if a["pods_not_running"] > 0 or a["pods_not_ready"] > 0:
        return "FAIL"
    return "OK"

def run(ssh_cfg: dict, k8s_cfg: dict, output_dirs: dict) -> CheckResult:
    raw_file = Path(output_dirs["raw"]) / "k8s_dis_nci_latest.txt"

    block = build_remote_block(
        namespace=k8s_cfg["namespace"],
        grep_patterns=k8s_cfg["grep_patterns"],
    )

    stdout = ""
    stderr = ""
    exit_code = 255

    try:
        stdout, stderr, exit_code = ssh_run_sudo_block(
            host=ssh_cfg["host"],
            port=ssh_cfg["port"],
            user=ssh_cfg["user"],
            key_path=ssh_cfg["key_path"],
            bash_block=block,
            connect_timeout=10,
            channel_timeout=20,
            read_timeout=60,
            tries=2,
        )

        write_raw_file(raw_file, ssh_cfg["host"], ssh_cfg["user"], k8s_cfg["namespace"], stdout, stderr, exit_code)

        a = analyze_raw(raw_file)
        status = compute_status(a)

        metrics = {
            "pods_total": float(a["pods_total"]),
            "pods_not_running": float(a["pods_not_running"]),
            "pods_not_ready": float(a["pods_not_ready"]),
        }

        details = {
            "namespace": k8s_cfg["namespace"],
            "pods": [{"pod": r["pod"], "short": r["short"]} for r in a["rows"]],
            "raw_exit_code": exit_code,
        }

        return CheckResult(
            name="k8s_dis_nci",
            status=status,
            metrics=metrics,
            details=details,
            raw_file=str(raw_file),
        )

    except Exception as e:
        # Siempre dejamos raw lo mejor posible
        try:
            write_raw_file(raw_file, ssh_cfg["host"], ssh_cfg["user"], k8s_cfg["namespace"], stdout, stderr, exit_code, exc=str(e))
        except Exception:
            pass

        return CheckResult(
            name="k8s_dis_nci",
            status="FAIL",
            metrics={},
            details={"error": str(e), "raw_exit_code": exit_code},
            raw_file=str(raw_file),
        )

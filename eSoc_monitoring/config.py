"""
config.py

Configuración central del proyecto.

- SERVERS: servidores por SSH (key o password)
- CHECKS : checks a ejecutar

Recomendación:
- Evitá hardcodear passwords. Usá variables de entorno.
  PowerShell:
    $env:NELMON_PASS="xxxx"
"""

import os

NELMON_PASS = os.environ.get("NELMON_PASS", "")

SERVERS = {
    "ci21_main": {
        "label": "CI21 Main (10.92.180.105)",
        "host": "10.92.180.105",
        "port": 22,
        "user": "cloud-user",
        "key_path": r"C:/Users/mgallegi/OneDrive - Nokia/Nokia/05 - Softwares/02 - Automatizacion/eSoc_monitoring/keys/cloud-user_login_isa",
    },

    "boundary_main": {
        "label": "Boundary Main (10.92.180.98)",
        "host": "10.92.180.98",
        "port": 22,
        "user": "cloud-user",
        "key_path": r"C:/Users/mgallegi/OneDrive - Nokia/Nokia/05 - Softwares/02 - Automatizacion/eSoc_monitoring/keys/cloud-user_login_isa",
    },

    "nelmon_1": {
        "label": "NLMSAN01 (10.105.93.164)",
        "host": "10.105.93.164",
        "port": 22,
        "user": "root",
        "password": NELMON_PASS or "K4rpat",
    },
    "nelmon_2": {
        "label": "NLMCOR01 (10.105.124.50)",
        "host": "10.105.124.50",
        "port": 22,
        "user": "root",
        "password": NELMON_PASS or "K4rpat",
    },
}

CHECKS = {
    "k8s_dis_nci": {
        "type": "k8s_dis_nci",
        "server": "ci21_main",
        "namespace": "dis-nci",
        "grep_patterns": ["ice-mapreduce", "webservice-rest", "iceca"],
    },

    "nelmon_check_1": {"type": "nelmon_check", "server": "nelmon_1"},
    "nelmon_check_2": {"type": "nelmon_check", "server": "nelmon_2"},

    "boundary": {"type": "boundary", "server": "boundary_main"},
}

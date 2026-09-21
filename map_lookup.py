#!/usr/bin/env python3
"""Lookup TestNG test class#method from PLM CASE_ID (case-insensitive).
Converts pytest-style :: separator to TestNG # separator."""
import json, sys, os
cid = sys.argv[1]
script_dir = os.path.dirname(os.path.abspath(__file__))
map_path = os.path.join(script_dir, ".plm-case-id-map.json")
try:
    with open(map_path) as f:
        d = json.load(f)
    nmap = d.get("nmap", d)
    v = nmap.get(cid)
    if not v:
        uc = cid.upper()
        for k, kv in nmap.items():
            if k.upper() == uc:
                v = kv
                break
    if not v:
        al = d.get("aliases") or {}
        v = al.get(cid)
        if not v:
            uc = cid.upper()
            for k, kv in al.items():
                if k.upper() == uc:
                    v = kv
                    break
    result = v[0] if isinstance(v, list) else (v if isinstance(v, str) else cid)
except Exception:
    result = cid
# Convert pytest :: separator to TestNG # for mvn surefire
print(result.replace("::", "#"))

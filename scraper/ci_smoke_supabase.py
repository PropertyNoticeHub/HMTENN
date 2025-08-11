# ci_smoke_supabase.py — fast Supabase connectivity & config check (no secrets leaked)

import os
import sys
import json
import socket
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

def main() -> int:
    # Load local dev env if present; in CI, GitHub envs will already be set.
    load_dotenv(dotenv_path=".env.local")

    supabase_url = (os.getenv("NEXT_PUBLIC_SUPABASE_URL") or "").strip().rstrip("/")
    anon_key     = (os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY") or "").strip()
    service_key  = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    api_key      = service_key or anon_key

    # 1) Basic presence checks (no secrets printed)
    mode = "SERVICE" if service_key else ("ANON" if anon_key else "MISSING")
    parsed = urlparse(supabase_url) if supabase_url else None
    host = parsed.netloc if parsed else ""
    scheme_ok = (parsed is not None and parsed.scheme in ("https", "http"))

    print(f"[SMOKE] Auth mode (expected SERVICE in CI): {mode}")
    print(f"[SMOKE] Supabase URL present: {bool(supabase_url)} | scheme_ok: {scheme_ok} | host: {host or '(none)'}")
    print(f"[SMOKE] API key present: {bool(api_key)}")

    if not supabase_url or not scheme_ok:
        print("[FAIL] Supabase base URL is missing or invalid. The uploader would build '/rest/...' and crash.")
        return 2
    if not api_key:
        print("[FAIL] No API key found (neither SERVICE nor ANON).")
        return 3

    # 2) Quick DNS reachability (best effort; non-fatal)
    try:
        socket.gethostbyname(host)
        print(f"[SMOKE] DNS ok for host: {host}")
    except Exception as e:
        print(f"[WARN] DNS lookup failed for {host}: {e}")

    # 3) Quick REST call: detect pin_rank support
    #    If 200/206 => column exists. If 400 => likely missing column. If 401/403 => auth issue.
    url = f"{supabase_url}/rest/v1/businesses?select=pin_rank&limit=1"
    headers = {"apikey": api_key, "Authorization": f"Bearer {api_key}"}
    try:
        r = requests.get(url, headers=headers, timeout=15)
        print(f"[SMOKE] GET {urlparse(url).path} -> HTTP {r.status_code}")
        if r.status_code in (200, 206):
            print("[SMOKE] pin_rank supported: True")
            print("[RESULT] SMOKE_OK")
            return 0
        elif r.status_code == 400:
            print("[SMOKE] pin_rank supported: False (400 from Supabase; column likely missing).")
            print("[RESULT] SMOKE_OK")
            return 0
        elif r.status_code in (401, 403):
            print(f"[FAIL] Auth rejected (HTTP {r.status_code}). Check keys/roles.")
            return 4
        else:
            print(f"[FAIL] Unexpected response (HTTP {r.status_code}): {r.text[:300]}")
            return 5
    except requests.exceptions.RequestException as e:
        print(f"[FAIL] Network/request error: {e}")
        return 6

if __name__ == "__main__":
    sys.exit(main())

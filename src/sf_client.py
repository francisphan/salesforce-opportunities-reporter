"""Salesforce client with OAuth auth, refresh token support, and retry logic."""

import json
import os
import time
import http.server
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceExpiredSession

load_dotenv()

TOKEN_CACHE = Path(__file__).parent.parent / ".token_cache.json"

MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds, doubled each retry


def _auth_oauth() -> Salesforce:
    """Authenticate via OAuth 2.0 browser flow (local/interactive use)."""
    client_id = os.environ["SF_CLIENT_ID"]
    client_secret = os.environ["SF_CLIENT_SECRET"]
    redirect_uri = os.environ.get("SF_REDIRECT_URI", "http://localhost:8400/callback")
    domain = os.environ.get("SF_DOMAIN") or "login"

    base_url = f"https://{domain}.salesforce.com"
    auth_url = (
        f"{base_url}/services/oauth2/authorize"
        f"?response_type=code&client_id={client_id}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri, safe='')}"
        f"&scope=full%20refresh_token"
    )

    print(f"\nOpen this URL in your browser to log in:\n\n  {auth_url}\n")

    auth_code = _wait_for_callback(redirect_uri)

    import requests

    token_resp = requests.post(
        f"{base_url}/services/oauth2/token",
        data={
            "grant_type": "authorization_code",
            "code": auth_code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
        },
    )
    token_resp.raise_for_status()
    data = token_resp.json()

    _save_token(data["instance_url"], data["access_token"], data.get("refresh_token"))
    return Salesforce(instance_url=data["instance_url"], session_id=data["access_token"])


def _save_token(instance_url: str, access_token: str, refresh_token: str | None = None):
    cache = {"instance_url": instance_url, "access_token": access_token}
    if refresh_token:
        cache["refresh_token"] = refresh_token
    elif TOKEN_CACHE.exists():
        # Preserve existing refresh token (refresh grant doesn't always return a new one)
        try:
            old = json.loads(TOKEN_CACHE.read_text())
            if old.get("refresh_token"):
                cache["refresh_token"] = old["refresh_token"]
        except Exception:
            pass
    TOKEN_CACHE.write_text(json.dumps(cache))


def _load_cached_token() -> Salesforce | None:
    """Try to connect using a cached token. Returns None if expired/missing."""
    if not TOKEN_CACHE.exists():
        return None
    try:
        data = json.loads(TOKEN_CACHE.read_text())
        sf = Salesforce(instance_url=data["instance_url"], session_id=data["access_token"])
        sf.describe()  # test if token is still valid
        return sf
    except Exception:
        return None


def _wait_for_callback(redirect_uri: str) -> str:
    """Start a tiny HTTP server to capture the OAuth callback code."""
    parsed = urllib.parse.urlparse(redirect_uri)
    port = parsed.port or 8400
    captured = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            code = qs.get("code", [None])[0]
            if code:
                captured["code"] = code
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(b"<h3>Auth complete. You can close this tab.</h3>")
            else:
                self.send_response(204)
                self.end_headers()

        def log_message(self, *args):
            pass

    with http.server.HTTPServer(("0.0.0.0", port), Handler) as server:
        server.timeout = 120
        print(f"Listening on port {port} for OAuth callback...")
        while not captured.get("code"):
            server.handle_request()

    return captured["code"]


def _refresh_oauth_token() -> Salesforce | None:
    """Try to silently refresh the access token using a refresh token.

    Checks both the token cache file and environment variables (for CI).
    """
    # Try env vars first (CI path: refresh token stored as GitHub Secret)
    refresh_token = os.environ.get("SF_REFRESH_TOKEN")
    instance_url = os.environ.get("SF_INSTANCE_URL")

    # Fall back to token cache file (local dev path)
    if not refresh_token:
        if not TOKEN_CACHE.exists():
            return None
        try:
            cache = json.loads(TOKEN_CACHE.read_text())
            refresh_token = cache.get("refresh_token")
            instance_url = cache.get("instance_url")
            if not refresh_token:
                return None
        except Exception:
            return None

    client_id = os.environ["SF_CLIENT_ID"]
    client_secret = os.environ["SF_CLIENT_SECRET"]
    domain = os.environ.get("SF_DOMAIN") or "login"
    base_url = f"https://{domain}.salesforce.com"

    import requests

    resp = requests.post(
        f"{base_url}/services/oauth2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )
    if not resp.ok:
        return None

    data = resp.json()
    _save_token(data["instance_url"], data["access_token"], data.get("refresh_token"))
    return Salesforce(instance_url=data["instance_url"], session_id=data["access_token"])


def _reconnect() -> Salesforce:
    """Re-authenticate: try refresh token first, fall back to full OAuth."""
    refreshed = _refresh_oauth_token()
    if refreshed:
        return refreshed
    return _auth_oauth()


def connect() -> Salesforce:
    """Connect to Salesforce using OAuth.

    Tries (in order): cached token, refresh token, full browser OAuth flow.
    """
    # Try cached access token (local dev)
    cached = _load_cached_token()
    if cached:
        return cached

    # Try refresh token (works in both local and CI)
    refreshed = _refresh_oauth_token()
    if refreshed:
        return refreshed

    # Full OAuth browser flow (local only — will fail in CI if refresh token is invalid)
    return _auth_oauth()


def _with_retry(sf_holder: list, func):
    """Execute func(sf) with retry on transient errors and token refresh on expired session.

    sf_holder is a single-element list so we can swap the connection on re-auth.
    """
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            return func(sf_holder[0])
        except SalesforceExpiredSession:
            sf_holder[0] = _reconnect()
            continue
        except Exception as e:
            last_exc = e
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF * (2 ** attempt)
                time.sleep(wait)
            continue
    raise last_exc


def query(sf_holder: list, soql: str) -> list[dict]:
    """Run a SOQL query with retry. sf_holder is [sf_connection]."""
    def _do(sf):
        result = sf.query_all(soql)
        records = result.get("records", [])
        for r in records:
            r.pop("attributes", None)
        return records
    return _with_retry(sf_holder, _do)

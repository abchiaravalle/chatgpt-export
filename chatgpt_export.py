#!/usr/bin/env python3
"""
ChatGPT Export Tool
====================
One-script tool to export all your ChatGPT conversations and generate
an offline viewer you can open by double-clicking.

Usage:
    python3 chatgpt_export.py                    # Full flow (opens browser for login)
    python3 chatgpt_export.py --token TOKEN      # Skip browser login
    python3 chatgpt_export.py --viewer-only      # Just regenerate the viewer
    python3 chatgpt_export.py --no-media         # Skip media downloads
    python3 chatgpt_export.py --output DIR       # Custom output directory
"""

import argparse
import importlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse


# ── Dependency Auto-Installer ────────────────────────────────────────────────

VENV_DIR = Path(__file__).parent / ".venv"


def _in_venv():
    """Check if we're running inside a virtual environment."""
    return sys.prefix != sys.base_prefix


def _relaunch_in_venv():
    """Create a venv and re-launch this script inside it."""
    if not VENV_DIR.exists():
        print(f"Creating virtual environment at {VENV_DIR}...")
        subprocess.check_call([sys.executable, "-m", "venv", str(VENV_DIR)])

    # Determine the venv python
    if sys.platform == "win32":
        venv_python = VENV_DIR / "Scripts" / "python.exe"
    else:
        venv_python = VENV_DIR / "bin" / "python3"
        if not venv_python.exists():
            venv_python = VENV_DIR / "bin" / "python"

    # Re-exec with the same arguments
    os.execv(str(venv_python), [str(venv_python)] + sys.argv)


def ensure_dependencies():
    """Install playwright and requests if missing, plus Chromium browser."""
    # If we're not in a venv and pip can't install globally, bootstrap one
    if not _in_venv():
        needs_venv = False
        try:
            # Test if pip can install (some systems block global installs)
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", "--dry-run", "--quiet", "requests"],
                capture_output=True, timeout=15,
            )
            if result.returncode != 0 and b"externally-managed" in result.stderr:
                needs_venv = True
        except Exception:
            needs_venv = True

        # Also need a venv if deps are missing and system is managed
        missing = []
        for pkg in ("playwright", "requests"):
            try:
                importlib.import_module(pkg)
            except ImportError:
                missing.append(pkg)

        if missing and needs_venv:
            _relaunch_in_venv()  # Does not return

    # Now install any missing packages (we're either in a venv or pip works globally)
    missing = []
    for pkg in ("playwright", "requests"):
        try:
            importlib.import_module(pkg)
        except ImportError:
            missing.append(pkg)

    if missing:
        print(f"Installing missing dependencies: {', '.join(missing)}")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet"] + missing
        )

    # Ensure Chromium is installed for Playwright
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(headless=True)
                browser.close()
            except Exception:
                print("Installing Chromium browser for Playwright...")
                subprocess.check_call(
                    [sys.executable, "-m", "playwright", "install", "chromium"]
                )
    except Exception:
        print("Installing Chromium browser for Playwright...")
        subprocess.check_call(
            [sys.executable, "-m", "playwright", "install", "chromium"]
        )


ensure_dependencies()

import requests
from playwright.sync_api import sync_playwright


# ── CLI Arguments ────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Export all ChatGPT conversations and build an offline viewer."
    )
    parser.add_argument(
        "--token", type=str, default=None,
        help="Bearer token (skips browser login)"
    )
    parser.add_argument(
        "--viewer-only", action="store_true",
        help="Skip downloading; just regenerate the viewer from existing data"
    )
    parser.add_argument(
        "--no-media", action="store_true",
        help="Skip downloading media files (images, attachments)"
    )
    parser.add_argument(
        "--output", type=str, default="export",
        help="Output directory (default: export)"
    )
    return parser.parse_args()


# ── Constants ────────────────────────────────────────────────────────────────

BASE_URL = "https://chatgpt.com/backend-api"
PAGE_SIZE = 28
REQUEST_TIMEOUT = 30

# Rate limiting
DELAY_BETWEEN_LIST_PAGES = 2.0
DELAY_BETWEEN_CONVERSATIONS = 1.5
DELAY_BETWEEN_MEDIA = 1.0

# Retry config
MAX_RETRIES = 5
RETRY_BACKOFF_BASE = 2


# ── Credential Management ────────────────────────────────────────────────────

def credentials_path(export_dir):
    return Path(export_dir) / ".credentials.json"


def load_cached_credentials(export_dir):
    """Load cached credentials if they exist."""
    path = credentials_path(export_dir)
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return None


def cache_credentials(export_dir, creds):
    """Save credentials to cache file."""
    Path(export_dir).mkdir(parents=True, exist_ok=True)
    path = credentials_path(export_dir)
    with open(path, "w") as f:
        json.dump(creds, f, indent=2)
    # Restrict permissions
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def validate_credentials(creds):
    """Test if credentials are still valid by hitting /backend-api/me."""
    try:
        s = requests.Session()
        s.headers.update({
            "authorization": f"Bearer {creds['token']}",
            "chatgpt-account-id": creds.get("account_id", ""),
            "oai-device-id": creds.get("device_id", ""),
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
        })
        resp = s.get(f"{BASE_URL}/me", timeout=10)
        return resp.status_code == 200
    except Exception:
        return False


def capture_credentials():
    """Open a browser for the user to log in, capture auth headers from network requests."""
    creds = {"token": None, "account_id": None, "device_id": None}

    def on_request(request):
        url = request.url
        if "chatgpt.com/backend-api" not in url:
            return
        headers = request.headers
        if "authorization" in headers and headers["authorization"].startswith("Bearer "):
            token = headers["authorization"].replace("Bearer ", "")
            if token and len(token) > 20:
                creds["token"] = token
        if "chatgpt-account-id" in headers:
            creds["account_id"] = headers["chatgpt-account-id"]
        if "oai-device-id" in headers:
            creds["device_id"] = headers["oai-device-id"]

    print("\nOpening browser for ChatGPT login...")
    print("Log in normally, then wait for the script to continue.")
    print("(You have 5 minutes to complete login)\n")

    # Use a persistent profile so Google doesn't flag it as an automated browser
    browser_profile = Path(__file__).parent / ".browser-profile"
    browser_profile.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(browser_profile),
            headless=False,
            viewport={"width": 1280, "height": 800},
            args=[
                "--disable-blink-features=AutomationControlled",
            ],
            ignore_default_args=["--enable-automation"],
        )

        # Attach request listener to ALL pages (current and future)
        # so we catch tokens even after OAuth redirects or new tabs
        def attach_listener(page):
            page.on("request", on_request)

        for pg in context.pages:
            attach_listener(pg)
        context.on("page", attach_listener)

        page = context.pages[0] if context.pages else context.new_page()
        page.goto("https://chatgpt.com/")

        # Wait for auth headers to appear (max 5 minutes)
        deadline = time.time() + 300
        while time.time() < deadline:
            if creds["token"]:
                # Give a moment for account_id and device_id to also arrive
                time.sleep(3)
                break
            time.sleep(1)

        context.close()

    if not creds["token"]:
        print("ERROR: Could not capture authentication token.")
        print("The login may have timed out or the browser was closed too early.")
        print("Try again, or use --token to provide a token manually.")
        sys.exit(1)

    print("Authentication captured successfully.")
    return creds


def get_credentials(args, export_dir):
    """Get credentials from CLI flag, cache, or browser login."""
    # 1. CLI --token flag
    if args.token:
        creds = {"token": args.token, "account_id": "", "device_id": ""}
        # Try to validate and get account info
        if validate_credentials(creds):
            print("Token from --token flag is valid.")
            cache_credentials(export_dir, creds)
            return creds
        else:
            print("WARNING: Provided token may be invalid. Continuing anyway...")
            return creds

    # 2. Cached credentials
    cached = load_cached_credentials(export_dir)
    if cached and cached.get("token"):
        print("Found cached credentials. Validating...")
        if validate_credentials(cached):
            print("Cached credentials are valid.")
            return cached
        else:
            print("Cached credentials expired.")

    # 3. Browser login
    creds = capture_credentials()
    cache_credentials(export_dir, creds)
    return creds


# ── HTTP Session ─────────────────────────────────────────────────────────────

def create_session(creds):
    """Create a requests session with auth headers."""
    s = requests.Session()
    headers = {
        "accept": "*/*",
        "authorization": f"Bearer {creds['token']}",
        "oai-language": "en-US",
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        ),
    }
    if creds.get("account_id"):
        headers["chatgpt-account-id"] = creds["account_id"]
    if creds.get("device_id"):
        headers["oai-device-id"] = creds["device_id"]
    s.headers.update(headers)
    return s


def request_with_retry(session, method, url, **kwargs):
    """Make an HTTP request with exponential backoff retry."""
    kwargs.setdefault("timeout", REQUEST_TIMEOUT)
    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.request(method, url, **kwargs)

            if resp.status_code == 200:
                return resp

            if resp.status_code == 401:
                print("\n  ERROR: 401 Unauthorized — token expired.")
                print("  Re-run the script to log in again.")
                print("  The script will resume where it left off.")
                sys.exit(1)

            if resp.status_code == 429:
                wait = RETRY_BACKOFF_BASE ** (attempt + 2)
                print(f"    Rate limited (429). Waiting {wait}s... (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = RETRY_BACKOFF_BASE ** attempt
                print(f"    Server error ({resp.status_code}). Retrying in {wait}s... (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait)
                continue

            resp.raise_for_status()

        except requests.exceptions.Timeout:
            wait = RETRY_BACKOFF_BASE ** attempt
            print(f"    Timeout. Retrying in {wait}s... (attempt {attempt+1}/{MAX_RETRIES})")
            time.sleep(wait)
            last_error = "timeout"
            continue

        except requests.exceptions.ConnectionError as e:
            wait = RETRY_BACKOFF_BASE ** attempt
            print(f"    Connection error. Retrying in {wait}s... (attempt {attempt+1}/{MAX_RETRIES})")
            time.sleep(wait)
            last_error = str(e)
            continue

    raise Exception(f"Failed after {MAX_RETRIES} retries. Last error: {last_error or resp.status_code}")


# ── List Conversations ───────────────────────────────────────────────────────

def list_all_conversations(session, include_archived=False):
    """Paginate through all conversations."""
    all_convos = []
    offset = 0

    while True:
        params = {
            "offset": offset,
            "limit": PAGE_SIZE,
            "order": "updated",
            "is_archived": str(include_archived).lower(),
        }
        print(f"    Fetching list (offset={offset})...")
        resp = request_with_retry(session, "GET", f"{BASE_URL}/conversations", params=params)
        data = resp.json()
        items = data.get("items", [])
        total = data.get("total", 0)

        if not items:
            break

        all_convos.extend(items)
        print(f"    {len(all_convos)}/{total} conversations indexed")

        offset += PAGE_SIZE
        if offset >= total:
            break

        time.sleep(DELAY_BETWEEN_LIST_PAGES)

    return all_convos


# ── Fetch Conversation Detail ────────────────────────────────────────────────

def fetch_conversation(session, convo_id):
    """Fetch full conversation content."""
    resp = request_with_retry(session, "GET", f"{BASE_URL}/conversation/{convo_id}")
    return resp.json()


# ── Media Extraction & Download ──────────────────────────────────────────────

def extract_media_refs(conversation_data):
    """Extract all media references from a conversation's message tree."""
    media_refs = []
    mapping = conversation_data.get("mapping", {})

    for node_id, node in mapping.items():
        msg = node.get("message")
        if not msg:
            continue

        msg_id = msg.get("id", node_id)
        content = msg.get("content", {})
        parts = content.get("parts", [])
        metadata = msg.get("metadata", {})

        for i, part in enumerate(parts):
            if isinstance(part, dict):
                if part.get("content_type") == "image_asset_pointer":
                    asset_pointer = part.get("asset_pointer", "")
                    file_id = asset_pointer.replace("file-service://", "")
                    media_refs.append({
                        "type": "image",
                        "file_id": file_id,
                        "message_id": msg_id,
                        "part_index": i,
                        "width": part.get("width"),
                        "height": part.get("height"),
                        "size_bytes": part.get("size_bytes"),
                        "metadata": {k: v for k, v in part.items()
                                     if k not in ("content_type", "asset_pointer")},
                    })

                elif part.get("content_type") == "multimodal_text" and "image" in str(part):
                    for sub in part.get("parts", []):
                        if isinstance(sub, dict) and sub.get("asset_pointer", "").startswith("file-service://"):
                            file_id = sub["asset_pointer"].replace("file-service://", "")
                            media_refs.append({
                                "type": "image",
                                "file_id": file_id,
                                "message_id": msg_id,
                                "part_index": i,
                                "width": sub.get("width"),
                                "height": sub.get("height"),
                                "size_bytes": sub.get("size_bytes"),
                            })

        for att in metadata.get("attachments", []):
            att_id = att.get("id", "")
            att_name = att.get("name", "unknown")
            if att_id:
                media_refs.append({
                    "type": "attachment",
                    "file_id": att_id,
                    "message_id": msg_id,
                    "filename": att_name,
                    "mime_type": att.get("mimeType", ""),
                    "size_bytes": att.get("size", 0),
                })

        dalle_meta = metadata.get("dalle", {})
        if dalle_meta:
            for gen in dalle_meta if isinstance(dalle_meta, list) else [dalle_meta]:
                if isinstance(gen, dict) and gen.get("asset_pointer", "").startswith("file-service://"):
                    file_id = gen["asset_pointer"].replace("file-service://", "")
                    media_refs.append({
                        "type": "dalle_image",
                        "file_id": file_id,
                        "message_id": msg_id,
                        "prompt": gen.get("prompt", ""),
                    })

        for ref in metadata.get("content_references", []):
            if isinstance(ref, dict) and ref.get("asset_pointer", "").startswith("file-service://"):
                file_id = ref["asset_pointer"].replace("file-service://", "")
                media_refs.append({
                    "type": "content_reference",
                    "file_id": file_id,
                    "message_id": msg_id,
                })

    return media_refs


def download_media(session, file_id, save_dir):
    """Download a media file. Returns the saved filename or None on failure."""
    save_dir.mkdir(parents=True, exist_ok=True)

    try:
        resp = request_with_retry(session, "GET", f"{BASE_URL}/files/{file_id}/download")
        data = resp.json()
        download_url = data.get("download_url", "")

        if not download_url:
            print(f"      No download URL for {file_id}")
            return None

        file_resp = request_with_retry(session, "GET", download_url)

        content_type = file_resp.headers.get("content-type", "")
        ext = guess_extension(content_type, download_url, file_id)

        filename = f"{file_id}{ext}"
        filepath = save_dir / filename

        with open(filepath, "wb") as f:
            f.write(file_resp.content)

        size_kb = len(file_resp.content) / 1024
        print(f"      Downloaded: {filename} ({size_kb:.1f} KB)")
        return filename

    except Exception as e:
        print(f"      Failed to download {file_id}: {e}")
        return None


def guess_extension(content_type, url, file_id):
    """Guess file extension from content-type or URL."""
    ct_map = {
        "image/png": ".png",
        "image/jpeg": ".jpg",
        "image/gif": ".gif",
        "image/webp": ".webp",
        "image/svg+xml": ".svg",
        "application/pdf": ".pdf",
        "text/plain": ".txt",
        "text/csv": ".csv",
        "application/json": ".json",
        "audio/mpeg": ".mp3",
        "audio/wav": ".wav",
        "video/mp4": ".mp4",
    }
    for ct, ext in ct_map.items():
        if ct in content_type:
            return ext

    path = urlparse(url).path
    if "." in path.split("/")[-1]:
        return "." + path.split(".")[-1].split("?")[0]

    return ".bin"


# ── Message Tree Walking ─────────────────────────────────────────────────────

def walk_message_tree(mapping, current_node):
    """Walk from root to current_node, producing messages in conversation order."""
    children_map = {}
    root_id = None
    for node_id, node in mapping.items():
        parent = node.get("parent")
        if parent is None:
            root_id = node_id
        else:
            children_map.setdefault(parent, []).append(node_id)

    if not root_id:
        return list(mapping.keys())

    active_path = set()
    if current_node:
        node_id = current_node
        while node_id:
            active_path.add(node_id)
            node_id = mapping.get(node_id, {}).get("parent")

    ordered = []
    queue = [root_id]
    while queue:
        nid = queue.pop(0)
        ordered.append(nid)
        kids = children_map.get(nid, [])
        if active_path:
            kids_sorted = sorted(kids, key=lambda k: k not in active_path)
            for kid in kids_sorted:
                if kid in active_path:
                    queue.append(kid)
                    break
            else:
                queue.extend(kids)
        else:
            queue.extend(kids)

    return ordered


def build_conversation_record(convo_summary, convo_detail, media_manifest):
    """Build a flattened, searchable record for one conversation."""
    convo_id = convo_detail.get("conversation_id", convo_summary.get("id", ""))
    mapping = convo_detail.get("mapping", {})

    messages = []
    ordered_nodes = walk_message_tree(mapping, convo_detail.get("current_node"))

    for node_id in ordered_nodes:
        node = mapping.get(node_id, {})
        msg = node.get("message")
        if not msg:
            continue

        content = msg.get("content", {})
        content_type = content.get("content_type", "")
        parts = content.get("parts", [])
        role = msg.get("author", {}).get("role", "unknown")
        metadata = msg.get("metadata", {})

        if role == "system" and content_type == "text" and not any(parts):
            continue

        text_parts = []
        for part in parts:
            if isinstance(part, str) and part.strip():
                text_parts.append(part)
            elif isinstance(part, dict):
                if part.get("content_type") == "image_asset_pointer":
                    text_parts.append(f"[Image: {part.get('asset_pointer', 'unknown')}]")

        text = "\n".join(text_parts)
        if not text and content_type in ("text", ""):
            continue

        msg_media = [m for m in media_manifest if m.get("message_id") == msg.get("id", node_id)]

        record = {
            "id": msg.get("id", node_id),
            "role": role,
            "content": text,
            "content_type": content_type,
            "timestamp": msg.get("create_time"),
            "model": metadata.get("model_slug"),
        }

        if msg_media:
            record["media"] = [{
                "type": m["type"],
                "file_id": m["file_id"],
                "filename": m.get("saved_as"),
                "original_name": m.get("filename"),
                "mime_type": m.get("mime_type"),
            } for m in msg_media]

        messages.append(record)

    return {
        "id": convo_id,
        "title": convo_detail.get("title", "Untitled"),
        "create_time": convo_detail.get("create_time"),
        "update_time": convo_detail.get("update_time"),
        "model": convo_detail.get("default_model_slug"),
        "is_archived": convo_detail.get("is_archived", False),
        "gizmo_id": convo_detail.get("gizmo_id"),
        "message_count": len(messages),
        "messages": messages,
    }


# ── Progress Tracking ────────────────────────────────────────────────────────

def load_progress(export_dir):
    """Load set of already-downloaded conversation IDs."""
    progress_file = Path(export_dir) / ".progress.json"
    if progress_file.exists():
        with open(progress_file) as f:
            return set(json.load(f))
    return set()


def save_progress(export_dir, done_ids):
    """Save set of completed conversation IDs."""
    progress_file = Path(export_dir) / ".progress.json"
    with open(progress_file, "w") as f:
        json.dump(sorted(done_ids), f)


# ── Export Orchestrator ──────────────────────────────────────────────────────

def run_export(session, export_dir, no_media=False):
    """Download all conversations and media. Returns list of conversation records."""
    export_path = Path(export_dir)
    conversations_dir = export_path / "conversations"
    export_path.mkdir(exist_ok=True)
    conversations_dir.mkdir(exist_ok=True)

    done_ids = load_progress(export_dir)

    print("\n" + "=" * 60)
    print("  ChatGPT Full Export")
    print("=" * 60)

    # Phase 1: Index all conversations
    print("\n[Phase 1] Indexing conversations...")

    print("  Active conversations:")
    conversations = list_all_conversations(session, include_archived=False)
    print(f"  Found {len(conversations)} active conversations.")

    time.sleep(DELAY_BETWEEN_LIST_PAGES)

    print("  Archived conversations:")
    archived = list_all_conversations(session, include_archived=True)
    seen = {c["id"] for c in conversations}
    added = 0
    for c in archived:
        if c["id"] not in seen:
            conversations.append(c)
            seen.add(c["id"])
            added += 1
    print(f"  Found {added} additional archived conversations.")
    print(f"  Total: {len(conversations)}")

    if done_ids:
        remaining = [c for c in conversations if c["id"] not in done_ids]
        print(f"  Already downloaded: {len(done_ids)}, Remaining: {len(remaining)}")

    # Phase 2: Download each conversation + media
    print(f"\n[Phase 2] Downloading conversations and media...")
    search_index = []
    downloaded = 0
    failed_ids = []

    for i, convo in enumerate(conversations):
        convo_id = convo["id"]
        title = convo.get("title", "Untitled")
        convo_dir = conversations_dir / convo_id
        media_dir = convo_dir / "media"

        if convo_id in done_ids:
            saved_convo = convo_dir / "conversation.json"
            if saved_convo.exists():
                with open(saved_convo) as f:
                    detail = json.load(f)
                manifest_file = convo_dir / "media_manifest.json"
                manifest = []
                if manifest_file.exists():
                    with open(manifest_file) as f:
                        manifest = json.load(f)
                search_index.append(build_conversation_record(convo, detail, manifest))
            print(f"  [{i+1}/{len(conversations)}] SKIP: {title}")
            continue

        print(f"  [{i+1}/{len(conversations)}] {title}")

        try:
            print(f"    Fetching conversation...")
            detail = fetch_conversation(session, convo_id)
            time.sleep(DELAY_BETWEEN_CONVERSATIONS)

            convo_dir.mkdir(parents=True, exist_ok=True)
            with open(convo_dir / "conversation.json", "w", encoding="utf-8") as f:
                json.dump(detail, f, indent=2, ensure_ascii=False)

            media_refs = extract_media_refs(detail)
            media_manifest = []

            if media_refs and not no_media:
                print(f"    Found {len(media_refs)} media files")
                for j, ref in enumerate(media_refs):
                    file_id = ref["file_id"]
                    print(f"    [{j+1}/{len(media_refs)}] Downloading {ref['type']}: {file_id}")
                    saved_as = download_media(session, file_id, media_dir)
                    ref["saved_as"] = saved_as
                    media_manifest.append(ref)
                    time.sleep(DELAY_BETWEEN_MEDIA)

                with open(convo_dir / "media_manifest.json", "w") as f:
                    json.dump(media_manifest, f, indent=2)
            elif media_refs:
                print(f"    Found {len(media_refs)} media files (skipped: --no-media)")
                media_manifest = media_refs

            search_index.append(build_conversation_record(convo, detail, media_manifest))

            done_ids.add(convo_id)
            save_progress(export_dir, done_ids)
            downloaded += 1
            print(f"    Done.")

        except Exception as e:
            print(f"    FAILED: {e}")
            failed_ids.append({"id": convo_id, "title": title, "error": str(e)})

    # Save raw conversation list
    with open(export_path / "conversations_list.json", "w", encoding="utf-8") as f:
        json.dump(conversations, f, indent=2, ensure_ascii=False)

    # Summary
    print("\n" + "=" * 60)
    print(f"  Export complete!")
    print(f"  Downloaded: {downloaded} new conversations")
    print(f"  Previously saved: {len(done_ids) - downloaded}")
    print(f"  Failed: {len(failed_ids)}")
    print("=" * 60)

    if failed_ids:
        print("\nFailed conversations:")
        for item in failed_ids:
            print(f"  - {item['title']} ({item['id'][:8]}): {item['error']}")
        with open(export_path / "failures.json", "w") as fh:
            json.dump(failed_ids, fh, indent=2)
        print(f"\nRe-run the script to retry failed conversations.")

    return search_index


def build_index_from_disk(export_dir):
    """Build conversation records from already-downloaded data (for --viewer-only)."""
    conversations_dir = Path(export_dir) / "conversations"
    if not conversations_dir.exists():
        print("ERROR: No conversations directory found. Run a full export first.")
        sys.exit(1)

    convo_dirs = sorted(conversations_dir.iterdir())
    print(f"Found {len(convo_dirs)} conversation directories")

    records = []
    for d in convo_dirs:
        if not d.is_dir():
            continue
        convo_file = d / "conversation.json"
        if not convo_file.exists():
            continue
        try:
            with open(convo_file) as f:
                detail = json.load(f)

            manifest = []
            manifest_file = d / "media_manifest.json"
            if manifest_file.exists():
                with open(manifest_file) as f:
                    manifest = json.load(f)

            summary = {"id": d.name, "title": detail.get("title", "Untitled")}
            record = build_conversation_record(summary, detail, manifest)
            records.append(record)
        except Exception as e:
            print(f"  Skipping {d.name}: {e}")

    records.sort(key=lambda r: r.get("update_time") or 0, reverse=True)
    return records


# ── Viewer Generation ────────────────────────────────────────────────────────

CHUNK_TARGET_BYTES = 5 * 1024 * 1024  # ~5MB per chunk


def generate_viewer(export_dir, records):
    """Generate the offline viewer: index.js, chunk files, and viewer.html."""
    export_path = Path(export_dir)
    data_dir = export_path / "data"
    chunks_dir = data_dir / "conversations"
    data_dir.mkdir(parents=True, exist_ok=True)
    chunks_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n[Viewer] Generating offline viewer...")

    # Sort records by update_time descending
    records.sort(key=lambda r: r.get("update_time") or 0, reverse=True)

    # Build index entries (metadata + 200-char preview, no full messages)
    # and prepare chunks (full message arrays)
    index_entries = []
    chunk_data = {}  # chunk_name -> {convo_id: messages_array}
    convo_to_chunk = {}  # convo_id -> chunk_name

    current_chunk_name = "chunk_000"
    current_chunk_size = 0
    chunk_counter = 0

    for rec in records:
        convo_id = rec["id"]
        messages = rec.get("messages", [])

        # Build preview from first user message
        preview = ""
        for msg in messages:
            if msg.get("role") == "user" and msg.get("content"):
                preview = msg["content"][:200]
                break

        index_entries.append({
            "id": convo_id,
            "title": rec.get("title", "Untitled"),
            "create_time": rec.get("create_time"),
            "update_time": rec.get("update_time"),
            "model": rec.get("model"),
            "is_archived": rec.get("is_archived", False),
            "gizmo_id": rec.get("gizmo_id"),
            "message_count": rec.get("message_count", 0),
            "preview": preview,
            "chunk": current_chunk_name,
        })

        # Estimate message size
        msg_json = json.dumps(messages, ensure_ascii=False)
        msg_size = len(msg_json.encode("utf-8"))

        if current_chunk_name not in chunk_data:
            chunk_data[current_chunk_name] = {}

        chunk_data[current_chunk_name][convo_id] = messages
        convo_to_chunk[convo_id] = current_chunk_name
        current_chunk_size += msg_size

        # Start new chunk if current one exceeds target
        if current_chunk_size >= CHUNK_TARGET_BYTES:
            chunk_counter += 1
            current_chunk_name = f"chunk_{chunk_counter:03d}"
            current_chunk_size = 0

    # Write index.js
    index_obj = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "total_conversations": len(records),
        "conversations": index_entries,
    }
    index_js = f"window.CHATGPT_INDEX = {json.dumps(index_obj, ensure_ascii=False)};\n"
    with open(data_dir / "index.js", "w", encoding="utf-8") as f:
        f.write(index_js)
    index_size_mb = len(index_js.encode("utf-8")) / 1024 / 1024
    print(f"  index.js: {index_size_mb:.1f} MB ({len(index_entries)} conversations)")

    # Write chunk files
    for chunk_name, convos in chunk_data.items():
        chunk_js = f'window.CHATGPT_CHUNKS = window.CHATGPT_CHUNKS || {{}};\nwindow.CHATGPT_CHUNKS["{chunk_name}"] = {json.dumps(convos, ensure_ascii=False)};\n'
        with open(chunks_dir / f"{chunk_name}.js", "w", encoding="utf-8") as f:
            f.write(chunk_js)
        chunk_size_mb = len(chunk_js.encode("utf-8")) / 1024 / 1024
        print(f"  {chunk_name}.js: {chunk_size_mb:.1f} MB ({len(convos)} conversations)")

    # Write viewer.html
    html = get_viewer_html()
    with open(export_path / "viewer.html", "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  viewer.html written")

    print(f"\nOpen {export_path / 'viewer.html'} to browse your conversations.")


def get_viewer_html():
    """Return the complete viewer HTML template."""
    return VIEWER_HTML_TEMPLATE


# ── Viewer HTML Template ─────────────────────────────────────────────────────
# This is the full offline viewer that works via file:// protocol.
# It loads data from data/index.js (synchronous <script> tag) and
# lazy-loads message chunks from data/conversations/chunk_NNN.js on demand.

VIEWER_HTML_TEMPLATE = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ChatGPT Archive</title>
<style>
  :root {
    --bg: #0d1117;
    --bg-secondary: #161b22;
    --bg-tertiary: #1c2128;
    --border: #30363d;
    --text: #e6edf3;
    --text-muted: #8b949e;
    --accent: #58a6ff;
    --user-bg: #1a2332;
    --assistant-bg: #161b22;
    --system-bg: #1c1c1c;
    --highlight: #f0c000;
    --green: #3fb950;
    --red: #f85149;
    --purple: #bc8cff;
  }

  * { margin: 0; padding: 0; box-sizing: border-box; }

  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
    background: var(--bg);
    color: var(--text);
    height: 100vh;
    display: flex;
    overflow: hidden;
  }

  /* ── Sidebar ── */
  .sidebar {
    width: 400px;
    min-width: 400px;
    border-right: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    height: 100vh;
    background: var(--bg-secondary);
  }

  .search-box {
    padding: 12px 16px;
    border-bottom: 1px solid var(--border);
  }

  .search-box input {
    width: 100%;
    padding: 10px 14px;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 8px;
    color: var(--text);
    font-size: 14px;
    outline: none;
  }

  .search-box input:focus { border-color: var(--accent); }
  .search-box input::placeholder { color: var(--text-muted); }

  .toolbar {
    padding: 8px 16px;
    border-bottom: 1px solid var(--border);
    display: flex;
    gap: 8px;
    align-items: center;
    flex-wrap: wrap;
  }

  .toolbar select {
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text);
    font-size: 12px;
    padding: 4px 8px;
    outline: none;
    cursor: pointer;
  }

  .toolbar label {
    font-size: 12px;
    color: var(--text-muted);
  }

  .search-stats {
    padding: 6px 16px;
    font-size: 12px;
    color: var(--text-muted);
    border-bottom: 1px solid var(--border);
    display: flex;
    justify-content: space-between;
    align-items: center;
  }

  .filter-bar {
    padding: 8px 16px;
    border-bottom: 1px solid var(--border);
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
  }

  .filter-btn {
    padding: 3px 10px;
    border-radius: 12px;
    border: 1px solid var(--border);
    background: transparent;
    color: var(--text-muted);
    font-size: 11px;
    cursor: pointer;
    transition: all 0.15s;
  }

  .filter-btn:hover { border-color: var(--text-muted); }

  .filter-btn.active {
    background: var(--accent);
    color: var(--bg);
    border-color: var(--accent);
  }

  .convo-list {
    flex: 1;
    overflow-y: auto;
    list-style: none;
  }

  .convo-item {
    padding: 10px 16px;
    border-bottom: 1px solid var(--border);
    cursor: pointer;
    transition: background 0.1s;
  }

  .convo-item:hover { background: var(--bg-tertiary); }

  .convo-item.active {
    background: var(--bg-tertiary);
    border-left: 3px solid var(--accent);
    padding-left: 13px;
  }

  .convo-title {
    font-size: 13px;
    font-weight: 500;
    margin-bottom: 3px;
    line-height: 1.3;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .convo-meta {
    font-size: 11px;
    color: var(--text-muted);
    display: flex;
    gap: 10px;
  }

  .convo-preview {
    font-size: 11px;
    color: var(--text-muted);
    margin-top: 3px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    opacity: 0.7;
  }

  mark {
    background: var(--highlight);
    color: var(--bg);
    border-radius: 2px;
    padding: 0 2px;
  }

  .load-more-sentinel {
    padding: 16px;
    text-align: center;
    color: var(--text-muted);
    font-size: 12px;
  }

  /* ── Main Content ── */
  .main {
    flex: 1;
    display: flex;
    flex-direction: column;
    height: 100vh;
    overflow: hidden;
  }

  .main-header {
    padding: 14px 24px;
    border-bottom: 1px solid var(--border);
    background: var(--bg-secondary);
  }

  .main-header h2 {
    font-size: 17px;
    font-weight: 600;
  }

  .main-header .meta {
    font-size: 12px;
    color: var(--text-muted);
    margin-top: 4px;
    display: flex;
    gap: 16px;
    flex-wrap: wrap;
  }

  .messages {
    flex: 1;
    overflow-y: auto;
    padding: 20px 24px;
  }

  .message {
    margin-bottom: 20px;
    max-width: 900px;
  }

  .message-role {
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 4px;
  }

  .message-role.user { color: var(--accent); }
  .message-role.assistant { color: var(--green); }
  .message-role.system { color: var(--text-muted); }
  .message-role.tool { color: var(--purple); }

  .message-content {
    padding: 12px 16px;
    border-radius: 10px;
    font-size: 14px;
    line-height: 1.7;
    white-space: pre-wrap;
    word-wrap: break-word;
  }

  .message.user .message-content { background: var(--user-bg); }
  .message.assistant .message-content { background: var(--assistant-bg); border: 1px solid var(--border); }
  .message.system .message-content { background: var(--system-bg); border: 1px solid var(--border); font-size: 12px; color: var(--text-muted); }
  .message.tool .message-content { background: var(--system-bg); border: 1px solid var(--border); font-size: 13px; font-family: 'SF Mono', Monaco, monospace; }

  .message-content img {
    max-width: 100%;
    border-radius: 8px;
    margin-top: 8px;
    cursor: pointer;
  }

  .message-content img:hover { opacity: 0.9; }

  .message-content code {
    background: var(--bg);
    padding: 2px 6px;
    border-radius: 4px;
    font-size: 13px;
    font-family: 'SF Mono', Monaco, monospace;
  }

  .message-content pre {
    background: var(--bg);
    padding: 14px;
    border-radius: 8px;
    overflow-x: auto;
    margin: 8px 0;
  }

  .message-content pre code { padding: 0; background: none; }

  .message-time {
    font-size: 11px;
    color: var(--text-muted);
    margin-top: 4px;
  }

  .attachment-badge {
    margin-top: 8px;
    padding: 8px 12px;
    background: var(--bg);
    border-radius: 6px;
    font-size: 12px;
    display: inline-flex;
    align-items: center;
    gap: 6px;
  }

  /* ── Empty / Loading States ── */
  .empty-state {
    display: flex;
    align-items: center;
    justify-content: center;
    height: 100%;
    color: var(--text-muted);
    font-size: 15px;
    flex-direction: column;
    gap: 8px;
  }

  .empty-state .big { font-size: 48px; margin-bottom: 8px; }
  .empty-state .shortcut {
    margin-top: 12px;
    padding: 4px 10px;
    background: var(--bg-tertiary);
    border: 1px solid var(--border);
    border-radius: 6px;
    font-size: 12px;
    font-family: monospace;
  }

  .loading-overlay {
    position: fixed;
    inset: 0;
    background: var(--bg);
    display: flex;
    align-items: center;
    justify-content: center;
    flex-direction: column;
    gap: 16px;
    z-index: 100;
  }

  .loading-overlay .spinner {
    width: 40px;
    height: 40px;
    border: 3px solid var(--border);
    border-top-color: var(--accent);
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
  }

  /* ── Image Lightbox ── */
  .lightbox {
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.9);
    z-index: 200;
    align-items: center;
    justify-content: center;
    cursor: zoom-out;
  }

  .lightbox.open { display: flex; }

  .lightbox img {
    max-width: 95vw;
    max-height: 95vh;
    object-fit: contain;
    border-radius: 8px;
  }

  @keyframes spin { to { transform: rotate(360deg); } }

  ::-webkit-scrollbar { width: 8px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }
  ::-webkit-scrollbar-thumb:hover { background: var(--text-muted); }
</style>
</head>
<body>

<div class="loading-overlay" id="loading">
  <div class="spinner"></div>
  <div>Loading your ChatGPT archive...</div>
  <div id="load-status" style="font-size: 13px; color: var(--text-muted);">Initializing...</div>
</div>

<div class="sidebar">
  <div class="search-box">
    <input type="text" id="search" placeholder="Search conversations... (Cmd+K)" autofocus>
  </div>
  <div class="toolbar">
    <label>Sort:</label>
    <select id="sort-select">
      <option value="newest">Newest first</option>
      <option value="oldest">Oldest first</option>
      <option value="most-messages">Most messages</option>
      <option value="title-az">Title A-Z</option>
    </select>
    <label>Date:</label>
    <select id="date-filter">
      <option value="all">All time</option>
    </select>
  </div>
  <div class="search-stats" id="stats"></div>
  <div class="filter-bar" id="filters"></div>
  <ul class="convo-list" id="convo-list"></ul>
</div>

<div class="main">
  <div class="main-header" id="main-header" style="display: none;">
    <h2 id="convo-title"></h2>
    <div class="meta" id="convo-meta"></div>
  </div>
  <div class="messages" id="messages">
    <div class="empty-state">
      <div class="big">&#128172;</div>
      <div>Select a conversation to view</div>
      <div style="font-size: 13px; color: var(--text-muted);">or search to find something specific</div>
      <div class="shortcut">Cmd+K to search</div>
    </div>
  </div>
</div>

<div class="lightbox" id="lightbox">
  <img id="lightbox-img" src="" alt="">
</div>

<script src="data/index.js"></script>
<script>
// ── State ──
let DATA = window.CHATGPT_INDEX;
let convoMap = new Map();
let filteredConvos = [];
let currentConvoId = null;
let searchDebounce = null;
let renderBatchSize = 100;
let renderedCount = 0;
let activeModelFilter = null;
let activeDateFilter = 'all';
let activeSort = 'newest';
let loadedChunks = {};  // chunk_name -> {convo_id: messages}
let loadingChunks = {}; // chunk_name -> [callbacks]

function init() {
  const statusEl = document.getElementById('load-status');

  if (!DATA || !DATA.conversations) {
    statusEl.textContent = 'Error: Could not load data/index.js';
    statusEl.style.color = 'var(--red)';
    return;
  }

  statusEl.textContent = 'Indexing ' + DATA.conversations.length + ' conversations...';

  // Build map for fast lookup
  DATA.conversations.forEach(c => convoMap.set(c.id, c));

  // Build search index (title + preview only — instant)
  buildSearchIndex();
  buildDateFilter();

  applySortAndFilter();
  renderStats();
  renderModelFilters();
  resetAndRenderList();

  document.getElementById('loading').style.display = 'none';

  // Bind events
  document.getElementById('search').addEventListener('input', (e) => {
    clearTimeout(searchDebounce);
    searchDebounce = setTimeout(() => doSearch(e.target.value), 200);
  });

  document.getElementById('sort-select').addEventListener('change', (e) => {
    activeSort = e.target.value;
    applySortAndFilter();
    resetAndRenderList();
  });

  document.getElementById('date-filter').addEventListener('change', (e) => {
    activeDateFilter = e.target.value;
    applySortAndFilter();
    resetAndRenderList();
    renderStats();
  });

  document.getElementById('convo-list').addEventListener('scroll', onListScroll);
}

// ── Chunk Loading ──
function loadChunk(chunkName, callback) {
  // Already loaded
  if (loadedChunks[chunkName]) {
    callback(loadedChunks[chunkName]);
    return;
  }

  // Already loading — queue callback
  if (loadingChunks[chunkName]) {
    loadingChunks[chunkName].push(callback);
    return;
  }

  loadingChunks[chunkName] = [callback];

  var script = document.createElement('script');
  script.src = 'data/conversations/' + chunkName + '.js';
  script.onload = function() {
    var data = (window.CHATGPT_CHUNKS || {})[chunkName] || {};
    loadedChunks[chunkName] = data;
    var cbs = loadingChunks[chunkName] || [];
    delete loadingChunks[chunkName];
    cbs.forEach(function(cb) { cb(data); });
  };
  script.onerror = function() {
    var cbs = loadingChunks[chunkName] || [];
    delete loadingChunks[chunkName];
    cbs.forEach(function(cb) { cb(null); });
  };
  document.head.appendChild(script);
}

function getConversationMessages(convoId, callback) {
  var entry = convoMap.get(convoId);
  if (!entry || !entry.chunk) { callback([]); return; }

  loadChunk(entry.chunk, function(chunkData) {
    if (chunkData && chunkData[convoId]) {
      callback(chunkData[convoId]);
    } else {
      callback([]);
    }
  });
}

// ── Search Index (title + preview only) ──
let searchIndex = [];

function buildSearchIndex() {
  searchIndex = new Array(DATA.conversations.length);
  for (let i = 0; i < DATA.conversations.length; i++) {
    const convo = DATA.conversations[i];
    const text = ((convo.title || '') + '\n' + (convo.preview || '')).toLowerCase();
    searchIndex[i] = {
      title: (convo.title || '').toLowerCase(),
      text: text,
    };
  }
}

function doSearch(query) {
  query = query.trim().toLowerCase();

  if (!query) {
    applySortAndFilter();
    resetAndRenderList();
    renderStats();
    return;
  }

  const terms = query.split(/\s+/).filter(Boolean);
  const scored = [];
  const base = getDateFilteredConvos();
  const baseSet = new Set(base.map(c => c.id));

  for (let i = 0; i < DATA.conversations.length; i++) {
    if (!baseSet.has(DATA.conversations[i].id)) continue;
    if (activeModelFilter && (DATA.conversations[i].model || 'unknown') !== activeModelFilter) continue;

    const idx = searchIndex[i];
    let score = 0;
    let allMatch = true;

    for (const term of terms) {
      const titleMatch = idx.title.includes(term);
      const textPos = idx.text.indexOf(term);

      if (titleMatch) score += 10;
      if (textPos !== -1) {
        score += 1;
      } else if (!titleMatch) {
        allMatch = false;
        break;
      }
    }

    if (allMatch && score > 0) {
      scored.push({ index: i, score });
    }
  }

  scored.sort((a, b) => b.score - a.score);
  filteredConvos = scored.map(s => DATA.conversations[s.index]);

  resetAndRenderList(query);
  renderStats(query);
}

// ── Date Filter ──
function buildDateFilter() {
  const select = document.getElementById('date-filter');
  const months = new Map();

  DATA.conversations.forEach(c => {
    if (!c.create_time) return;
    const d = new Date(c.create_time * 1000);
    const key = d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0');
    const label = d.toLocaleString('default', { month: 'short', year: 'numeric' });
    if (!months.has(key)) months.set(key, { label, count: 0 });
    months.get(key).count++;
  });

  const years = new Map();
  for (const [key, val] of months) {
    const year = key.split('-')[0];
    if (!years.has(year)) years.set(year, 0);
    years.set(year, years.get(year) + val.count);
  }

  for (const [year, count] of [...years].sort((a, b) => b[0] - a[0])) {
    const opt = document.createElement('option');
    opt.value = 'year-' + year;
    opt.textContent = year + ' (' + count + ')';
    select.appendChild(opt);
  }

  const sortedMonths = [...months].sort((a, b) => b[0].localeCompare(a[0]));
  for (const [key, val] of sortedMonths) {
    const opt = document.createElement('option');
    opt.value = key;
    opt.textContent = '  ' + val.label + ' (' + val.count + ')';
    select.appendChild(opt);
  }
}

function getDateFilteredConvos() {
  if (activeDateFilter === 'all') return DATA.conversations;

  return DATA.conversations.filter(c => {
    if (!c.create_time) return false;
    const d = new Date(c.create_time * 1000);
    if (activeDateFilter.startsWith('year-')) {
      return String(d.getFullYear()) === activeDateFilter.replace('year-', '');
    }
    const key = d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0');
    return key === activeDateFilter;
  });
}

// ── Sort & Filter ──
function applySortAndFilter() {
  let convos = getDateFilteredConvos();

  if (activeModelFilter) {
    convos = convos.filter(c => (c.model || 'unknown') === activeModelFilter);
  }

  switch (activeSort) {
    case 'newest':
      convos.sort((a, b) => (b.update_time || 0) - (a.update_time || 0));
      break;
    case 'oldest':
      convos.sort((a, b) => (a.create_time || 0) - (b.create_time || 0));
      break;
    case 'most-messages':
      convos.sort((a, b) => (b.message_count || 0) - (a.message_count || 0));
      break;
    case 'title-az':
      convos.sort((a, b) => (a.title || '').localeCompare(b.title || ''));
      break;
  }

  filteredConvos = convos;
}

// ── Render ──
function renderStats(query) {
  const el = document.getElementById('stats');
  const total = DATA.conversations.length;
  const shown = filteredConvos.length;
  const totalMessages = DATA.conversations.reduce((sum, c) => sum + (c.message_count || 0), 0);

  if (query) {
    el.innerHTML = '<span>' + shown.toLocaleString() + ' results</span><span>' + total.toLocaleString() + ' total</span>';
  } else {
    el.innerHTML = '<span>' + shown.toLocaleString() + ' conversations</span><span>' + totalMessages.toLocaleString() + ' messages</span>';
  }
}

function renderModelFilters() {
  const models = {};
  DATA.conversations.forEach(c => {
    const m = c.model || 'unknown';
    models[m] = (models[m] || 0) + 1;
  });

  const el = document.getElementById('filters');
  const sorted = Object.entries(models).sort((a, b) => b[1] - a[1]).slice(0, 8);

  el.innerHTML = sorted.map(([model, count]) =>
    '<button class="filter-btn" data-model="' + model + '">' + model + ' (' + count + ')</button>'
  ).join('');

  el.addEventListener('click', (e) => {
    const btn = e.target.closest('.filter-btn');
    if (!btn) return;

    const isActive = btn.classList.contains('active');
    el.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));

    if (isActive) {
      activeModelFilter = null;
    } else {
      btn.classList.add('active');
      activeModelFilter = btn.dataset.model;
    }

    const query = document.getElementById('search').value.trim();
    if (query) {
      doSearch(query);
    } else {
      applySortAndFilter();
      resetAndRenderList();
      renderStats();
    }
  });
}

function resetAndRenderList(highlightQuery) {
  renderedCount = 0;
  document.getElementById('convo-list').innerHTML = '';
  renderMoreItems(highlightQuery);
}

function renderMoreItems(highlightQuery) {
  const el = document.getElementById('convo-list');
  const start = renderedCount;
  const end = Math.min(renderedCount + renderBatchSize, filteredConvos.length);

  if (start >= end) return;

  const fragment = document.createDocumentFragment();

  for (let i = start; i < end; i++) {
    const convo = filteredConvos[i];
    const li = document.createElement('li');
    li.className = 'convo-item' + (convo.id === currentConvoId ? ' active' : '');
    li.dataset.id = convo.id;
    li.dataset.index = i;

    const date = convo.create_time
      ? new Date(convo.create_time * 1000).toLocaleDateString()
      : '';
    const msgCount = convo.message_count || 0;

    let title = escapeHtml(convo.title || 'Untitled');
    let preview = escapeHtml((convo.preview || '').substring(0, 100));

    if (highlightQuery) {
      title = highlightText(title, highlightQuery);
      preview = highlightText(preview, highlightQuery);
    }

    li.innerHTML =
      '<div class="convo-title">' + title + '</div>' +
      '<div class="convo-meta">' +
        '<span>' + date + '</span>' +
        '<span>' + msgCount + ' msgs</span>' +
        '<span>' + (convo.model || '') + '</span>' +
      '</div>' +
      (preview ? '<div class="convo-preview">' + preview + '</div>' : '');

    li.addEventListener('click', () => selectConversation(convo.id));
    fragment.appendChild(li);
  }

  el.appendChild(fragment);
  renderedCount = end;

  const existing = el.querySelector('.load-more-sentinel');
  if (existing) existing.remove();

  if (renderedCount < filteredConvos.length) {
    const sentinel = document.createElement('li');
    sentinel.className = 'load-more-sentinel';
    sentinel.textContent = 'Showing ' + renderedCount + ' of ' + filteredConvos.length + ' — scroll for more';
    el.appendChild(sentinel);
  }
}

function onListScroll(e) {
  const el = e.target;
  if (el.scrollTop + el.clientHeight >= el.scrollHeight - 200) {
    const query = document.getElementById('search').value.trim().toLowerCase();
    renderMoreItems(query || undefined);
  }
}

function selectConversation(id) {
  currentConvoId = id;
  const convo = convoMap.get(id);
  if (!convo) return;

  // Update sidebar active
  document.querySelectorAll('.convo-item.active').forEach(el => el.classList.remove('active'));
  const active = document.querySelector('.convo-item[data-id="' + id + '"]');
  if (active) active.classList.add('active');

  // Header
  const header = document.getElementById('main-header');
  header.style.display = 'block';
  document.getElementById('convo-title').textContent = convo.title || 'Untitled';

  const created = convo.create_time ? new Date(convo.create_time * 1000).toLocaleString() : '';
  const updated = convo.update_time ? new Date(convo.update_time * 1000).toLocaleString() : '';
  document.getElementById('convo-meta').innerHTML =
    '<span>Created: ' + created + '</span>' +
    '<span>Updated: ' + updated + '</span>' +
    '<span>' + (convo.message_count || 0) + ' messages</span>' +
    '<span>' + (convo.model || '') + '</span>' +
    (convo.is_archived ? '<span style="color: var(--red);">Archived</span>' : '') +
    (convo.gizmo_id ? '<span style="color: var(--purple);">Custom GPT</span>' : '');

  // Show loading state in messages area
  const messagesEl = document.getElementById('messages');
  messagesEl.innerHTML = '<div class="empty-state"><div class="spinner" style="width:24px;height:24px;border:2px solid var(--border);border-top-color:var(--accent);border-radius:50%;animation:spin 0.8s linear infinite;"></div><div>Loading messages...</div></div>';

  // Lazy-load chunk then render messages
  getConversationMessages(id, function(messages) {
    // Check we haven't navigated away
    if (currentConvoId !== id) return;
    renderMessages(id, messages);
  });
}

function renderMessages(convoId, messages) {
  const messagesEl = document.getElementById('messages');
  const fragment = document.createDocumentFragment();
  const searchQuery = document.getElementById('search').value.trim().toLowerCase();

  (messages || []).forEach(msg => {
    if (msg.role === 'system' && !msg.content) return;
    if (msg.content_type === 'user_editable_context' && !msg.content) return;
    if (msg.content_type === 'model_editable_context' && !msg.content) return;

    const div = document.createElement('div');
    div.className = 'message ' + msg.role;

    let content = escapeHtml(msg.content || '');
    content = renderMarkdown(content);

    if (searchQuery) {
      content = highlightText(content, searchQuery);
    }

    const time = msg.timestamp
      ? new Date(msg.timestamp * 1000).toLocaleString()
      : '';

    let mediaHtml = '';
    if (msg.media && msg.media.length > 0) {
      mediaHtml = msg.media.map(m => {
        if (m.filename && (m.type === 'image' || m.type === 'dalle_image')) {
          const src = 'conversations/' + convoId + '/media/' + m.filename;
          return '<img src="' + src + '" alt="' + m.type + '" loading="lazy" onclick="openLightbox(this.src)">';
        } else if (m.filename && m.type === 'attachment') {
          const src = 'conversations/' + convoId + '/media/' + m.filename;
          return '<div class="attachment-badge"><a href="' + src + '" target="_blank" style="color: var(--accent); text-decoration: none;">&#128206; ' + escapeHtml(m.original_name || m.filename) + '</a></div>';
        } else if (m.original_name) {
          return '<div class="attachment-badge">&#128206; ' + escapeHtml(m.original_name) + '</div>';
        }
        return '';
      }).join('');
    }

    div.innerHTML =
      '<div class="message-role ' + msg.role + '">' + msg.role + (msg.model ? ' &middot; ' + msg.model : '') + '</div>' +
      '<div class="message-content">' + content + mediaHtml + '</div>' +
      (time ? '<div class="message-time">' + time + '</div>' : '');

    fragment.appendChild(div);
  });

  messagesEl.innerHTML = '';
  if (fragment.childNodes.length === 0) {
    messagesEl.innerHTML = '<div class="empty-state"><div>No messages in this conversation</div></div>';
  } else {
    messagesEl.appendChild(fragment);
  }
  messagesEl.scrollTop = 0;
}

// ── Lightbox ──
function openLightbox(src) {
  const lb = document.getElementById('lightbox');
  document.getElementById('lightbox-img').src = src;
  lb.classList.add('open');
}

document.getElementById('lightbox').addEventListener('click', () => {
  document.getElementById('lightbox').classList.remove('open');
});

// ── Helpers ──
function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

function highlightText(html, query) {
  const terms = query.toLowerCase().split(/\s+/).filter(Boolean);
  for (const term of terms) {
    const escaped = term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const regex = new RegExp('(' + escaped + ')', 'gi');
    html = html.replace(regex, '<mark>$1</mark>');
  }
  return html;
}

function renderMarkdown(text) {
  text = text.replace(/```(\w*)\n([\s\S]*?)```/g, '<pre><code>$2</code></pre>');
  text = text.replace(/`([^`]+)`/g, '<code>$1</code>');
  text = text.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
  text = text.replace(/(?<!\w)\*(.+?)\*(?!\w)/g, '<em>$1</em>');
  text = text.replace(/^### (.+)$/gm, '<strong style="font-size:15px;">$1</strong>');
  text = text.replace(/^## (.+)$/gm, '<strong style="font-size:16px;">$1</strong>');
  text = text.replace(/^# (.+)$/gm, '<strong style="font-size:18px;">$1</strong>');
  text = text.replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank" style="color: var(--accent);">$1</a>');
  text = text.replace(/^---$/gm, '<hr style="border: none; border-top: 1px solid var(--border); margin: 12px 0;">');
  return text;
}

// ── Keyboard Shortcuts ──
document.addEventListener('keydown', (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
    e.preventDefault();
    document.getElementById('search').focus();
    document.getElementById('search').select();
  }

  if (e.key === 'Escape') {
    const lb = document.getElementById('lightbox');
    if (lb.classList.contains('open')) {
      lb.classList.remove('open');
      return;
    }
    const search = document.getElementById('search');
    if (search.value) {
      search.value = '';
      doSearch('');
      search.focus();
    }
  }

  if (e.key === 'ArrowDown' && e.altKey) {
    e.preventDefault();
    navigateConvo(1);
  }
  if (e.key === 'ArrowUp' && e.altKey) {
    e.preventDefault();
    navigateConvo(-1);
  }
});

function navigateConvo(direction) {
  if (!currentConvoId) {
    if (filteredConvos.length > 0) selectConversation(filteredConvos[0].id);
    return;
  }
  const idx = filteredConvos.findIndex(c => c.id === currentConvoId);
  const next = idx + direction;
  if (next >= 0 && next < filteredConvos.length) {
    selectConversation(filteredConvos[next].id);
    const el = document.querySelector('.convo-item[data-id="' + filteredConvos[next].id + '"]');
    if (el) el.scrollIntoView({ block: 'nearest' });
  }
}

init();
</script>
</body>
</html>
'''


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    export_dir = args.output

    if args.viewer_only:
        print("Regenerating viewer from existing data...")
        records = build_index_from_disk(export_dir)
        if records:
            generate_viewer(export_dir, records)
        else:
            print("No conversation data found.")
        return

    # Get credentials (browser login, cache, or --token)
    creds = get_credentials(args, export_dir)

    # Create authenticated session
    session = create_session(creds)

    # Run the export
    records = run_export(session, export_dir, no_media=args.no_media)

    # Generate viewer
    if records:
        generate_viewer(export_dir, records)
    else:
        print("\nNo conversations to generate viewer for.")


if __name__ == "__main__":
    main()

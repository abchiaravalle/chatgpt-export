# ChatGPT Export

Export all your ChatGPT conversations and browse them offline.

## Quick Start

```bash
git clone <this-repo>
cd chatgpt-export
python3 chatgpt_export.py
```

That's it. The script will:

1. Install dependencies automatically (playwright, requests)
2. Open a browser window — log into ChatGPT normally
3. Capture your auth token from network requests
4. Download all conversations and media
5. Generate an offline viewer

When it's done, open `export/viewer.html` by double-clicking it.

## Options

```
python3 chatgpt_export.py                    # Full flow
python3 chatgpt_export.py --token TOKEN      # Skip browser login
python3 chatgpt_export.py --viewer-only      # Regenerate viewer from existing data
python3 chatgpt_export.py --no-media         # Skip media downloads
python3 chatgpt_export.py --output DIR       # Custom output directory (default: export)
```

## Resume Support

If the export is interrupted (network error, rate limit, etc.), just re-run the script. It tracks progress and picks up where it left off.

## Output Structure

```
export/
  viewer.html                    <- open this
  data/
    index.js                     <- sidebar data (~1MB)
    conversations/
      chunk_000.js               <- message data (~5MB each)
      chunk_001.js
      ...
  conversations/                 <- raw data + media
    {convo_id}/
      conversation.json
      media/
        {file_id}.ext
  .progress.json
  .credentials.json
```

## Viewer Features

- Dark theme
- Full-text search (title + preview)
- Filter by date (month/year) and model
- Sort by newest, oldest, most messages, or title
- Keyboard shortcuts: Cmd+K to search, Alt+Up/Down to navigate, Esc to clear
- Image lightbox
- Works offline via `file://` protocol (no server needed)

## Requirements

- Python 3.8+
- Internet connection (for downloading)
- Dependencies are auto-installed: `playwright`, `requests`

On modern macOS/Linux where system Python is "externally managed", the script automatically creates a `.venv/` virtual environment and re-launches itself inside it. No manual setup needed.

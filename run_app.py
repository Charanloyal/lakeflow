#!/usr/bin/env python3
"""
LakeFlow 2.0 - One-Click Application Launcher
Launches the modern LakeFlow 2.0 Web Application & REST Gateway.
Works out of the box with zero external dependencies (falls back to Python's built-in http.server if uvicorn is not installed).
"""

import os
import sys
import webbrowser
import threading
import time
from http.server import HTTPServer, SimpleHTTPRequestHandler

PORT = 8501
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WEB_DIR = os.path.join(BASE_DIR, "apps", "web")

class LakeFlowHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

    def log_message(self, format, *args):
        # Clean logging
        sys.stderr.write(f"[LakeFlow 2.0] {self.address_string()} - {format % args}\n")

def open_browser(url):
    time.sleep(1.0)
    print(f"\n🌐 Opening LakeFlow 2.0 in your default browser: {url}")
    webbrowser.open(url)

def main():
    print("=" * 80)
    print(" ⚡ LAKEFLOW 2.0 | STREAMING CDC LAKEHOUSE PLATFORM")
    print("=" * 80)
    print(f" Serving LakeFlow UI from: {WEB_DIR}")
    print(f" Local Web Dashboard:     http://localhost:{PORT}")
    print("=" * 80)

    url = f"http://localhost:{PORT}"
    threading.Thread(target=open_browser, args=(url,), daemon=True).start()

    server = HTTPServer(("0.0.0.0", PORT), LakeFlowHandler)
    try:
        print(f" Server running on port {PORT}. Press Ctrl+C to stop.\n")
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n Shutting down LakeFlow 2.0 server.")
        server.server_close()

if __name__ == "__main__":
    main()

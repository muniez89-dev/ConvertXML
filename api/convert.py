import base64
import json
import os
from http.server import BaseHTTPRequestHandler
from pathlib import Path

from src.pain001_v09 import csv_to_pain09_xml


AUTH_TOKEN = os.environ.get("API_TOKEN", "").strip()


class handler(BaseHTTPRequestHandler):
    def _send(self, status: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        # Segurança simples (recomendado)
        if AUTH_TOKEN:
            auth = self.headers.get("Authorization", "")
            if auth != f"Bearer {AUTH_TOKEN}":
                return self._send(401, {"error": "unauthorized"})

        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            data = json.loads(raw.decode("utf-8"))

            filename = data.get("filename", "lote.csv")
            csv_b64 = data["csv_base64"]

            csv_bytes = base64.b64decode(csv_b64)
            csv_text = csv_bytes.decode("utf-8-sig")

            # XSD incluído no repo (ao lado do pain001_v09.py)
            here = Path(__file__).resolve().parent.parent  # /api -> repo root
            xsd_path = here / "pain.001.001.09.xsd"

            xml_text = csv_to_pain09_xml(csv_text, xsd_path=xsd_path)

            xml_b64 = base64.b64encode(xml_text.encode("utf-8")).decode("ascii")
            xml_name = filename.rsplit(".", 1)[0] + ".xml"

            return self._send(200, {"xml_filename": xml_name, "xml_base64": xml_b64})

        except Exception as e:
            return self._send(400, {"error": str(e)})

import base64
import csv
import json
from io import StringIO
import xml.etree.ElementTree as ET

def _csv_text_to_xml(csv_text: str, delimiter: str = ";") -> str:
    f = StringIO(csv_text)
    reader = csv.DictReader(f, delimiter=delimiter)

    root = ET.Element("Faturas")

    for row in reader:
        fatura = ET.SubElement(root, "Fatura")
        ET.SubElement(fatura, "CodFatura").text = (row.get("CodFatura") or "").strip()
        ET.SubElement(fatura, "NIF").text = (row.get("NIF") or "").strip()
        ET.SubElement(fatura, "Empresa").text = (row.get("Empresa") or "").strip()
        ET.SubElement(fatura, "Valor").text = (row.get("Valor") or "").strip()
        ET.SubElement(fatura, "Data").text = (row.get("Data") or "").strip()

    return ET.tostring(root, encoding="utf-8", xml_declaration=True).decode("utf-8")


def handler(request):
    # Vercel Python Function handler
    if request.method != "POST":
        return (json.dumps({"error": "Use POST"}), 405, {"Content-Type": "application/json"})

    try:
        body = request.get_json()
    except Exception:
        body = None

    if not body:
        return (json.dumps({"error": "JSON inválido"}), 400, {"Content-Type": "application/json"})

    csv_base64 = body.get("csvBase64", "")
    delimiter = body.get("delimiter", ";")

    if not csv_base64:
        return (json.dumps({"error": "csvBase64 em falta"}), 400, {"Content-Type": "application/json"})

    csv_bytes = base64.b64decode(csv_base64)
    csv_text = csv_bytes.decode("utf-8", errors="replace")

    xml_text = _csv_text_to_xml(csv_text, delimiter=delimiter)
    xml_base64 = base64.b64encode(xml_text.encode("utf-8")).decode("utf-8")

    return (json.dumps({"xmlBase64": xml_base64}), 200, {"Content-Type": "application/json"})

from __future__ import annotations

import csv
import io
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import xml.etree.ElementTree as ET
from xml.dom import minidom
from lxml import etree


# =========================
# Namespace v09
# =========================
PAIN09_NS = "urn:iso:std:iso:20022:tech:xsd:pain.001.001.09"
ET.register_namespace("", PAIN09_NS)

# limites (práticos)
MAX_E2E = 35
MAX_USTRD = 140
MAX_NAME = 140

IBAN_REGEX = re.compile(r"^[A-Z]{2}[0-9]{2}[A-Z0-9]{1,30}$")
BICFI_REGEX = re.compile(r"^[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?$")  # 8/11


def normalize_text(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[\x00-\x1F\x7F]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s


def ensure_len(label: str, s: str, max_len: int) -> str:
    s = normalize_text(s)
    if len(s) > max_len:
        raise ValueError(f"{label} excede {max_len} caracteres: {len(s)}")
    return s


def iban_is_valid(iban: str) -> bool:
    iban = normalize_text(iban).replace(" ", "").upper()
    if not IBAN_REGEX.match(iban):
        return False
    rearranged = iban[4:] + iban[:4]
    digits = ""
    for ch in rearranged:
        digits += ch if ch.isdigit() else str(ord(ch) - 55)
    remainder = 0
    for i in range(0, len(digits), 9):
        remainder = int(str(remainder) + digits[i : i + 9]) % 97
    return remainder == 1


def bicfi_is_valid(bicfi: str) -> bool:
    return bool(BICFI_REGEX.match(normalize_text(bicfi).upper()))


def parse_decimal_eur(value: str) -> Decimal:
    v_raw = normalize_text(value)
    # suporta PT: "1.234,56" => "1234.56"
    if "," in v_raw:
        v = v_raw.replace(".", "").replace(",", ".")
    else:
        v = v_raw
    try:
        d = Decimal(v)
    except InvalidOperation:
        raise ValueError(f"Valor inválido: {value!r}")
    if d <= 0:
        raise ValueError(f"Montante deve ser > 0: {value!r}")
    return d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def parse_date_pt(value: str) -> date:
    v = normalize_text(value)
    try:
        return datetime.strptime(v, "%d/%m/%Y").date()
    except ValueError:
        raise ValueError(f"Data inválida (esperado dd/mm/yyyy): {value!r}")


def make_msg_id(prefix: str = "C2B") -> str:
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    rnd = uuid.uuid4().hex[:8].upper()
    return ensure_len("MsgId", f"{prefix}-{ts}-{rnd}", 35)


def make_pmt_inf_id(prefix: str = "PMT") -> str:
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    rnd = uuid.uuid4().hex[:6].upper()
    return ensure_len("PmtInfId", f"{prefix}-{ts}-{rnd}", 35)


@dataclass(frozen=True)
class BatchHeader:
    debtor_name: str
    debtor_nif: str
    debtor_iban: str
    debtor_bicfi: str
    exec_date: date


@dataclass(frozen=True)
class PaymentRow:
    creditor_name: str
    creditor_iban: str
    creditor_bicfi: Optional[str]
    amount: Decimal


# Header FINAL (sem NIF_FORNECEDOR)
CSV_HEADER = [
    "Nome_ORDENANTE",
    "NIF_ORDENANTE",
    "IBAN_ORDENANTE",
    "BIC_ORDENANTE",
    "Data_EXECUCAO",
    "Valor",
    "Nome_FORNECEDOR",
    "IBAN_FORNECEDOR",
    "BIC_FORNECEDOR",
]


def _parse_payments_reader(reader: csv.DictReader, delimiter: str = ";") -> Tuple[BatchHeader, List[PaymentRow]]:
    if reader.fieldnames is None:
        raise ValueError("CSV sem cabeçalho (header).")

    received = [normalize_text(h) for h in reader.fieldnames]
    missing = [c for c in CSV_HEADER if c not in set(received)]
    if missing:
        raise ValueError(f"CSV inválido. Faltam colunas: {missing}. Header recebido: {received}")

    batch: Optional[BatchHeader] = None
    payments: List[PaymentRow] = []
    exec_dates: set[date] = set()

    for line_no, row in enumerate(reader, start=2):
        debtor_name = ensure_len("Nome_ORDENANTE", row.get("Nome_ORDENANTE", ""), MAX_NAME)

        debtor_nif = normalize_text(row.get("NIF_ORDENANTE", ""))
        if not debtor_nif.isdigit():
            raise ValueError(f"Linha {line_no}: NIF_ORDENANTE inválido: {debtor_nif!r}")
        debtor_nif = ensure_len("NIF_ORDENANTE", debtor_nif, 35)

        debtor_iban = normalize_text(row.get("IBAN_ORDENANTE", "")).replace(" ", "").upper()
        if not iban_is_valid(debtor_iban):
            raise ValueError(f"Linha {line_no}: IBAN_ORDENANTE inválido: {debtor_iban!r}")

        debtor_bicfi = normalize_text(row.get("BIC_ORDENANTE", "")).upper()
        if not bicfi_is_valid(debtor_bicfi):
            raise ValueError(f"Linha {line_no}: BIC_ORDENANTE inválido: {debtor_bicfi!r}")

        exec_date = parse_date_pt(row.get("Data_EXECUCAO", ""))
        exec_dates.add(exec_date)

        if batch is None:
            batch = BatchHeader(
                debtor_name=debtor_name,
                debtor_nif=debtor_nif,
                debtor_iban=debtor_iban,
                debtor_bicfi=debtor_bicfi,
                exec_date=exec_date,
            )
        else:
            if (
                batch.debtor_name != debtor_name
                or batch.debtor_nif != debtor_nif
                or batch.debtor_iban != debtor_iban
                or batch.debtor_bicfi != debtor_bicfi
            ):
                raise ValueError(f"Linha {line_no}: Ordenante diferente dentro do mesmo lote (não permitido).")

        amount = parse_decimal_eur(row.get("Valor", ""))
        creditor_name = ensure_len("Nome_FORNECEDOR", row.get("Nome_FORNECEDOR", ""), MAX_NAME)

        creditor_iban = normalize_text(row.get("IBAN_FORNECEDOR", "")).replace(" ", "").upper()
        if not iban_is_valid(creditor_iban):
            raise ValueError(f"Linha {line_no}: IBAN_FORNECEDOR inválido: {creditor_iban!r}")

        creditor_bicfi = normalize_text(row.get("BIC_FORNECEDOR", "")).upper()
        if creditor_bicfi:
            if not bicfi_is_valid(creditor_bicfi):
                raise ValueError(f"Linha {line_no}: BIC_FORNECEDOR inválido: {creditor_bicfi!r}")
        else:
            creditor_bicfi = None

        payments.append(
            PaymentRow(
                creditor_name=creditor_name,
                creditor_iban=creditor_iban,
                creditor_bicfi=creditor_bicfi,
                amount=amount,
            )
        )

    if batch is None or not payments:
        raise ValueError("CSV sem linhas válidas.")

    if len(exec_dates) != 1:
        raise ValueError(f"CSV inválido: Data_EXECUCAO não é igual em todas as linhas: {sorted(exec_dates)}")

    return batch, payments


def parse_payments_csv(path: Path, delimiter: str = ";") -> Tuple[BatchHeader, List[PaymentRow]]:
    text = path.read_text(encoding="utf-8-sig")
    return parse_payments_csv_text(text, delimiter=delimiter)


def parse_payments_csv_text(csv_text: str, delimiter: str = ";") -> Tuple[BatchHeader, List[PaymentRow]]:
    f = io.StringIO(csv_text)
    reader = csv.DictReader(f, delimiter=delimiter)
    return _parse_payments_reader(reader, delimiter=delimiter)


def calc_ctrl_sum(rows: List[PaymentRow]) -> Decimal:
    total = sum((r.amount for r in rows), Decimal("0.00"))
    return total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def el(parent: ET.Element, tag: str, text: Optional[str] = None, attrib: Optional[Dict[str, str]] = None) -> ET.Element:
    node = ET.SubElement(parent, f"{{{PAIN09_NS}}}{tag}", attrib=attrib or {})
    if text is not None:
        node.text = text
    return node


def build_grp_hdr(cstmr: ET.Element, batch: BatchHeader, payments: List[PaymentRow]) -> None:
    grp = el(cstmr, "GrpHdr")
    el(grp, "MsgId", make_msg_id("C2B"))
    el(grp, "CreDtTm", datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
    el(grp, "NbOfTxs", str(len(payments)))
    el(grp, "CtrlSum", f"{calc_ctrl_sum(payments):.2f}")

    initg = el(grp, "InitgPty")
    el(initg, "Nm", batch.debtor_name)

    # Mantemos o NIF do ordenante como identificador do initiating party
    init_id = el(initg, "Id")
    prvt = el(init_id, "PrvtId")
    othr = el(prvt, "Othr")
    el(othr, "Id", batch.debtor_nif)


def build_pmt_inf(cstmr: ET.Element, batch: BatchHeader, payments: List[PaymentRow]) -> ET.Element:
    pmt = el(cstmr, "PmtInf")
    el(pmt, "PmtInfId", make_pmt_inf_id("PMT"))
    el(pmt, "PmtMtd", "TRF")
    el(pmt, "NbOfTxs", str(len(payments)))
    el(pmt, "CtrlSum", f"{calc_ctrl_sum(payments):.2f}")

    pmt_tp = el(pmt, "PmtTpInf")
    svc = el(pmt_tp, "SvcLvl")
    el(svc, "Cd", "SEPA")
    ctgy = el(pmt_tp, "CtgyPurp")
    el(ctgy, "Cd", "SUPP")

    # pain.001.001.09: ReqdExctnDt (choice) com Dt
    red = el(pmt, "ReqdExctnDt")
    el(red, "Dt", batch.exec_date.strftime("%Y-%m-%d"))

    dbtr = el(pmt, "Dbtr")
    el(dbtr, "Nm", batch.debtor_name)

    dbtr_acct = el(pmt, "DbtrAcct")
    dbtr_id = el(dbtr_acct, "Id")
    el(dbtr_id, "IBAN", batch.debtor_iban)
    el(dbtr_acct, "Ccy", "EUR")

    dbtr_agt = el(pmt, "DbtrAgt")
    fin = el(dbtr_agt, "FinInstnId")
    el(fin, "BICFI", batch.debtor_bicfi)

    return pmt


def build_cdt_trf_tx_inf(pmt: ET.Element, row: PaymentRow) -> None:
    tx = el(pmt, "CdtTrfTxInf")

    pmt_id = el(tx, "PmtId")
    el(pmt_id, "EndToEndId", "NOTPROVIDED")

    amt = el(tx, "Amt")
    el(amt, "InstdAmt", f"{row.amount:.2f}", attrib={"Ccy": "EUR"})

    if row.creditor_bicfi:
        cdtr_agt = el(tx, "CdtrAgt")
        fin = el(cdtr_agt, "FinInstnId")
        el(fin, "BICFI", row.creditor_bicfi)

    cdtr = el(tx, "Cdtr")
    el(cdtr, "Nm", row.creditor_name)

    cdtr_acct = el(tx, "CdtrAcct")
    cdtr_id = el(cdtr_acct, "Id")
    el(cdtr_id, "IBAN", row.creditor_iban)

    purp = el(tx, "Purp")
    el(purp, "Cd", "SUPP")

    rmt = el(tx, "RmtInf")
    el(rmt, "Ustrd", "PAGAMENTO")


def build_document(batch: BatchHeader, payments: List[PaymentRow]) -> ET.ElementTree:
    doc = ET.Element(f"{{{PAIN09_NS}}}Document")
    cstmr = el(doc, "CstmrCdtTrfInitn")
    build_grp_hdr(cstmr, batch, payments)
    pmt = build_pmt_inf(cstmr, batch, payments)
    for r in payments:
        build_cdt_trf_tx_inf(pmt, r)
    return ET.ElementTree(doc)


def xml_pretty(tree: ET.ElementTree) -> str:
    raw = ET.tostring(tree.getroot(), encoding="utf-8", xml_declaration=True)
    parsed = minidom.parseString(raw)
    return parsed.toprettyxml(indent=" ", encoding="utf-8").decode("utf-8")


def validate_against_xsd(xml_string: str, xsd_path: Path) -> None:
    xml_doc = etree.fromstring(xml_string.encode("utf-8"))
    xsd_doc = etree.parse(str(xsd_path))
    schema = etree.XMLSchema(xsd_doc)
    if not schema.validate(xml_doc):
        errors = "\n".join(str(e) for e in schema.error_log)
        raise ValueError(f"XML NÃO valida no XSD:\n{errors}")


def csv_to_pain09_xml(csv_text: str, xsd_path: Optional[Path] = None, delimiter: str = ";") -> str:
    batch, payments = parse_payments_csv_text(csv_text, delimiter=delimiter)
    tree = build_document(batch, payments)
    xml_out = xml_pretty(tree)

    if xsd_path is not None and xsd_path.exists():
        validate_against_xsd(xml_out, xsd_path)

    return xml_out


# =========================
# Execução local (teste)
# =========================
def main() -> None:
    here = Path(__file__).resolve().parent
    csv_path = here / "lote.csv"
    xsd_path = here / "pain.001.001.09.xsd"
    out_path = here / "lote.xml"

    if not csv_path.exists():
        raise SystemExit(f"Não encontrei {csv_path}")
    if not xsd_path.exists():
        raise SystemExit(f"Não encontrei {xsd_path}")

    csv_text = csv_path.read_text(encoding="utf-8-sig")
    xml_out = csv_to_pain09_xml(csv_text, xsd_path=xsd_path)

    out_path.write_text(xml_out, encoding="utf-8")
    print(f"OK: gerado {out_path.name} e validado no XSD.")


if __name__ == "__main__":
    main()

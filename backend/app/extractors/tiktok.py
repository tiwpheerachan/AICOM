# -*- coding: utf-8 -*-
# backend/app/extractors/tiktok.py
from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple

from .common import normalize_text, finalize_row

# Prefer vendor code mapping from your source-of-truth mapping module
try:
    from .vendor_mapping import get_vendor_code  # type: ignore
except Exception:
    get_vendor_code = None  # type: ignore


# -------------------------------------------------------------------
# TikTok Tax Invoice / Receipt patterns (robust)
# -------------------------------------------------------------------

# Invoice number seen on doc: TTSTH20250008665805 (or similar)
RE_TIKTOK_INVOICE_NO = re.compile(r"\bTTSTH\d{8,}\b", re.IGNORECASE)

# Header label lines
RE_INVOICE_NUMBER_LINE = re.compile(
    r"(invoice\s*(?:no|number))\s*[:：#\-]?\s*([A-Za-z0-9][A-Za-z0-9\-_\/]{6,})",
    re.IGNORECASE,
)
RE_INVOICE_DATE_LINE = re.compile(
    r"(invoice\s*date)\s*[:：\-]?\s*(.+)",
    re.IGNORECASE,
)

RE_VENDOR_TAX_LINE = re.compile(
    r"(tax\s*registration\s*number)\s*[:：\-]?\s*(\d{13})",
    re.IGNORECASE,
)
RE_TAX_ID_13_ANY = re.compile(r"\b\d{13}\b")

RE_BRANCH_5 = re.compile(r"(branch|สาขา)\s*[:\-]?\s*(\d{1,5})", re.IGNORECASE)

# Dates
RE_DATE_YMD = re.compile(r"\b(\d{4})[-/](\d{1,2})[-/](\d{1,2})\b")
RE_DATE_MON_DD_YYYY = re.compile(
    r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),\s*(\d{4})\b",
    re.IGNORECASE,
)

# Money and totals
RE_MONEY = re.compile(r"(-?\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?|-?\d+(?:\.\d{1,2})?)")
RE_TOTAL_INCL = re.compile(
    r"(total\s*amount\s*\(\s*including\s*vat\s*\)|total\s*amount.*including\s*vat|amount\s*in\s*thb\s*\(\s*including\s*vat\s*\)|grand\s*total|amount\s*due)",
    re.IGNORECASE,
)
RE_TOTAL_VAT = re.compile(r"(total\s*vat\s*7%|total\s*vat|vat\s*amount|value\s*added\s*tax)", re.IGNORECASE)
RE_SUBTOTAL_EXCL = re.compile(
    r"(subtotal\s*\(\s*excluding\s*vat\s*\)|subtotal.*excluding\s*vat|total.*excluding\s*vat|amount\s*in\s*thb\s*\(\s*excluding\s*vat\s*\))",
    re.IGNORECASE,
)

# WHT (TikTok footer: withheld tax at rate 3% amounting to ฿4,414.88)
RE_WHT_AMOUNTING = re.compile(
    r"(withheld\s*tax|withholding\s*tax).*?rate\s*of\s*(\d{1,2})\s*%.*?amounting\s*to\s*฿?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE | re.DOTALL,
)
RE_WHT_GENERIC = re.compile(
    r"(withheld|withholding|wht|ภาษี\s*ณ\s*ที่\s*จ่าย).*?(\d{1,2})\s*%.*?(?:฿|THB)?\s*([0-9,]+(?:\.[0-9]{1,2})?)",
    re.IGNORECASE | re.DOTALL,
)

# Group hint
RE_ADS_HINT = re.compile(r"\b(ads|advertising|promotion|โฆษณา|ค่าโฆษณา)\b", re.IGNORECASE)

RE_ALL_WS = re.compile(r"\s+")


# TikTok vendor tax-id aliasing (ปรับได้ตาม vendor_mapping.py ของคุณ)
TIKTOK_VENDOR_TAX_ALIASES: Dict[str, str] = {
    "0105566214176": "0105566214176",  # TikTok Shop (Thailand) Ltd.
}


def _clean_digits(s: Any, max_len: int | None = None) -> str:
    if s is None:
        return ""
    out = "".join([c for c in str(s) if c.isdigit()])
    if max_len is not None:
        out = out[:max_len]
    return out


def _compact_no_ws(v: Any) -> str:
    s = "" if v is None else str(v)
    s = s.strip()
    if not s:
        return ""
    return RE_ALL_WS.sub("", s)


def _money_to_str(v: str) -> str:
    if not v:
        return ""
    s = v.strip().replace(",", "").replace("฿", "").replace("THB", "").strip()
    try:
        x = float(s)
        if x < 0:
            return ""
        return f"{x:.2f}"
    except Exception:
        return ""


def _to_yyyymmdd_from_text(s: str) -> str:
    if not s:
        return ""

    m = RE_DATE_YMD.search(s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31 and 1900 <= y <= 2100:
            return f"{y:04d}{mo:02d}{d:02d}"

    m2 = RE_DATE_MON_DD_YYYY.search(s)
    if m2:
        mon_map = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
        }
        mon = mon_map.get(m2.group(1).lower(), 0)
        d = int(m2.group(2))
        y = int(m2.group(3))
        if 1 <= mon <= 12 and 1 <= d <= 31 and 1900 <= y <= 2100:
            return f"{y:04d}{mon:02d}{d:02d}"

    return ""


def _find_amount_near_keyword(text: str, keyword_re: re.Pattern, window: int = 280) -> str:
    """
    Find the last numeric amount near a keyword block.
    """
    if not text:
        return ""
    m = keyword_re.search(text)
    if not m:
        return ""

    start = max(0, m.start() - 120)
    end = min(len(text), m.end() + window)
    chunk = text[start:end]

    nums = RE_MONEY.findall(chunk)
    if not nums:
        return ""
    return _money_to_str(nums[-1])


def _extract_wht_from_text_best_effort(text: str) -> Tuple[str, str]:
    """
    Returns (rate_percent_str, amount_str) e.g. ("3", "4414.88")
    """
    if not text:
        return ("", "")

    m = RE_WHT_AMOUNTING.search(text)
    if m:
        rate = _clean_digits(m.group(2), 2)
        amt = _money_to_str(m.group(3))
        if rate and amt:
            return (rate, amt)

    m2 = RE_WHT_GENERIC.search(text)
    if m2:
        rate = _clean_digits(m2.group(2), 2)
        amt = _money_to_str(m2.group(3))
        if rate and amt:
            return (rate, amt)

    return ("", "")


def _alias_vendor_tax_id(vendor_tax_id: str) -> str:
    v = _clean_digits(vendor_tax_id, 13)
    if not v:
        return ""
    return TIKTOK_VENDOR_TAX_ALIASES.get(v, v)


def _blank_row() -> Dict[str, Any]:
    return {
        "B_doc_date": "",
        "C_reference": "",
        "D_vendor_code": "Unknown",
        "E_tax_id_13": "",
        "F_branch_5": "00000",
        "G_invoice_no": "",
        "H_invoice_date": "",
        "I_tax_purchase_date": "",
        "J_price_type": "1",
        "K_account": "",
        "L_description": "",
        "M_qty": "1",
        "N_unit_price": "0",
        "O_vat_rate": "7%",
        "P_wht": "",          # ✅ TikTok: usually has WHT 3% (we will fill if found)
        "Q_payment_method": "",
        "R_paid_amount": "0", # IMPORTANT: set to GROSS incl VAT; finalize_row will net-of-wht if WHT exists
        "S_pnd": "",
        "T_note": "",
        "U_group": "Marketplace Expense",
    }


def _extract_invoice_no(t: str) -> str:
    if not t:
        return ""
    m = RE_TIKTOK_INVOICE_NO.search(t)
    if m:
        return _compact_no_ws(m.group(0))

    m2 = RE_INVOICE_NUMBER_LINE.search(t)
    if m2:
        return _compact_no_ws(m2.group(2))

    return ""


def _extract_vendor_tax_id(t: str, *, client_tax_id: str = "") -> str:
    vendor_tax = ""
    m_vendor = RE_VENDOR_TAX_LINE.search(t)
    if m_vendor:
        vendor_tax = _clean_digits(m_vendor.group(2), 13)

    ctax = _clean_digits(client_tax_id, 13) if client_tax_id else ""
    if not vendor_tax:
        # pick first 13-digit that is not client tax id
        all_tax = RE_TAX_ID_13_ANY.findall(t)
        for x in all_tax:
            x13 = _clean_digits(x, 13)
            if ctax and x13 == ctax:
                continue
            if x13:
                vendor_tax = x13
                break

    return _alias_vendor_tax_id(vendor_tax)


def _extract_doc_date(t: str) -> str:
    # Prefer "Invoice date :" line
    m = RE_INVOICE_DATE_LINE.search(t)
    if m:
        d = _to_yyyymmdd_from_text(m.group(2))
        if d:
            return d

    # Fallback: first date match in whole text
    d = _to_yyyymmdd_from_text(t)
    return d


def _extract_amounts_summary(t: str) -> Tuple[str, str, str]:
    """
    Returns (subtotal_excl_vat, vat_amount, total_incl_vat) as strings with 2 decimals (or "").
    """
    subtotal_ex = _find_amount_near_keyword(t, RE_SUBTOTAL_EXCL)
    vat_amt = _find_amount_near_keyword(t, RE_TOTAL_VAT)
    total_incl = _find_amount_near_keyword(t, RE_TOTAL_INCL)

    # If total incl missing but subtotal+vat present -> derive
    if (not total_incl) and subtotal_ex and vat_amt:
        try:
            total_incl = f"{(float(subtotal_ex) + float(vat_amt)):.2f}"
        except Exception:
            pass

    # If still missing total incl but subtotal exists -> as last resort (better than 0)
    if not total_incl and subtotal_ex:
        total_incl = subtotal_ex

    return (subtotal_ex, vat_amt, total_incl)


def extract_tiktok(
    text: str,
    filename: str = "",
    client_tax_id: str = "",
    cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    TikTok Tax Invoice / Receipt extractor (smart + compatible with finalize_row)

    - Extract invoice no, invoice date, vendor tax id, totals (incl VAT), WHT
    - Set R_paid_amount = GROSS incl VAT (IMPORTANT)
    - Set P_wht if found (TikTok commonly 3%)
    - finalize_row() will:
        * lock fields
        * build description pattern
        * map Q_payment_method wallet code
        * apply WHT policy => net = gross - wht
    """
    cfg = cfg or {}
    t = normalize_text(text or "")
    row = _blank_row()

    if not t.strip():
        return finalize_row(
            row,
            platform="TIKTOK",
            text=t,
            filename=filename,
            client_tax_id=client_tax_id,
            cfg=cfg,
        )

    # --- Invoice number / reference ---
    inv_no = _extract_invoice_no(t)
    if inv_no:
        row["C_reference"] = inv_no
        row["G_invoice_no"] = inv_no

    # --- Vendor tax id (TikTok Shop TH) ---
    vendor_tax = _extract_vendor_tax_id(t, client_tax_id=client_tax_id)
    row["E_tax_id_13"] = vendor_tax

    # --- Branch (normally 00000 in these invoices) ---
    m_br = RE_BRANCH_5.search(t)
    if m_br:
        br = _clean_digits(m_br.group(2), 5)
        row["F_branch_5"] = br.zfill(5) if br else "00000"
    else:
        row["F_branch_5"] = "00000"

    # --- Dates ---
    doc_date = _extract_doc_date(t)
    if doc_date:
        row["H_invoice_date"] = doc_date
        row["B_doc_date"] = doc_date
        row["I_tax_purchase_date"] = doc_date

    # --- Amounts ---
    subtotal_ex, vat_amt, total_incl = _extract_amounts_summary(t)

    # IMPORTANT:
    # - Put gross incl VAT in R_paid_amount so finalize_row can subtract WHT correctly
    if total_incl:
        row["N_unit_price"] = total_incl
        row["R_paid_amount"] = total_incl

    row["J_price_type"] = "1"
    row["O_vat_rate"] = "7%"

    # --- WHT (TikTok often 3% with explicit amount) ---
    rate_str, wht_amt = _extract_wht_from_text_best_effort(t)
    if wht_amt:
        row["P_wht"] = wht_amt
        # S_pnd let finalize decide if empty (it will set 53 when WHT exists)

    # --- Vendor Code (Cxxxxx) ---
    if callable(get_vendor_code):
        try:
            code = get_vendor_code(
                client_tax_id,
                vendor_tax_id=vendor_tax,
                vendor_name="TikTok Shop (Thailand) Ltd.",
            )
        except Exception:
            code = ""
        if isinstance(code, str) and re.match(r"^C\d{5}$", code.strip(), re.IGNORECASE):
            row["D_vendor_code"] = code.strip().upper()
        else:
            row["D_vendor_code"] = "Unknown"
    else:
        row["D_vendor_code"] = "Unknown"

    # --- Group hint ---
    row["U_group"] = "Advertising Expense" if RE_ADS_HINT.search(t) else "Marketplace Expense"

    # Let finalize_row build the structured description template
    row["L_description"] = ""
    row["T_note"] = ""

    # Compact refs
    row["C_reference"] = _compact_no_ws(row.get("C_reference"))
    row["G_invoice_no"] = _compact_no_ws(row.get("G_invoice_no"))
    if not row["C_reference"] and row["G_invoice_no"]:
        row["C_reference"] = row["G_invoice_no"]
    if not row["G_invoice_no"] and row["C_reference"]:
        row["G_invoice_no"] = row["C_reference"]

    return finalize_row(
        row,
        platform="TIKTOK",
        text=t,
        filename=filename,
        client_tax_id=client_tax_id,
        cfg=cfg,
    )

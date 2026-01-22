from __future__ import annotations

from typing import Dict, Any, Tuple, List, Callable, Optional
import os
import logging
import inspect
import re
from datetime import datetime

from .classifier import classify_platform
from ..extractors.wallet_mapping import resolve_wallet_code
from ..extractors.generic import extract_generic
from ..extractors.shopee import extract_shopee
from ..extractors.lazada import extract_lazada
from ..extractors.tiktok import extract_tiktok

# ✅ platform normalization (must match job_worker/platform_constants)
from .platform_constants import normalize_platform as _norm_platform

# ✅ Meta/Google Ads extractors (rule-based)
try:
    from ..extractors.ads_meta import extract_meta_ads
    _META_EXTRACTOR_OK = True
except Exception:  # pragma: no cover
    extract_meta_ads = None  # type: ignore
    _META_EXTRACTOR_OK = False

try:
    from ..extractors.ads_google import extract_google_ads
    _GOOGLE_EXTRACTOR_OK = True
except Exception:  # pragma: no cover
    extract_google_ads = None  # type: ignore
    _GOOGLE_EXTRACTOR_OK = False

# ✅ SPX extractor (optional)
try:
    from ..extractors.spx import extract_spx  # type: ignore
    _SPX_EXTRACTOR_OK = True
except Exception:  # pragma: no cover
    extract_spx = None  # type: ignore
    _SPX_EXTRACTOR_OK = False

# ✅ Vendor code mapping (Cxxxxx)
try:
    from ..extractors.vendor_mapping import get_vendor_code, detect_client_from_context
    _VENDOR_MAPPING_OK = True
except Exception:  # pragma: no cover
    get_vendor_code = None  # type: ignore
    detect_client_from_context = None  # type: ignore
    _VENDOR_MAPPING_OK = False

# ✅ Wallet / Payment mapping (EWLxxx) (optional)
try:
    from ..extractors.wallet_mapping import resolve_wallet_code  # type: ignore
    _WALLET_MAPPING_OK = True
except Exception:  # pragma: no cover
    resolve_wallet_code = None  # type: ignore
    _WALLET_MAPPING_OK = False

from ..utils.validators import (
    validate_yyyymmdd,
    validate_branch5,
    validate_tax13,
    validate_price_type,
    validate_vat_rate,
)

# ✅ AI extractor (optional)
try:
    from .ai_service import ai_fill_peak_row as extract_with_ai  # platform-aware JSON patch
    _AI_OK = True
except Exception:  # pragma: no cover
    try:
        from .ai_extract_service import extract_with_ai  # type: ignore
        _AI_OK = True
    except Exception:
        extract_with_ai = None  # type: ignore
        _AI_OK = False


logger = logging.getLogger(__name__)

# ============================================================
# Platform groups + defaults (กลาง)
# ============================================================

PLATFORM_GROUPS = {
    "META": "Advertising Expense",
    "GOOGLE": "Advertising Expense",
    "SHOPEE": "Marketplace Expense",
    "LAZADA": "Marketplace Expense",
    "TIKTOK": "Marketplace Expense",
    "SPX": "Marketplace Expense",  # โลจิสติกส์
    "THAI_TAX": "General Expense",
    "UNKNOWN": "Other Expense",
    "GENERIC": "Other Expense",
}

PLATFORM_DESCRIPTIONS = {
    "META": "Meta Ads",
    "GOOGLE": "Google Ads",
    "SHOPEE": "Shopee Marketplace Fee",
    "LAZADA": "Lazada Marketplace Fee",
    "TIKTOK": "TikTok Shop Fee",
    "SPX": "Shopee Express",
    "THAI_TAX": "Tax Invoice",
    "UNKNOWN": "",
    "GENERIC": "",
}

# ============================================================
# Client constants (ตามที่คุณใช้ใน UI)
# ============================================================

CLIENT_RABBIT = "0105561071873"
CLIENT_SHD = "0105563022918"
CLIENT_TOPONE = "0105565027615"

DEFAULT_COMPANY_NAME_BY_TAX = {
    CLIENT_RABBIT: "RABBIT",
    CLIENT_SHD: "SHD",
    CLIENT_TOPONE: "TOPONE",
}

# tag -> tax id (ใช้ตอน cfg มีหลายบริษัท)
CLIENT_TAX_BY_TAG = {
    "RABBIT": CLIENT_RABBIT,
    "SHD": CLIENT_SHD,
    "TOPONE": CLIENT_TOPONE,
    # HASHTAG: ไม่ทราบเลขภาษี -> ปล่อยว่าง
}

# ============================================================
# PEAK A–U schema lock
# ============================================================

PEAK_KEYS_ORDER: List[str] = [
    "A_seq",
    "A_company_name",
    "B_doc_date",
    "C_reference",
    "D_vendor_code",
    "E_tax_id_13",
    "F_branch_5",
    "G_invoice_no",
    "H_invoice_date",
    "I_tax_purchase_date",
    "J_price_type",
    "K_account",
    "L_description",
    "M_qty",
    "N_unit_price",
    "O_vat_rate",
    "P_wht",
    "Q_payment_method",
    "R_paid_amount",
    "S_pnd",
    "T_note",
    "U_group",
]

# keys ที่ “ห้าม AI ไปย้ายคอลัมน์/ทำเลื่อน”
_AI_BLACKLIST_KEYS = {"T_note", "U_group", "K_account"}
_INTERNAL_OK_PREFIXES = ("_",)

_RE_ALL_WS = re.compile(r"\s+")

# ============================================================
# Reference normalizer (ตัด Shopee-TIV- ให้เหลือ TRS... / SPX ให้เหลือ RCS...)
# ============================================================

RE_TRS_CORE = re.compile(r"(TRS[A-Z0-9\-_/.]{10,})", re.IGNORECASE)
RE_RCS_CORE = re.compile(r"(RCS[A-Z0-9\-_/.]{10,})", re.IGNORECASE)
RE_TTSTH_CORE = re.compile(r"(TTSTH\d{8,})", re.IGNORECASE)

# Lazada invoice no มักเป็น THMPTI...
RE_LAZ_INVOICE = re.compile(r"\b(THMPTI\d{10,})\b", re.IGNORECASE)

# Generic: 32-hex md5-like (ไฟล์ hash)
RE_HASH32 = re.compile(r"^[a-f0-9]{32}$", re.IGNORECASE)

RE_LEADING_NOISE_PREFIX = re.compile(
    r"^(?:Shopee-)?TI[VR]-|^Shopee-|^TIV-|^TIR-|^SPX-|^LAZ-|^LZD-|^TikTok-",
    re.IGNORECASE,
)


def _strip_ext(s: str) -> str:
    return re.sub(r"\.(pdf|png|jpg|jpeg|xlsx|xls)$", "", s, flags=re.IGNORECASE).strip()


# ============================================================
# helpers: sanitize / merge / compact
# ============================================================

def _sanitize_incoming_row(d: Any) -> Dict[str, Any]:
    return d if isinstance(d, dict) else {}


def _compact_no_ws(v: Any) -> str:
    s = "" if v is None else str(v)
    s = s.strip()
    if not s:
        return ""
    return _RE_ALL_WS.sub("", s)


def _normalize_reference_core(value: Any) -> str:
    """
    Normalize reference/invoice ให้เป็นแกนเอกสารที่ถูกต้อง
    Example:
      "Shopee-TIV-TRSPEMKP00-00000-251203-0012589.pdf" -> "TRSPEMKP00-00000-251203-0012589"
      "SPX Express-RCT-RCSPXSPR00-00000-251205-0000625.pdf" -> "RCSPXSPR00-00000-251205-0000625"
    """
    s = _compact_no_ws(value)
    if not s:
        return ""
    s = _strip_ext(s)

    # ดึง core ถ้ามี
    for pat in (RE_TRS_CORE, RE_RCS_CORE, RE_TTSTH_CORE, RE_LAZ_INVOICE):
        m = pat.search(s)
        if m:
            return _compact_no_ws(m.group(1))

    # ตัด prefix noise
    s2 = RE_LEADING_NOISE_PREFIX.sub("", s).strip()
    s2 = _strip_ext(s2)
    return _compact_no_ws(s2) if s2 else _compact_no_ws(s)


def _try_get_source_filename(filename: str, row: Dict[str, Any]) -> str:
    """
    ใช้ filename ที่ส่งเข้ามาเป็นหลัก ถ้าไม่มีค่อยดูใน meta ของ row
    """
    if filename:
        try:
            return os.path.basename(str(filename))
        except Exception:
            return str(filename)

    for k in ("_filename", "filename", "source_file", "_source_file", "_file", "file"):
        v = row.get(k)
        if v:
            try:
                return os.path.basename(str(v))
            except Exception:
                return str(v)
    return ""


def _sanitize_ai_row(ai: Dict[str, Any]) -> Dict[str, Any]:
    if not ai:
        return {}

    cleaned: Dict[str, Any] = {}
    for k, v in ai.items():
        if not k:
            continue
        if k in _AI_BLACKLIST_KEYS:
            continue
        if v in ("", None):
            continue

        if k in PEAK_KEYS_ORDER:
            cleaned[k] = v
            continue
        if isinstance(k, str) and k.startswith(_INTERNAL_OK_PREFIXES):
            cleaned[k] = v
            continue

    return cleaned


def _merge_rows(base: Dict[str, Any], patch: Dict[str, Any], *, fill_missing: bool = True) -> Dict[str, Any]:
    if not patch:
        return base

    out = dict(base)
    for k, v in patch.items():
        if not k or k in _AI_BLACKLIST_KEYS:
            continue
        if v in ("", None):
            continue

        if fill_missing:
            cur = out.get(k, "")
            if cur in ("", None, "0", "0.00"):
                out[k] = v
        else:
            out[k] = v
    return out


# ============================================================
# helpers: date parsing (จาก text เท่านั้น)  ✅ HARD LOCK: ห้ามเดาจากชื่อไฟล์
# ============================================================

RE_DATE_ISO = re.compile(r"\b(20\d{2})[-/](\d{2})[-/](\d{2})\b")
RE_DATE_THAI_LONG = re.compile(
    r"(?:Invoice\s*Date|วันที่(?:ใบกำกับ|เอกสาร|ออกเอกสาร)|Date)\s*[:：]?\s*(20\d{2})[-/](\d{2})[-/](\d{2})",
    re.IGNORECASE,
)

def _yyyymmdd_from_parts(y: str, m: str, d: str) -> str:
    try:
        yy = int(y)
        mm = int(m)
        dd = int(d)
        if yy < 2000 or yy > 2099:
            return ""
        if mm < 1 or mm > 12:
            return ""
        if dd < 1 or dd > 31:
            return ""
        return f"{yy:04d}{mm:02d}{dd:02d}"
    except Exception:
        return ""

def _extract_doc_date_from_text(text: str) -> str:
    """
    พยายามหา "Invoice Date: YYYY-MM-DD" หรือ "วันที่...: YYYY-MM-DD" จากเนื้อเอกสาร
    """
    if not text:
        return ""
    m = RE_DATE_THAI_LONG.search(text)
    if m:
        return _yyyymmdd_from_parts(m.group(1), m.group(2), m.group(3))

    # fallback: first ISO date in doc (ระวัง false positive แต่ยังดีกว่าเดาจากชื่อไฟล์)
    m2 = RE_DATE_ISO.search(text)
    if m2:
        return _yyyymmdd_from_parts(m2.group(1), m2.group(2), m2.group(3))
    return ""


# ============================================================
# helpers: invoice/reference extraction from TEXT (กันกรณี Lazada filename เป็น hash)
# ============================================================

RE_INVNO_BLOCK = re.compile(
    r"(?:Invoice\s*No\.?|Tax\s*Invoice\s*/\s*Receipt|Receipt\s*No\.?)\s*[:：]?\s*([A-Z0-9\-_/.]{8,})",
    re.IGNORECASE,
)

def _extract_reference_candidates_from_text(text: str) -> List[str]:
    if not text:
        return []
    out: List[str] = []

    # Lazada explicit
    for m in RE_LAZ_INVOICE.finditer(text):
        out.append(_normalize_reference_core(m.group(1)))

    # Generic invoice no field
    for m in RE_INVNO_BLOCK.finditer(text):
        out.append(_normalize_reference_core(m.group(1)))

    # TRS/RCS/TTSTH in text
    for pat in (RE_TRS_CORE, RE_RCS_CORE, RE_TTSTH_CORE):
        for m in pat.finditer(text):
            out.append(_normalize_reference_core(m.group(1)))

    # dedup preserve order
    seen = set()
    uniq: List[str] = []
    for x in out:
        if x and x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def _is_probably_hash(s: str) -> bool:
    s2 = (s or "").strip()
    return bool(s2) and bool(RE_HASH32.match(s2))


def _score_reference(platform: str, ref: str) -> int:
    """
    score สูง = น่าเชื่อถือกว่า
    """
    p = (platform or "").upper().strip()
    r = (ref or "").strip()
    if not r:
        return 0

    # หลีกเลี่ยง hash/uuid
    if _is_probably_hash(r):
        return 5

    # platform-specific cores
    if RE_TRS_CORE.match(r):
        return 100
    if RE_RCS_CORE.match(r):
        return 95
    if RE_TTSTH_CORE.match(r):
        return 85
    if RE_LAZ_INVOICE.match(r):
        return 90

    # Lazada: ถ้าเป็น Marketplace ให้ favor invoice-like token
    if p == "LAZADA":
        # Lazada invoice มักยาวและเริ่มด้วย TH...
        if r.upper().startswith("TH") and len(r) >= 12:
            return 80

    # general: alnum long
    if len(r) >= 10 and re.fullmatch(r"[A-Z0-9\-_/.]+", r, flags=re.IGNORECASE):
        return 60

    return 30


def _pick_best_reference(
    *,
    platform: str,
    src_file: str,
    row: Dict[str, Any],
    text: str,
) -> str:
    """
    ✅ FIX: ห้าม prefer filename ถ้าเอกสารมี invoice no จริง
    order ความน่าเชื่อถือ:
      1) extractor-provided invoice/reference ที่ "ดูมีรูปแบบ"
      2) invoice/reference ที่ parse ได้จาก TEXT
      3) filename core (เฉพาะเมื่อไม่ใช่ hash หรือเป็น TRS/RCS/etc)
    """
    p = (platform or "").upper().strip()

    cands: List[str] = []

    # 1) from row fields
    r_c = _normalize_reference_core(row.get("C_reference", ""))
    r_g = _normalize_reference_core(row.get("G_invoice_no", ""))
    if r_g:
        cands.append(r_g)
    if r_c:
        cands.append(r_c)

    # 2) from text
    cands.extend(_extract_reference_candidates_from_text(text))

    # 3) from filename (LAST)
    ref_from_file = _normalize_reference_core(src_file) if src_file else ""
    if ref_from_file:
        cands.append(ref_from_file)

    # dedup preserve order
    seen = set()
    uniq: List[str] = []
    for x in cands:
        x2 = (x or "").strip()
        if not x2:
            continue
        if x2 not in seen:
            seen.add(x2)
            uniq.append(x2)

    if not uniq:
        return ""

    # choose best by score; tie -> earlier in list wins
    best = uniq[0]
    best_score = _score_reference(p, best)
    for ref in uniq[1:]:
        sc = _score_reference(p, ref)
        if sc > best_score:
            best = ref
            best_score = sc

    # special: if best is hash but there exists non-hash -> take non-hash
    if _is_probably_hash(best):
        for ref in uniq:
            if ref and not _is_probably_hash(ref):
                best = ref
                break

    return _compact_no_ws(best)


# ============================================================
# helpers: validation
# ============================================================

def _validate_row(row: Dict[str, Any]) -> List[str]:
    errors: List[str] = []

    if not validate_yyyymmdd(row.get("B_doc_date", "")):
        errors.append("วันที่เอกสารรูปแบบไม่ถูกต้อง")

    if row.get("H_invoice_date") and not validate_yyyymmdd(row.get("H_invoice_date", "")):
        errors.append("วันที่ใบกำกับฯรูปแบบไม่ถูกต้อง")

    if row.get("I_tax_purchase_date") and not validate_yyyymmdd(row.get("I_tax_purchase_date", "")):
        errors.append("วันที่ภาษีซื้อรูปแบบไม่ถูกต้อง")

    if row.get("F_branch_5") and not validate_branch5(row.get("F_branch_5", "")):
        errors.append("เลขสาขาไม่ใช่ 5 หลัก")

    if row.get("E_tax_id_13") and not validate_tax13(row.get("E_tax_id_13", "")):
        errors.append("เลขภาษีไม่ใช่ 13 หลัก")

    if row.get("J_price_type") and not validate_price_type(row.get("J_price_type", "")):
        errors.append("ประเภทราคาไม่ถูกต้อง")

    if row.get("O_vat_rate") and not validate_vat_rate(row.get("O_vat_rate", "")):
        errors.append("อัตราภาษีไม่ถูกต้อง")

    return errors


# ============================================================
# ✅ client tax resolve: support client_tax_id + client_tax_ids(list) + client_tags
# ============================================================

def _as_list(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        out: List[str] = []
        for x in v:
            s = str(x).strip()
            if s:
                out.append(s)
        return out
    s = str(v).strip()
    if not s:
        return []
    # try JSON list string
    if (s.startswith("[") and s.endswith("]")) or (s.startswith('"') and s.endswith('"')):
        try:
            j = __import__("json").loads(s)
            if isinstance(j, list):
                return [str(x).strip() for x in j if str(x).strip()]
            if isinstance(j, str) and j.strip():
                return [j.strip()]
        except Exception:
            pass
    # comma separated
    if "," in s:
        return [x.strip() for x in s.split(",") if x.strip()]
    return [s]


def _resolve_client_tax_id_from_cfg(cfg: Dict[str, Any], *, filename: str = "", text: str = "") -> str:
    """
    รองรับ:
      - cfg["client_tax_id"] (string)
      - cfg["client_tax_ids"] (list/str)
      - cfg["client_tags"] (list/str) -> map เป็น tax id (RABBIT/SHD/TOPONE)
    """
    cfg = cfg or {}

    # 1) explicit single
    c1 = str(cfg.get("client_tax_id") or "").strip()
    if c1:
        return c1

    # 2) list
    ids = _as_list(cfg.get("client_tax_ids"))
    if len(ids) == 1:
        return ids[0].strip()

    # 3) if multiple ids: try use client_tags -> tax
    tags = [t.upper().strip() for t in _as_list(cfg.get("client_tags"))]
    for t in tags:
        tax = CLIENT_TAX_BY_TAG.get(t)
        if tax and tax in ids:
            return tax

    # 4) if still multiple: fallback first
    if ids:
        return ids[0].strip()

    # 5) last: detect from context (optional)
    if detect_client_from_context is not None:
        try:
            c = (detect_client_from_context(text) or "").strip()
            if c:
                return c
        except Exception:
            pass

    return ""


# ============================================================
# extractor call (backward compatible) + MUST PASS filename+cfg
# ============================================================

def _safe_call_extractor(
    fn: Callable[..., Dict[str, Any]],
    text: str,
    *,
    filename: str = "",
    client_tax_id: str = "",
    cfg: Optional[Dict[str, Any]] = None,
    platform_hint: str = "",
) -> Dict[str, Any]:
    cfg = cfg or {}

    try:
        sig = inspect.signature(fn)
        params = sig.parameters
        kwargs: Dict[str, Any] = {}

        if "filename" in params:
            kwargs["filename"] = filename
        if "client_tax_id" in params and client_tax_id:
            kwargs["client_tax_id"] = client_tax_id
        if "cfg" in params:
            kwargs["cfg"] = cfg
        if "platform_hint" in params and platform_hint:
            kwargs["platform_hint"] = platform_hint

        if kwargs:
            return fn(text, **kwargs)  # type: ignore[arg-type]
    except Exception:
        pass

    if client_tax_id:
        try:
            return fn(text, client_tax_id=client_tax_id)  # type: ignore
        except TypeError:
            pass

    return fn(text)  # type: ignore


# ============================================================
# Vendor mapping: force D_vendor_code = Cxxxxx
# ============================================================

def _apply_vendor_code_mapping(row: Dict[str, Any], text: str, client_tax_id: str) -> Dict[str, Any]:
    if not isinstance(row, dict):
        return row
    if not _VENDOR_MAPPING_OK or get_vendor_code is None:
        return row

    ctax = (client_tax_id or "").strip()
    if not ctax and detect_client_from_context is not None:
        try:
            ctax = detect_client_from_context(text) or ""
        except Exception:
            ctax = ""

    if not ctax:
        return row

    vtax = str(row.get("E_tax_id_13") or "").strip()
    vname = str(row.get("D_vendor_code") or "").strip()

    try:
        code = get_vendor_code(client_tax_id=ctax, vendor_tax_id=vtax, vendor_name=vname)
    except Exception:
        return row

    if isinstance(code, str) and code.startswith("C") and len(code) >= 5:
        row["D_vendor_code"] = code
        if os.getenv("STORE_VENDOR_MAPPING_META", "1") == "1":
            row["_client_tax_id_used"] = ctax
            row["_vendor_tax_id_used"] = vtax or ""
            row["_vendor_code_resolved"] = code

    return row


# ============================================================
# Wallet mapping: normalize Q_payment_method (EWLxxx) if available
# ============================================================

def _apply_payment_method_mapping(row: Dict[str, Any], text: str) -> Dict[str, Any]:
    if not isinstance(row, dict):
        return row
    if not _WALLET_MAPPING_OK or resolve_wallet_code is None:
        return row

    # ถ้ามีอยู่แล้วและเป็น EWLxxx ให้ถือว่าถูกแล้ว
    cur = str(row.get("Q_payment_method") or "").strip()
    if cur and cur.upper().startswith("EWL"):
        return row

    # build a small context for mapping
    ctx = {
        "platform": str(row.get("_platform") or row.get("_platform_route") or "").strip(),
        "shop_id": str(row.get("shop_id") or row.get("seller_id") or row.get("merchant_id") or "").strip(),
        "shop_name": str(row.get("shop_name") or row.get("username") or row.get("seller_username") or "").strip(),
        "filename": str(row.get("_filename") or row.get("filename") or row.get("file") or "").strip(),
        "text": text or "",
    }

    try:
        code = resolve_wallet_code(ctx)  # type: ignore[arg-type]
    except Exception:
        code = ""

    if isinstance(code, str) and code.strip():
        row["Q_payment_method"] = code.strip()
        if os.getenv("STORE_WALLET_MAPPING_META", "1") == "1":
            row["_wallet_code_resolved"] = code.strip()

    return row


# ============================================================
# Platform-specific enforcement (กลาง)
# ============================================================

def _enforce_platform_rules(row: Dict[str, Any], platform: str) -> Dict[str, Any]:
    p = (platform or "").upper().strip()

    # group default
    if p in PLATFORM_GROUPS and not str(row.get("U_group") or "").strip():
        row["U_group"] = PLATFORM_GROUPS[p]

    # description default (only if extractor didn't fill)
    if not str(row.get("L_description") or "").strip():
        desc = PLATFORM_DESCRIPTIONS.get(p, "")
        if desc:
            row["L_description"] = desc

    # VAT defaults
    if p in ("META", "GOOGLE"):
        if not str(row.get("O_vat_rate") or "").strip():
            row["O_vat_rate"] = "NO"
        if not str(row.get("J_price_type") or "").strip():
            row["J_price_type"] = "3"
    elif p in ("SHOPEE", "LAZADA", "TIKTOK", "SPX"):
        if not str(row.get("O_vat_rate") or "").strip():
            row["O_vat_rate"] = "7%"
        if not str(row.get("J_price_type") or "").strip():
            row["J_price_type"] = "1"

    # Marketplace bucket
    if p in ("SHOPEE", "LAZADA", "TIKTOK", "SPX"):
        row["U_group"] = "Marketplace Expense"
        if str(row.get("K_account") or "").strip() == "Marketplace Expense":
            row["K_account"] = ""

    return row


# ============================================================
# LOCK schema A–U (กันคอลัมน์เลื่อน)
# ============================================================

def lock_peak_columns(row: Dict[str, Any]) -> Dict[str, Any]:
    safe = _sanitize_incoming_row(row)
    out: Dict[str, Any] = {}

    for k, v in safe.items():
        if isinstance(k, str) and k.startswith(_INTERNAL_OK_PREFIXES):
            out[k] = v

    for k in PEAK_KEYS_ORDER:
        out[k] = safe.get(k, "")

    return out


# ============================================================
# ✅ WHT helpers + doc parsing + policy
# ============================================================

def _to_float(v: Any) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return 0.0
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return 0.0


def _fmt_2(v: float) -> str:
    try:
        return f"{float(v):.2f}"
    except Exception:
        return "0.00"


def _parse_vat_rate(v: Any) -> float:
    """
    รับ "7%" -> 0.07, "NO" -> 0.0, 7 -> 0.07, 0.07 -> 0.07
    """
    if v is None:
        return 0.0
    s = str(v).strip().upper()
    if not s:
        return 0.0
    if s in ("NO", "NONE", "0", "0%", "EXEMPT"):
        return 0.0
    if s.endswith("%"):
        return _to_float(s[:-1]) / 100.0
    x = _to_float(s)
    if x > 1.0:
        return x / 100.0
    return x


def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on", "enable", "enabled", "✅"):
        return True
    if s in ("0", "false", "no", "n", "off", "disable", "disabled", "❌"):
        return False
    return False


# parse WHT from document text (Thai + EN)
RE_WHT_TH = re.compile(
    r"(?:หักภาษี\s*ณ\s*ที่จ่าย|ภาษีหัก\s*ณ\s*ที่จ่าย)[^\d%]{0,40}(\d{1,2}(?:\.\d+)?)\s*%[^\d]{0,40}([\d,]+\.\d{2}|\d+)",
    re.IGNORECASE,
)
RE_WHT_EN = re.compile(
    r"(?:withholding\s*tax)[^\d%]{0,40}(\d{1,2}(?:\.\d+)?)\s*%[^\d]{0,40}([\d,]+\.\d{2}|\d+)",
    re.IGNORECASE,
)

def _extract_wht_from_text(text: str) -> Tuple[float, float]:
    """
    returns (rate, amount)
    """
    if not text:
        return (0.0, 0.0)

    m = RE_WHT_TH.search(text)
    if m:
        rate = _to_float(m.group(1)) / 100.0
        amt = _to_float(m.group(2))
        return (rate, amt)

    m = RE_WHT_EN.search(text)
    if m:
        rate = _to_float(m.group(1)) / 100.0
        amt = _to_float(m.group(2))
        return (rate, amt)

    return (0.0, 0.0)


def _apply_wht_policy(row: Dict[str, Any], cfg: Dict[str, Any], *, text: str = "") -> Dict[str, Any]:
    """
    ✅ SMART + CONSISTENT (HARD LOCK compliant):
    - WHT base ต้องมาจาก Subtotal (ex VAT) เท่านั้น
    - Paid (R_paid_amount) ต้องเป็นยอดจ่ายหลังหัก ณ ที่จ่าย:
        R_paid_amount = gross_including_vat - wht_amount

    แหล่ง WHT:
      1) ถ้า extractor ให้ P_wht มาแล้ว -> ใช้ (default ไม่เขียนทับ)
      2) ถ้าไม่มี และ cfg.auto_detect_wht=1 -> parse จาก text
      3) ถ้ายังไม่มี และ cfg.calculate_wht=1 -> คำนวณ fallback: base_ex_vat * rate

    Gross policy:
      - gross คือ "ยอดรวม VAT" (Total including VAT)
      - จะพยายามหา gross จาก:
          a) R_paid_amount (ถ้า extractor ใส่เป็น total incl vat)
          b) fallback: parse total incl vat จาก text (ถ้ามี helper)
          c) fallback: ถ้ามี subtotal + vat -> gross = subtotal + vat
          d) fallback สุดท้าย: N_unit_price (บาง extractor ใส่ total ไว้)

    cfg parameters:
      - calculate_wht / wht_enabled: True/False
      - auto_detect_wht: default True
      - wht_rate: default 0.03
      - pnd_when_wht: default "53"
      - pnd_when_no_wht: default "53"
      - wht_override_existing: default False
    """
    cfg = cfg or {}
    enabled_calc = _truthy(cfg.get("calculate_wht", cfg.get("wht_enabled")))
    auto_detect = _truthy(cfg.get("auto_detect_wht", "1"))
    try:
        rate_f = float(cfg.get("wht_rate", 0.03))
    except Exception:
        rate_f = 0.03

    pnd_when_wht = str(cfg.get("pnd_when_wht", "53")).strip() or "53"
    pnd_when_no = str(cfg.get("pnd_when_no_wht", "53")).strip() or "53"
    override_existing = _truthy(cfg.get("wht_override_existing", "0"))

    text = text or ""
    vat = _parse_vat_rate(row.get("O_vat_rate"))

    # -------------------------
    # 0) current WHT
    # -------------------------
    cur_wht_s = str(row.get("P_wht") or "").strip()
    cur_wht = _to_float(cur_wht_s)

    # -------------------------
    # 1) Resolve gross (incl VAT)
    # -------------------------
    gross = _to_float(row.get("R_paid_amount"))  # many extractors put total here
    if gross <= 0:
        gross = _to_float(row.get("N_unit_price"))  # last fallback

    # Optional: if you have text parsers, use them (safe)
    # These helpers are assumed to exist in your codebase; if not, remove this block.
    subtotal_ex = 0.0
    vat_amt = 0.0
    try:
        # If you have a function to extract subtotal/vat/total (recommended)
        # return (subtotal_ex_vat, vat_amount, total_including_vat)
        sx, vx, tx = _extract_amounts_summary_from_text(text)  # type: ignore[name-defined]
        subtotal_ex = _to_float(sx)
        vat_amt = _to_float(vx)
        total_incl = _to_float(tx)
        if gross <= 0 and total_incl > 0:
            gross = total_incl
    except Exception:
        # no parser available → ignore safely
        pass

    # If still no gross but we have subtotal+vat, compute it
    if gross <= 0 and subtotal_ex > 0 and vat_amt > 0:
        gross = subtotal_ex + vat_amt

    # If still none, cannot compute net reliably
    # (but we can still keep detected WHT)
    # -------------------------
    # 2) Detect WHT from text (if missing or override)
    # -------------------------
    if auto_detect and (override_existing or cur_wht <= 0.0):
        detected_rate, detected_amt = _extract_wht_from_text(text)
        if detected_amt > 0:
            row["P_wht"] = _fmt_2(round(detected_amt + 1e-9, 2))
            cur_wht = detected_amt
            if os.getenv("STORE_WHT_META", "1") == "1":
                row["_wht_detected_rate"] = f"{float(detected_rate):.4f}"
                row["_wht_detected_amount"] = _fmt_2(detected_amt)

    # -------------------------
    # 3) Calc WHT fallback (base ex VAT)
    # -------------------------
    if enabled_calc and (override_existing or cur_wht <= 0.0):
        # determine base_ex_vat
        base_ex_vat = 0.0

        # prefer parsed subtotal_ex if available
        if subtotal_ex > 0:
            base_ex_vat = subtotal_ex
        elif gross > 0:
            base_ex_vat = gross / (1.0 + vat) if vat > 0 else gross

        if base_ex_vat > 0:
            wht_amount = base_ex_vat * rate_f
            if wht_amount < 0:
                wht_amount = 0.0
            wht_amount = round(wht_amount + 1e-9, 2)
            row["P_wht"] = _fmt_2(wht_amount)
            cur_wht = wht_amount
            if os.getenv("STORE_WHT_META", "1") == "1":
                row["_wht_calc_rate"] = f"{rate_f:.4f}"
                row["_wht_calc_base_ex_vat"] = _fmt_2(round(base_ex_vat + 1e-9, 2))

    # -------------------------
    # 4) Apply net paid = gross - wht (IMPORTANT)
    # -------------------------
    if cur_wht > 0:
        if gross > 0:
            if os.getenv("STORE_WHT_META", "1") == "1":
                row["_gross_amount_before_wht"] = _fmt_2(round(gross + 1e-9, 2))

            net = gross - cur_wht
            if net < 0:
                net = 0.0
            row["R_paid_amount"] = _fmt_2(round(net + 1e-9, 2))

        # set PND when WHT exists
        if not str(row.get("S_pnd") or "").strip():
            row["S_pnd"] = pnd_when_wht
        return row

    # -------------------------
    # 5) No WHT
    # -------------------------
    # If WHT not enabled and none detected -> keep empty
    if not enabled_calc and not cur_wht_s:
        row["P_wht"] = ""

    if not str(row.get("S_pnd") or "").strip():
        row["S_pnd"] = pnd_when_no

    return row


# ============================================================
# ✅ Finalize helpers: company, GL code, description structure
# ============================================================

def _resolve_client_tax_id(text: str, client_tax_id: str, cfg: Dict[str, Any]) -> str:
    ctax = (client_tax_id or "").strip()
    if ctax:
        return ctax

    ctax = _resolve_client_tax_id_from_cfg(
        cfg,
        filename=cfg.get("_filename", "") if isinstance(cfg, dict) else "",
        text=text,
    )
    return (ctax or "").strip()


def _resolve_company_name(client_tax_id: str, cfg: Dict[str, Any]) -> str:
    # cfg override
    mp = cfg.get("company_name_by_tax_id")
    if isinstance(mp, dict):
        v = mp.get(client_tax_id)
        if isinstance(v, str) and v.strip():
            return v.strip()

    # env override (optional)
    if client_tax_id == CLIENT_RABBIT and os.getenv("COMPANY_NAME_RABBIT"):
        return os.getenv("COMPANY_NAME_RABBIT", "").strip()
    if client_tax_id == CLIENT_SHD and os.getenv("COMPANY_NAME_SHD"):
        return os.getenv("COMPANY_NAME_SHD", "").strip()
    if client_tax_id == CLIENT_TOPONE and os.getenv("COMPANY_NAME_TOPONE"):
        return os.getenv("COMPANY_NAME_TOPONE", "").strip()

    return DEFAULT_COMPANY_NAME_BY_TAX.get(client_tax_id, "")


def _resolve_gl_code(client_tax_id: str, platform: str, row: Dict[str, Any], cfg: Dict[str, Any]) -> str:
    """
    เติม K_account ให้ครบ:
    - cfg["gl_code_map"] รองรับ:
        1) {"0105...": "520317"}
        2) {"0105...": {"MARKETPLACE":"520317","ADS":"520201","DEFAULT":"520203"}}
    - หรือ env: GL_CODE_RABBIT/SHD/TOPONE
    - ถ้าไม่มีจริงๆ fallback เป็น U_group
    """
    # 1) cfg map
    mp = cfg.get("gl_code_map")
    if isinstance(mp, dict):
        v = mp.get(client_tax_id)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, dict):
            p = (platform or "").upper()
            bucket = "ADS" if p in ("META", "GOOGLE") else "MARKETPLACE" if p in ("SHOPEE", "LAZADA", "TIKTOK", "SPX") else "DEFAULT"
            vv = v.get(bucket) or v.get("DEFAULT") or ""
            if isinstance(vv, str) and vv.strip():
                return vv.strip()

    # 2) env
    if client_tax_id == CLIENT_RABBIT and os.getenv("GL_CODE_RABBIT"):
        return os.getenv("GL_CODE_RABBIT", "").strip()
    if client_tax_id == CLIENT_SHD and os.getenv("GL_CODE_SHD"):
        return os.getenv("GL_CODE_SHD", "").strip()
    if client_tax_id == CLIENT_TOPONE and os.getenv("GL_CODE_TOPONE"):
        return os.getenv("GL_CODE_TOPONE", "").strip()

    # 3) fallback: if extractor already filled
    cur = str(row.get("K_account") or "").strip()
    if cur:
        return cur

    # 4) last fallback: use group
    grp = str(row.get("U_group") or "").strip()
    return grp


def _guess_seller_id(row: Dict[str, Any], text: str) -> str:
    for k in ("seller_id", "sellerId", "shop_id", "shopid", "shopId", "merchant_id", "merchantId"):
        v = row.get(k)
        if v:
            s = str(v).strip()
            if s:
                return s
    m = re.search(r"(?:seller\s*id|shop\s*id)\s*[:#]?\s*([0-9]{4,})", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""


def _guess_username(row: Dict[str, Any], text: str) -> str:
    for k in ("username", "user_name", "seller_username", "shop_name", "shopName", "sellerName"):
        v = row.get(k)
        if v:
            s = str(v).strip()
            if s:
                return s
    m = re.search(r"(?:username|user\s*name|shop\s*name)\s*[:#]?\s*([A-Za-z0-9_.\-]{3,})", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""


def _build_description_structure(
    base_desc: str,
    platform: str,
    seller_id: str,
    username: str,
    src_file: str,
) -> str:
    parts: List[str] = []
    bd = (base_desc or "").strip()
    if not bd:
        bd = PLATFORM_DESCRIPTIONS.get((platform or "").upper(), "") or ""
    if bd:
        parts.append(bd)

    tags: List[str] = []
    if seller_id:
        tags.append(f"SellerID={seller_id}")
    if username:
        tags.append(f"Username={username}")
    if src_file:
        tags.append(f"File={src_file}")

    if tags:
        parts.append(" | ".join(tags))

    return " — ".join([p for p in parts if p.strip()]).strip()


# ============================================================
# ✅ FINALIZE (THE IMPORTANT PART) — SMART & DETERMINISTIC
# ============================================================

def finalize_row(
    row: Dict[str, Any],
    *,
    platform: str,
    text: str,
    filename: str,
    client_tax_id: str,
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    row = _sanitize_incoming_row(row)
    p = (platform or "UNKNOWN").upper().strip()
    cfg = cfg or {}
    text = text or ""

    # policy: T_note must be empty
    row["T_note"] = ""

    # ✅ Ensure doc date from TEXT if missing (HARD LOCK: not from filename)
    if not str(row.get("B_doc_date") or "").strip():
        dd = _extract_doc_date_from_text(text)
        if dd:
            row["B_doc_date"] = dd

    # resolve client tax id + company
    ctax = _resolve_client_tax_id(text, client_tax_id, cfg)
    if ctax and not str(row.get("A_company_name") or "").strip():
        row["A_company_name"] = _resolve_company_name(ctax, cfg)

    # enforce platform rules (group/desc/vat defaults)
    row = _enforce_platform_rules(row, p)

    # ✅ normalize references (SMART: doc > text > filename; avoid hash)
    src_file = _try_get_source_filename(filename, row)
    best_ref = _pick_best_reference(platform=p, src_file=src_file, row=row, text=text)
    row["C_reference"] = best_ref
    row["G_invoice_no"] = best_ref

    # compact no ws
    row["C_reference"] = _compact_no_ws(row.get("C_reference", ""))
    row["G_invoice_no"] = _compact_no_ws(row.get("G_invoice_no", ""))

    # ✅ description structure + seller id/username/file
    seller_id = _guess_seller_id(row, text)
    username = _guess_username(row, text)

    base_desc = str(row.get("L_description") or "").strip()
    row["L_description"] = _build_description_structure(
        base_desc=base_desc,
        platform=p,
        seller_id=seller_id,
        username=username,
        src_file=src_file,
    )

    # ✅ (A) Q_payment_method mapping (EWLxxx) — use wallet mapping
    # - client tax id must be OUR company tax id (ctax)
    # - seller_id: digits (Shopee seller/shop id)
    # - shop_name: use username first (often store name), fallback src_file, or row fields
    if not str(row.get("Q_payment_method") or "").strip():
        shop_name = (
            str(row.get("shop_name") or "").strip()
            or str(row.get("seller_name") or "").strip()
            or str(row.get("username") or "").strip()
            or (username or "")
            or (src_file or "")
        )
        try:
            wallet = resolve_wallet_code(
                ctax or client_tax_id,
                seller_id=str(seller_id or ""),
                shop_name=shop_name,
                text=text,
            )
        except Exception:
            wallet = ""

        if wallet:
            row["Q_payment_method"] = wallet
        else:
            # ไม่เดา → ปล่อยว่างให้ NEEDS_REVIEW ได้
            row["Q_payment_method"] = str(row.get("Q_payment_method") or "").strip()

    # ✅ GL code fill
    if not str(row.get("K_account") or "").strip():
        row["K_account"] = _resolve_gl_code(ctax, p, row, cfg)

    # minimal defaults (กัน PEAK import error)
    row.setdefault("A_seq", "")
    row.setdefault("J_price_type", row.get("J_price_type") or ("3" if p in ("META", "GOOGLE") else "1"))
    row.setdefault("M_qty", row.get("M_qty") or "1")
    if not str(row.get("O_vat_rate") or "").strip():
        row["O_vat_rate"] = "NO" if p in ("META", "GOOGLE") else "7%"

    # ✅ WHT policy (SMART: keep/extract/calc + net paid)
    row = _apply_wht_policy(row, cfg, text=text)

    # lock schema
    row = lock_peak_columns(row)
    return row


def _record_ai_error(row: Dict[str, Any], stage: str, exc: Exception) -> None:
    if os.getenv("STORE_AI_ERROR_META", "1") != "1":
        return
    msg = f"{stage}: {type(exc).__name__}: {str(exc)}"
    msg = msg[:500]
    arr = row.get("_ai_errors")
    if not isinstance(arr, list):
        arr = []
    arr.append(msg)
    row["_ai_errors"] = arr


# ============================================================
# Platform normalization mapping: classifier -> router
# ============================================================

def _normalize_platform_label(platform_raw: str) -> str:
    p = _norm_platform(platform_raw) or "UNKNOWN"
    if p in ("META", "GOOGLE", "SHOPEE", "LAZADA", "TIKTOK", "SPX", "THAI_TAX"):
        return p
    if p in ("UNKNOWN", ""):
        return "GENERIC"
    return "GENERIC"


# ============================================================
# 🔥 MAIN CORE FUNCTION (ตัวจริง)
# ============================================================

def extract_row(
    text: str,
    filename: str = "",
    client_tax_id: str = "",
    cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[str, Dict[str, Any], List[str]]:
    """
    ✅ ตัวจริง: แนะนำให้ส่วนอื่นในระบบใช้ชื่อ extract_row เป็นหลัก
    ✅ MUST PASS filename + cfg ลงไปถึง extractor ทุกตัว
    ✅ SMART FIXES:
      - resolve client_tax_id from cfg
      - reference selection: doc/text > filename (avoid hash)
      - auto-detect WHT from doc text (optional) + net paid
      - normalize payment method (EWLxxx) if wallet_mapping available
      - deterministic debug metadata
    """
    text = text or ""
    filename = filename or ""
    client_tax_id = (client_tax_id or "").strip()
    cfg = cfg or {}

    # ✅ resolve tax from cfg if empty / list
    resolved_tax = client_tax_id or _resolve_client_tax_id_from_cfg(cfg, filename=filename, text=text)
    if resolved_tax:
        client_tax_id = resolved_tax

    # 1) classify
    try:
        try:
            sig = inspect.signature(classify_platform)
            params = sig.parameters
            if "cfg" in params:
                platform_raw = classify_platform(text, filename=filename, cfg=cfg)
            else:
                platform_raw = classify_platform(text, filename=filename)
        except Exception:
            platform_raw = classify_platform(text, filename=filename)
    except Exception as e:
        logger.exception("classify_platform failed: %s", e)
        platform_raw = "UNKNOWN"

    platform_route = _normalize_platform_label(platform_raw)
    platform_out = platform_route if platform_route != "GENERIC" else "UNKNOWN"

    logger.info("Platform classified: %s -> route=%s (file=%s)", platform_raw, platform_route, filename)

    # 2) route to extractor
    try:
        if platform_route == "META":
            if _META_EXTRACTOR_OK and extract_meta_ads is not None:
                row = _safe_call_extractor(
                    extract_meta_ads,
                    text,
                    filename=filename,
                    client_tax_id=client_tax_id,
                    cfg=cfg,
                    platform_hint="META",
                )
                row["_extraction_method"] = "rule_based_meta"
            else:
                if _AI_OK and extract_with_ai is not None:
                    row = _safe_call_extractor(
                        extract_with_ai,
                        text,
                        filename=filename,
                        client_tax_id=client_tax_id,
                        cfg=cfg,
                        platform_hint="META",
                    )
                    row["_extraction_method"] = "ai_meta_fallback"
                else:
                    row = _safe_call_extractor(
                        extract_generic,
                        text,
                        filename=filename,
                        client_tax_id=client_tax_id,
                        cfg=cfg,
                        platform_hint="META",
                    )
                    row["_extraction_method"] = "generic_meta_fallback"
                    row["_missing_extractor"] = "meta"

        elif platform_route == "GOOGLE":
            if _GOOGLE_EXTRACTOR_OK and extract_google_ads is not None:
                row = _safe_call_extractor(
                    extract_google_ads,
                    text,
                    filename=filename,
                    client_tax_id=client_tax_id,
                    cfg=cfg,
                    platform_hint="GOOGLE",
                )
                row["_extraction_method"] = "rule_based_google"
            else:
                if _AI_OK and extract_with_ai is not None:
                    row = _safe_call_extractor(
                        extract_with_ai,
                        text,
                        filename=filename,
                        client_tax_id=client_tax_id,
                        cfg=cfg,
                        platform_hint="GOOGLE",
                    )
                    row["_extraction_method"] = "ai_google_fallback"
                else:
                    row = _safe_call_extractor(
                        extract_generic,
                        text,
                        filename=filename,
                        client_tax_id=client_tax_id,
                        cfg=cfg,
                        platform_hint="GOOGLE",
                    )
                    row["_extraction_method"] = "generic_google_fallback"
                    row["_missing_extractor"] = "google"

        elif platform_route == "SHOPEE":
            row = _safe_call_extractor(
                extract_shopee,
                text,
                filename=filename,
                client_tax_id=client_tax_id,
                cfg=cfg,
                platform_hint="SHOPEE",
            )
            row["_extraction_method"] = "rule_based_shopee"

        elif platform_route == "LAZADA":
            row = _safe_call_extractor(
                extract_lazada,
                text,
                filename=filename,
                client_tax_id=client_tax_id,
                cfg=cfg,
                platform_hint="LAZADA",
            )
            row["_extraction_method"] = "rule_based_lazada"

        elif platform_route == "TIKTOK":
            row = _safe_call_extractor(
                extract_tiktok,
                text,
                filename=filename,
                client_tax_id=client_tax_id,
                cfg=cfg,
                platform_hint="TIKTOK",
            )
            row["_extraction_method"] = "rule_based_tiktok"

        elif platform_route == "SPX":
            if _SPX_EXTRACTOR_OK and extract_spx is not None:
                row = _safe_call_extractor(
                    extract_spx,
                    text,
                    filename=filename,
                    client_tax_id=client_tax_id,
                    cfg=cfg,
                    platform_hint="SPX",
                )
                row["_extraction_method"] = "rule_based_spx"
            else:
                row = _safe_call_extractor(
                    extract_generic,
                    text,
                    filename=filename,
                    client_tax_id=client_tax_id,
                    cfg=cfg,
                    platform_hint="SPX",
                )
                row["_extraction_method"] = "generic_spx_fallback"
                row["_missing_extractor"] = "spx"

        elif platform_route == "THAI_TAX":
            # ถ้าอยากใช้ AI ช่วยเติม Thai Tax ให้เปิด ENABLE_AI_EXTRACT + ENABLE_LLM ใน env
            if _AI_OK and extract_with_ai is not None and os.getenv("ENABLE_AI_THAI_TAX", "1") == "1":
                row = _safe_call_extractor(
                    extract_with_ai,
                    text,
                    filename=filename,
                    client_tax_id=client_tax_id,
                    cfg=cfg,
                    platform_hint="THAI_TAX",
                )
                row["_extraction_method"] = "ai_thai_tax"
            else:
                row = _safe_call_extractor(
                    extract_generic,
                    text,
                    filename=filename,
                    client_tax_id=client_tax_id,
                    cfg=cfg,
                    platform_hint="THAI_TAX",
                )
                row["_extraction_method"] = "generic_thai_tax_fallback"

        else:
            row = _safe_call_extractor(
                extract_generic,
                text,
                filename=filename,
                client_tax_id=client_tax_id,
                cfg=cfg,
                platform_hint="GENERIC",
            )
            row["_extraction_method"] = "generic"

    except Exception as e:
        logger.exception("Extractor error (platform=%s, file=%s)", platform_route, filename)
        row = _sanitize_incoming_row(extract_generic(text))
        row["_extractor_error"] = f"{type(e).__name__}: {str(e)}"[:500]
        row["_extraction_method"] = "generic_error_fallback"

    row = _sanitize_incoming_row(row)

    # 2.1 minimal defaults
    row.setdefault("A_seq", "")
    if row.get("M_qty") in ("", None):
        row["M_qty"] = "1"

    # debug meta (helps deploy vs local)
    if os.getenv("STORE_CLASSIFIER_META", "1") == "1":
        row["_platform"] = platform_out
        row["_platform_route"] = platform_route
        row["_platform_raw"] = platform_raw
        row["_filename"] = filename

        # also store important flags so you can compare deploy/local quickly
        row["_env_ai_extract"] = os.getenv("ENABLE_AI_EXTRACT", "0")
        row["_env_ai_repair"] = os.getenv("AI_REPAIR_PASS", "0")
        row["_env_ai_fill_missing"] = os.getenv("AI_FILL_MISSING", "1")
        row["_env_enable_llm"] = os.getenv("ENABLE_LLM", "")
        row["_env_ocr_provider"] = os.getenv("OCR_PROVIDER", "")
        if cfg:
            row["_cfg"] = str(cfg)[:300]

    # 3) optional AI enhancement for non-meta/google (ONLY when explicitly enabled)
    should_enhance = (
        platform_route not in ("META", "GOOGLE")
        and _AI_OK
        and extract_with_ai is not None
        and os.getenv("ENABLE_AI_EXTRACT", "0") == "1"
    )

    if should_enhance:
        try:
            ai_raw = _safe_call_extractor(
                extract_with_ai,
                text,
                filename=filename,
                client_tax_id=client_tax_id,
                cfg=cfg,
                platform_hint=platform_out,
            )
            ai_row = _sanitize_ai_row(_sanitize_incoming_row(ai_raw))
            fill_missing = os.getenv("AI_FILL_MISSING", "1") == "1"
            row = _merge_rows(row, ai_row, fill_missing=fill_missing)
            if row.get("_extraction_method"):
                row["_extraction_method"] = f"{row['_extraction_method']}+ai"
        except Exception as e:
            logger.warning("AI enhancement failed (file=%s): %s", filename, e)
            _record_ai_error(row, "ai_enhance", e)

    # 4) validate
    errors = _validate_row(row)

    # 5) optional AI repair pass if errors (ONLY when enabled)
    if (
        errors
        and platform_route not in ("META", "GOOGLE")
        and _AI_OK
        and extract_with_ai is not None
        and os.getenv("AI_REPAIR_PASS", "0") == "1"
    ):
        try:
            prompt = (text or "") + "\n\n# VALIDATION_ERRORS\n" + "\n".join(errors)
            ai_fix_raw = _safe_call_extractor(
                extract_with_ai,
                prompt,
                filename=filename,
                client_tax_id=client_tax_id,
                cfg=cfg,
                platform_hint=platform_out,
            )
            ai_fix = _sanitize_ai_row(_sanitize_incoming_row(ai_fix_raw))
            row = _merge_rows(row, ai_fix, fill_missing=False)
            errors = _validate_row(row)
        except Exception as e:
            logger.warning("AI repair failed (file=%s): %s", filename, e)
            _record_ai_error(row, "ai_repair", e)

    # ✅ refresh client_tax_id again (บาง extractor อาจตั้งค่าใน cfg/row)
    client_tax_id = (client_tax_id or "").strip() or _resolve_client_tax_id_from_cfg(cfg, filename=filename, text=text)

    # 6) vendor mapping pass (force Cxxxxx)
    row = _apply_vendor_code_mapping(row, text, client_tax_id)

    # 7) payment method normalize (pre-finalize)
    row = _apply_payment_method_mapping(row, text)

    # 8) ✅ FINALIZE + LOCK (MUST PASS cfg + filename)
    row = finalize_row(
        row,
        platform=platform_out,
        text=text,
        filename=filename,
        client_tax_id=client_tax_id,
        cfg=cfg,
    )

    return platform_out, row, errors


# ============================================================
# ✅ ALIAS (ตัวที่ job_worker import ต้องเจอชื่อนี้แน่นอน)
# ============================================================

def extract_row_from_text(
    text: str,
    filename: str = "",
    client_tax_id: str = "",
    cfg: Optional[Dict[str, Any]] = None,
) -> Tuple[str, Dict[str, Any], List[str]]:
    """
    ✅ Backward-compatible alias:
    job_worker.py does:
      from .extract_service import extract_row_from_text
    """
    return extract_row(text, filename=filename, client_tax_id=client_tax_id, cfg=cfg)


__all__ = [
    "extract_row",  # ✅ new canonical
    "extract_row_from_text",  # ✅ backward-compatible
    "finalize_row",
    "PEAK_KEYS_ORDER",
    "PLATFORM_GROUPS",
    "PLATFORM_DESCRIPTIONS",
]

# 🔒 fingerprint log (ช่วยเช็ค deploy ว่าใช้ไฟล์เวอร์ชันนี้จริง)
logger.warning("EXTRACT_SERVICE_FINGERPRINT=v2026-01-22-smart-all-platforms-1")

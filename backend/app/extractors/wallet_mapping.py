from __future__ import annotations

"""
Wallet Mapping System — PEAK Importer (Q_payment_method)

Goal:
- Fill PEAK column Q_payment_method ("ชำระโดย") with wallet code EWLxxx
- Use our company (client_tax_id) + seller/shop identity to map reliably

Design:
- Accept seller/shop id that can be:
    * Digits (Shopee seller_id)
    * Alphanumeric (Lazada seller_id like TH1..., TikTok like THLC...)
- Fallback: shop_name / label keywords (normalized string)
- Optional: extract seller/shop id from OCR text (patterns)
- Robust normalization (Thai digits, whitespace, punctuation)

Behavior:
- Return "" if cannot resolve (caller should mark NEEDS_REVIEW)
- NEVER return platform name (Shopee/Lazada/etc.)
"""

from typing import Dict, Tuple, List
import re

# ============================================================
# Client Tax ID Constants (our companies)
# ============================================================
CLIENT_RABBIT = "0105561071873"
CLIENT_SHD = "0105563022918"
CLIENT_TOPONE = "0105565027615"

# ============================================================
# Wallet mappings by seller/shop id (key is normalized id)
# - Shopee: digits string
# - Lazada/TikTok: alphanumeric like TH1..., THLC...
# ============================================================

# Rabbit wallets
RABBIT_WALLET_BY_SELLER_ID: Dict[str, str] = {
    "253227155": "EWL001",
    "235607098": "EWL002",
    "516516644": "EWL003",
    "1443909809": "EWL004",
    "1232116856": "EWL005",
    "1357179095": "EWL006",
    "1416156484": "EWL007",
    "418530715": "EWL008",
    "349400909": "EWL009",
    "142025022504068027": "EWL010",
}

# SHD wallets
SHD_WALLET_BY_SELLER_ID: Dict[str, str] = {
    "628286975": "EWL001",
    "340395201": "EWL002",
    "383844799": "EWL003",
    "261472748": "EWL004",
    "517180669": "EWL005",
    "426162640": "EWL006",
    "231427130": "EWL007",
    "1646465545": "EWL008",
}

# TOPONE wallets (เพิ่มครบตามที่คุณให้มา)
TOPONE_WALLET_BY_SELLER_ID: Dict[str, str] = {
    # Shopee (digits)
    "538498056": "EWL001",  # Vinko Thailand store
    "503500831": "EWL002",  # New Age Pet official store

    # Lazada (alphanumeric)
    "TH1K0CDIML": "EWL003",  # Vinko
    "TH1JSB2Z2K": "EWL004",  # New Age Pet

    # TikTok (alphanumeric)
    "THLC6LWARA": "EWL005",  # NewAgePet
    "THLCTGW4XH": "EWL006",  # Vinko Thailand
}

# ============================================================
# Fallback mapping by shop name keywords (normalized lowercase)
# (Use when seller_id missing)
# ============================================================

RABBIT_WALLET_BY_SHOP_KEYWORD: Dict[str, str] = {
    "shopee-70mai": "EWL001",
    "70mai": "EWL001",
    "shopee-ddpai": "EWL002",
    "ddpai": "EWL002",
    "shopee-jimmy": "EWL003",
    "jimmy": "EWL003",
    "shopee-mibro": "EWL004",
    "mibro": "EWL004",
    "shopee-mova": "EWL005",
    "mova": "EWL005",
    "shopee-toptoy": "EWL006",
    "toptoy": "EWL006",
    "shopee-uwant": "EWL007",
    "uwant": "EWL007",
    "shopee-wanbo": "EWL008",
    "wanbo": "EWL008",
    "shopee-zepp": "EWL009",
    "zepp": "EWL009",
    "rabbit": "EWL010",
}

SHD_WALLET_BY_SHOP_KEYWORD: Dict[str, str] = {
    "shopee-ankerthailandstore": "EWL001",
    "ankerthailandstore": "EWL001",
    "anker": "EWL001",
    "shopee-dreamofficial": "EWL002",
    "dreamofficial": "EWL002",
    "dreame": "EWL002",
    "shopee-levoitofficialstore": "EWL003",
    "levoitofficialstore": "EWL003",
    "levoit": "EWL003",
    "shopee-soundcoreofficialstore": "EWL004",
    "soundcoreofficialstore": "EWL004",
    "soundcore": "EWL004",
    "xiaomismartappliances": "EWL005",
    "shopee-xiaomi.thailand": "EWL006",
    "xiaomi.thailand": "EWL006",
    "xiaomi_home_appliances": "EWL007",
    "shopee-nextgadget": "EWL008",
    "nextgadget": "EWL008",
}

TOPONE_WALLET_BY_SHOP_KEYWORD: Dict[str, str] = {
    # Shopee
    "shopee-vinkothailandstore": "EWL001",
    "vinkothailandstore": "EWL001",
    "vinko": "EWL001",
    "newagepetofficialstore": "EWL002",
    "new age pet": "EWL002",
    "newagepet": "EWL002",

    # Lazada
    "lazada": "",  # ไม่ map ด้วยคำว่า lazada ตรงๆ กัน false positive
    "th1k0cdiml": "EWL003",
    "th1jsb2z2k": "EWL004",

    # TikTok
    "tiktok": "",  # กัน false positive
    "thlc6lwara": "EWL005",
    "thlctgw4xh": "EWL006",
}

# ============================================================
# Regex for extracting seller/shop ids from OCR text
# - Shopee: digits
# - Lazada/TikTok: TH... alphanumeric
# ============================================================

EWL_RE = re.compile(r"^EWL\d{3}$", re.IGNORECASE)

# Thai digit normalize
_TH_DIGITS = "๐๑๒๓๔๕๖๗๘๙"
_AR_DIGITS = "0123456789"
_TH2AR = str.maketrans({_TH_DIGITS[i]: _AR_DIGITS[i] for i in range(10)})

def _thai_digits_to_arabic(s: str) -> str:
    return (s or "").translate(_TH2AR)

def _norm_text(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    s = _thai_digits_to_arabic(s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def _digits_only(s: str) -> str:
    if not s:
        return ""
    s = _thai_digits_to_arabic(str(s))
    return "".join(ch for ch in s if ch.isdigit())

def _norm_id_loose(s: str) -> str:
    """
    Normalize seller/shop id:
    - Keep digits-only id as digits
    - Keep alphanumeric id as uppercase, strip spaces/punct
    """
    s = _norm_text(s)
    if not s:
        return ""
    # remove common separators
    s2 = re.sub(r"[^\w]+", "", s)  # keep A-Z0-9_
    if not s2:
        return ""
    # if looks numeric -> digits only
    if s2.isdigit():
        return _digits_only(s2)
    return s2.upper()

def _norm_shop_name(shop_name: str) -> str:
    s = _norm_text(shop_name).lower()
    if not s:
        return ""
    s = re.sub(r"[\"'`“”‘’\(\)\[\]\{\}<>]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _is_valid_wallet(code: str) -> bool:
    return bool(code) and bool(EWL_RE.match(code.strip()))

def _client_bucket(client_tax_id: str) -> str:
    d = _digits_only(client_tax_id)
    if d == CLIENT_RABBIT:
        return "RABBIT"
    if d == CLIENT_SHD:
        return "SHD"
    if d == CLIENT_TOPONE:
        return "TOPONE"
    return ""

def _tables_for_client(bucket: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    if bucket == "RABBIT":
        return (RABBIT_WALLET_BY_SELLER_ID, RABBIT_WALLET_BY_SHOP_KEYWORD)
    if bucket == "SHD":
        return (SHD_WALLET_BY_SELLER_ID, SHD_WALLET_BY_SHOP_KEYWORD)
    if bucket == "TOPONE":
        return (TOPONE_WALLET_BY_SELLER_ID, TOPONE_WALLET_BY_SHOP_KEYWORD)
    return ({}, {})

def _match_shop_keyword(shop_norm: str, by_shop: Dict[str, str]) -> str:
    if not shop_norm or not by_shop:
        return ""
    keys = sorted((k for k in by_shop.keys() if k), key=len, reverse=True)
    for k in keys:
        code = by_shop.get(k, "")
        if not _is_valid_wallet(code):
            continue
        if k in shop_norm:
            return code
    return ""

# ---- OCR ID patterns ----
# Shopee digits:
_RX_SID_DIGITS = re.compile(r"\b(?:seller|shop|merchant|store)\s*(?:id)?\s*[:#=\-]?\s*([0-9๐-๙][0-9๐-๙\s,\-]{4,30})\b", re.IGNORECASE)
# Lazada/TikTok code-like:
_RX_ID_TH = re.compile(r"\b(TH[0-9A-Z]{6,})\b", re.IGNORECASE)

def _extract_id_from_text(text: str) -> str:
    t = _norm_text(text)
    if not t:
        return ""
    # 1) digits id
    m = _RX_SID_DIGITS.search(t)
    if m:
        raw = m.group(1) or ""
        sid = _norm_id_loose(raw)
        if sid and (sid.isdigit() and len(sid) >= 5):
            return sid
    # 2) TH... id
    m2 = _RX_ID_TH.search(t)
    if m2:
        return _norm_id_loose(m2.group(1))
    return ""

# ============================================================
# Public API
# ============================================================
def resolve_wallet_code(
    client_tax_id: str,
    *,
    seller_id: str = "",
    shop_name: str = "",
    text: str = "",
) -> str:
    bucket = _client_bucket(client_tax_id)
    if not bucket:
        return ""

    by_id, by_shop = _tables_for_client(bucket)

    # 1) direct id
    sid = _norm_id_loose(seller_id)
    if sid:
        code = by_id.get(sid, "")
        if _is_valid_wallet(code):
            return code

    # 2) extract id from OCR text
    if (not sid) and text:
        sid = _extract_id_from_text(text)
        if sid:
            code = by_id.get(sid, "")
            if _is_valid_wallet(code):
                return code

    # 3) fallback by shop name keyword
    shop_norm = _norm_shop_name(shop_name)
    if shop_norm:
        code = _match_shop_keyword(shop_norm, by_shop)
        if _is_valid_wallet(code):
            return code

    # 4) last fallback: keyword search in OCR text (riskier)
    if text:
        t_norm = _norm_shop_name(text)
        code = _match_shop_keyword(t_norm, by_shop)
        if _is_valid_wallet(code):
            return code

    return ""

def extract_seller_id_best_effort(text: str) -> str:
    return _extract_id_from_text(text)

__all__ = [
    "resolve_wallet_code",
    "extract_seller_id_best_effort",
    "CLIENT_RABBIT",
    "CLIENT_SHD",
    "CLIENT_TOPONE",
]

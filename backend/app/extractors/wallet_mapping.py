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
CLIENT_HASHTAG = "0105568015456"

# ============================================================
# Wallet mappings by seller/shop id (key is normalized id)
# - Shopee: digits string
# - Lazada/TikTok: alphanumeric like TH1..., THLC...
# ============================================================

# Rabbit wallets
RABBIT_WALLET_BY_SELLER_ID: Dict[str, str] = {
    # Shopee (digits)
    "253227155": "EWL001",          # Shopee - 70maiofficialstore1
    "235607098": "EWL002",          # Shopee - ddpaiofficialstore
    "516516644": "EWL003",          # Shopee - jimmyofficialstore
    "1443909809": "EWL004",         # Shopee - mibrothailandstore
    "1232116856": "EWL005",         # Shopee - movaofficialstore
    "1357179095": "EWL006",         # Shopee - toptoythailandstore
    "1416156484": "EWL007",         # Shopee - uwantthailand
    "418530715": "EWL008",          # Shopee - wanboofficialstore
    "349400909": "EWL009",          # Shopee - zepp_thailand
    "142025022504068027": "EWL010", # Shopify - Rabbit Thailand

    # Lazada (alphanumeric)
    "TH1HOOSEO": "EWL011",          # Lazada - Jimmy Home Appliances
    "TH1JHFZ0EM": "EWL012",         # Lazada - 70Mai
    "TH1JHJLZ8F": "EWL013",         # Lazada - Zepp
    "TH1JHEP23B": "EWL014",         # Lazada - DDPai Official
    # EWL015 Lazada - Wanbo  : รอเลข shop id จริง (sheet ยังเป็น THXXX)
    # EWL016 Lazada - Toptoy : รอเลข shop id จริง (sheet ยังเป็น THXXX)
    # EWL017 Lazada - Mibro  : รอเลข shop id จริง (sheet ยังเป็น THXXX)
    # EWL018 Lazada - Mova   : รอเลข shop id จริง (sheet ยังเป็น THXXX)

    # TikTok (alphanumeric)
    "THLCN2WLDN": "EWL019",         # TikTok - Jimmy Thailand
    # EWL020 TikTok - Zepp  / EWL021 TikTok - DDPAI : sheet ให้เลขซ้ำกัน (THLCYWWL3G) รอยืนยัน
    "THLC22WLL9": "EWL022",         # TikTok - wanbo Thailand Store
    "THLCLLWL2H": "EWL023",         # TikTok - 70Mai
    # EWL024 TikTok - Toptoy : รอเลข shop id จริง (sheet ยังเป็น THXXX)
    # EWL025 TikTok - Mibro  : รอเลข shop id จริง (sheet ยังเป็น THXXX)
    # EWL026 TikTok - Mibro  : รอเลข shop id จริง (sheet ยังเป็น THXXX, ซ้ำแบรนด์กับ EWL025)
    # EWL027 TikTok - Mova   : รอเลข shop id จริง (sheet ยังเป็น THXXX)
}

# SHD wallets
SHD_WALLET_BY_SELLER_ID: Dict[str, str] = {
    # Shopee (digits)
    "628286975": "EWL001",          # Shopee - ankerthailandstore
    "340395201": "EWL002",          # Shopee - dreameofficial
    "383844799": "EWL003",          # Shopee - levoitofficialstore
    "261472748": "EWL004",          # Shopee - soundcoreofficialstore
    "517180669": "EWL005",          # Shopee - xiaomismartappliances
    "426162640": "EWL006",          # Shopee - xiaomi.thailand
    "231427130": "EWL007",          # Shopee - xiaomi_home_appliances
    "1646465545": "EWL008",         # Shopee - nextgadget
    "142024121303920354": "EWL009", # Shopify - เอสเอชดี เทคโนโลยี

    # Lazada (alphanumeric)
    "TH1JHTKKML": "EWL010",         # Lazada - Anker Official Store
    "TH1JHM0VG9": "EWL011",         # Lazada - Levolt
    "TH1JHKN2ZK": "EWL012",         # Lazada - DreameOfficialStore
    "TH1JHLR7K0": "EWL013",         # Lazada - Xiaomi Thailand Store
    "TH1JT7SFKG": "EWL014",         # Lazada - Xiaomi Home Appliamces
    "TH1JHRLDDJ": "EWL015",         # Lazada - PerySmith Home Appliances
    "TH1JHOJ40S": "EWL016",         # Lazada - SoundcoreOfficialStore

    # TikTok (alphanumeric)
    "THLCEMWTR6": "EWL017",         # TikTok - Anker Thailand Store
    "THLCJXW4B9": "EWL018",         # TikTok - Xiaomi Home Appliances (โปรดยืนยันเลข ID ถูกตัดขอบภาพ)
    "THLCR2WLDW": "EWL019",         # TikTok - Dreame Thailand
    # EWL020 TikTok - Soundcore Thailand Store : เลขในภาพถูกตัด (THLCJTWNL...) รอเลขเต็ม
    "THLCQ2WLDD": "EWL021",         # TikTok - Levoit Thailand
    "THLC76WHER": "EWL022",         # TikTok - Xiaomi Thailand Store
    # EWL023 NocNoc - Dreame          : ไม่มี seller id (map ด้วย keyword)
    # EWL024 NocNoc - Levolt          : ไม่มี seller id (map ด้วย keyword)
    # EWL025 Lazada - Youpin Online   : ไม่มี seller id (map ด้วย keyword)
    # EWL026 Shopee - Dreame personal care : ไม่มี seller id (map ด้วย keyword)
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
    # EWL007 Shopee - Xiaomi Home      : ไม่มี seller id (map ด้วย keyword)
    # EWL008 Shopee - Shopee-nextgadget: ไม่มี seller id (map ด้วย keyword)
}

# Hashtag wallets — ตารางในภาพไม่มี seller id เลย (ทุกแถวเป็น Xiaomi)
# แยกได้ด้วย "แพลตฟอร์ม" เท่านั้น -> ใช้ *_BY_PLATFORM_KEYWORD ด้านล่าง
HASHTAG_WALLET_BY_SELLER_ID: Dict[str, str] = {}

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
    # Shopee brands (keyword fallback = Shopee default)
    "shopee-ankerthailandstore": "EWL001",
    "ankerthailandstore": "EWL001",
    "anker": "EWL001",
    "shopee-dreamofficial": "EWL002",
    "dreameofficial": "EWL002",
    "dreamofficial": "EWL002",
    "dreame": "EWL002",
    "shopee-levoitofficialstore": "EWL003",
    "levoitofficialstore": "EWL003",
    "levoit": "EWL003",
    "shopee-soundcoreofficialstore": "EWL004",
    "soundcoreofficialstore": "EWL004",
    "soundcore": "EWL004",
    "xiaomismartappliances": "EWL005",
    "xiaomi smart appliances": "EWL005",
    "shopee-xiaomi.thailand": "EWL006",
    "xiaomi.thailand": "EWL006",
    "xiaomi_home_appliances": "EWL007",
    "shopee-nextgadget": "EWL008",
    "nextgadget": "EWL008",
    # Unique no-id shops (แบรนด์ไม่ชนกับแพลตฟอร์มอื่น -> map ด้วย keyword ตรง ๆ ได้)
    "perysmith": "EWL015",              # Lazada - PerySmith Home Appliances (มี id ด้วย)
    "youpin": "EWL025",                 # Lazada - Youpin Online
    "dreame personal care": "EWL026",   # Shopee - Dreame personal care (ยาวกว่า "dreame" -> ชนะ longest-first)
    "nocnoc dreame": "EWL023",          # NocNoc - Dreame
    "nocnoc levolt": "EWL024",          # NocNoc - Levolt
    "nocnoc levoit": "EWL024",
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
    # no-id shops
    "xiaomi home": "EWL007",     # Shopee - Xiaomi Home
    "nextgadget": "EWL008",      # Shopee - Shopee-nextgadget
}

# Hashtag: ทุกแถวเป็น "Xiaomi" ไม่มี id -> keyword ธรรมดาแยกไม่ได้ ต้องพึ่ง platform
# (ปล่อย keyword ธรรมดาให้ default = Shopee tier)
HASHTAG_WALLET_BY_SHOP_KEYWORD: Dict[str, str] = {
    "xiaomi smart": "EWL002",   # Shopee - Xiaomi Smart
    "xiaomi thai": "EWL001",    # Shopee - Xiaomi Thai
}

# ============================================================
# Platform-aware keyword fallback (ใช้เมื่อรู้ platform)
# client -> platform -> {keyword: EWLxxx}
# แก้ปัญหาแบรนด์ซ้ำข้ามแพลตฟอร์ม (เช่น Hashtag "Xiaomi" ทุก platform)
# ============================================================
HASHTAG_WALLET_BY_PLATFORM_KEYWORD: Dict[str, Dict[str, str]] = {
    "SHOPEE": {"xiaomi smart": "EWL002", "xiaomi thai": "EWL001", "xiaomi": "EWL001"},
    "LAZADA": {"xiaomi": "EWL003"},   # Lazada - Xiaomi Thailand
    "TIKTOK": {"xiaomi": "EWL004"},   # TikTok - Xiaomi Thailand
}

WALLET_BY_PLATFORM_KEYWORD: Dict[str, Dict[str, Dict[str, str]]] = {
    "HASHTAG": HASHTAG_WALLET_BY_PLATFORM_KEYWORD,
}

# ============================================================
# Platform-default wallet — ใช้เมื่อ (บริษัท × platform) มีร้านเดียว
# หรือมี "ค่าเริ่มต้น" ที่ปลอดภัย (ใช้เป็น fallback สุดท้ายเมื่อรู้ platform)
# Hashtag: Lazada/TikTok มีร้านเดียว (Xiaomi Thailand), Shopee ตั้งต้น Xiaomi Thai
# ⚠️ ใส่เฉพาะบริษัทที่ปลอดภัยเท่านั้น (บริษัทที่มีหลายร้านต่อ platform ห้ามใส่)
# ============================================================
HASHTAG_WALLET_BY_PLATFORM_DEFAULT: Dict[str, str] = {
    "SHOPEE": "EWL001",   # Shopee - Xiaomi Thai (ค่าเริ่มต้นเมื่อ keyword แยกไม่ได้)
    "LAZADA": "EWL003",   # Lazada - Xiaomi Thailand (ร้านเดียว)
    "TIKTOK": "EWL004",   # TikTok - Xiaomi Thailand (ร้านเดียว)
}

WALLET_BY_PLATFORM_DEFAULT: Dict[str, Dict[str, str]] = {
    "HASHTAG": HASHTAG_WALLET_BY_PLATFORM_DEFAULT,
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
    if d == CLIENT_HASHTAG:
        return "HASHTAG"
    return ""

def _tables_for_client(bucket: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    if bucket == "RABBIT":
        return (RABBIT_WALLET_BY_SELLER_ID, RABBIT_WALLET_BY_SHOP_KEYWORD)
    if bucket == "SHD":
        return (SHD_WALLET_BY_SELLER_ID, SHD_WALLET_BY_SHOP_KEYWORD)
    if bucket == "TOPONE":
        return (TOPONE_WALLET_BY_SELLER_ID, TOPONE_WALLET_BY_SHOP_KEYWORD)
    if bucket == "HASHTAG":
        return (HASHTAG_WALLET_BY_SELLER_ID, HASHTAG_WALLET_BY_SHOP_KEYWORD)
    return ({}, {})

# platform label (จาก classifier/extractor) -> platform key ของ keyword map
_PLATFORM_ALIASES: Dict[str, str] = {
    "SHOPEE": "SHOPEE", "SHOPEE MALL": "SHOPEE",
    "LAZADA": "LAZADA", "LAZADA EXPRESS": "LAZADA", "LEX": "LAZADA",
    "TIKTOK": "TIKTOK", "TIKTOK SHOP": "TIKTOK",
    "SHOPIFY": "SHOPIFY",
    "NOCNOC": "NOCNOC", "NOC NOC": "NOCNOC",
}

def _norm_platform_key(platform: str) -> str:
    p = _norm_text(platform).upper().strip()
    if not p:
        return ""
    if p in _PLATFORM_ALIASES:
        return _PLATFORM_ALIASES[p]
    for k in sorted(_PLATFORM_ALIASES.keys(), key=len, reverse=True):
        if k in p:
            return _PLATFORM_ALIASES[k]
    return ""

def _match_platform_keyword(bucket: str, platform_key: str, *hays: str) -> str:
    if not bucket or not platform_key:
        return ""
    by_plat = WALLET_BY_PLATFORM_KEYWORD.get(bucket, {})
    kw_map = by_plat.get(platform_key, {})
    if not kw_map:
        return ""
    for hay in hays:
        h = _norm_shop_name(hay)
        if not h:
            continue
        code = _match_shop_keyword(h, kw_map)
        if _is_valid_wallet(code):
            return code
    return ""

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

def _scan_known_id(by_id: Dict[str, str], *hays: str) -> str:
    """
    หา seller/shop id ที่ "รู้จักอยู่แล้ว" (คีย์ใน by_id) จากข้อความดิบ
    - id ตัวเลข: จับแบบมีขอบเขต (ไม่ให้เป็นส่วนหนึ่งของเลขยาวกว่า เช่น เลขภาษี)
    - id ตัวอักษร (TH...): จับแบบมีขอบเขต A-Z0-9
    คืน EWLxxx ทันทีที่เจอ (คีย์ยาวสุดก่อน กัน false hit)
    """
    if not by_id:
        return ""
    up = " ".join(_norm_text(h).upper() for h in hays if h).strip()
    if not up:
        return ""
    for key in sorted((k for k in by_id.keys() if k), key=len, reverse=True):
        code = by_id.get(key, "")
        if not _is_valid_wallet(code):
            continue
        k = key.upper()
        if k.isdigit():
            pat = r"(?<!\d)" + re.escape(k) + r"(?!\d)"
        else:
            pat = r"(?<![A-Z0-9])" + re.escape(k) + r"(?![A-Z0-9])"
        if re.search(pat, up):
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
    platform: str = "",
) -> str:
    bucket = _client_bucket(client_tax_id)
    if not bucket:
        return ""

    by_id, by_shop = _tables_for_client(bucket)
    pkey = _norm_platform_key(platform)

    # 1) direct id (ตรงตัว)
    sid = _norm_id_loose(seller_id)
    if sid:
        code = by_id.get(sid, "")
        if _is_valid_wallet(code):
            return code

    # 2) extract id from OCR text (label-based)
    #    ⚠️ ทำเสมอแม้ seller_id ที่ส่งมาจะไม่ match (กันกรณี job_worker เดา seller_id ผิด)
    if text:
        tid = _extract_id_from_text(text)
        if tid:
            code = by_id.get(tid, "")
            if _is_valid_wallet(code):
                return code

    # 3) scan หา "id ที่รู้จัก" จาก seller_id/ชื่อร้าน/ชื่อไฟล์/ข้อความ (deterministic)
    #    แก้เคสหลักที่ ชำระโดย ตกเป็น "หักจากยอดขาย" เพราะจับ seller_id ไม่ได้
    code = _scan_known_id(by_id, seller_id, shop_name, text)
    if _is_valid_wallet(code):
        return code

    # 4) platform-aware keyword (แก้แบรนด์ซ้ำข้ามแพลตฟอร์ม เช่น Hashtag "Xiaomi")
    if pkey:
        code = _match_platform_keyword(bucket, pkey, shop_name, text)
        if _is_valid_wallet(code):
            return code

    # 4.5) platform-default (บริษัทที่มีร้านเดียวต่อ platform เช่น Hashtag)
    if pkey:
        dflt = WALLET_BY_PLATFORM_DEFAULT.get(bucket, {}).get(pkey, "")
        if _is_valid_wallet(dflt):
            return dflt

    # 5) fallback by shop name keyword (default tier = Shopee)
    shop_norm = _norm_shop_name(shop_name)
    if shop_norm:
        code = _match_shop_keyword(shop_norm, by_shop)
        if _is_valid_wallet(code):
            return code

    # 6) last fallback: keyword search in OCR text (riskier)
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
    "CLIENT_HASHTAG",
]

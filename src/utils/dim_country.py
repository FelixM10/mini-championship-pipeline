"""
Canonical country normalisation for 2024/25 Championship data.

This version includes:
- Extended RAW_TO_CANONICAL aliases
- Second-pass aliasing after pycountry resolution
- Fixed typos (e.g., ECU)
"""

from __future__ import annotations

from functools import lru_cache
from typing import Optional

import pycountry


# ---------------------------------------------------------------------
# Explicit aliases: raw (lowercased) -> canonical full name
# ---------------------------------------------------------------------

RAW_TO_CANONICAL = {
    # UK home nations / football-specific
    "eng": "England",
    "england": "England",
    "sco": "Scotland",
    "scotland": "Scotland",
    "wal": "Wales",
    "wales": "Wales",
    "nir": "Northern Ireland",
    "northern ireland": "Northern Ireland",

    # Ireland
    "irl": "Republic of Ireland",
    "ireland": "Republic of Ireland",
    "republic of ireland": "Republic of Ireland",

    # United States
    "usa": "United States",
    "us": "United States",
    "u.s.": "United States",
    "united states": "United States",
    "united states of america": "United States",

    # DR Congo
    "cod": "Democratic Republic of the Congo",
    "dr congo": "Democratic Republic of the Congo",
    "democratic republic of the congo": "Democratic Republic of the Congo",
    "congo, democratic republic of the": "Democratic Republic of the Congo",
    "congo, the democratic republic of the": "Democratic Republic of the Congo",

    # Republic of the Congo
    "cgo": "Republic of the Congo",
    "congo": "Republic of the Congo",

    # Ivory Coast
    "civ": "Ivory Coast",
    "ivory coast": "Ivory Coast",
    "côte d'ivoire": "Ivory Coast",
    "cote d'ivoire": "Ivory Coast",

    # The Gambia
    "gam": "The Gambia",
    "gambia": "The Gambia",
    "the gambia": "The Gambia",
    "gambia, the": "The Gambia",

    # Curaçao
    "cuw": "Curaçao",
    "curacao": "Curaçao",

    # Guinea / Guinea-Bissau
    "gui": "Guinea",
    "guinea": "Guinea",
    "gnb": "Guinea-Bissau",
    "guinea bissau": "Guinea-Bissau",
    "guinea-bissau": "Guinea-Bissau",

    # South Africa
    "rsa": "South Africa",
    "south africa": "South Africa",

    # Cape Verde
    "cpv": "Cape Verde",
    "cape verde": "Cape Verde",
    "cabo verde": "Cape Verde",

    # Hong Kong
    "hkg": "Hong Kong",
    "hk china": "Hong Kong",
    "hong kong": "Hong Kong",

    # Kosovo
    "kos": "Kosovo",
    "kosovo": "Kosovo",

    # Czech Republic
    "czech republic": "Czech Republic",
    "czechia": "Czech Republic",

    # Bosnia and Herzegovina
    "bosnia-herzegovina": "Bosnia and Herzegovina",
    "bosnia and herzegovina": "Bosnia and Herzegovina",

    # Fix typo: use lowercase key
    "ecu": "Ecuador",

    # South Korea variants
    "kor": "South Korea",
    "korea, south": "South Korea",
    "korea, republic of": "South Korea",

    # Tanzania ISO form
    "tanzania, united republic of": "Tanzania",

    # Some common 3-letter codes
    "alb": "Albania",
    "alg": "Algeria",
    "ang": "Angola",
    "aus": "Australia",
    "aut": "Austria",
    "ban": "Bangladesh",
    "bel": "Belgium",
    "ben": "Benin",
    "ber": "Bermuda",
    "bih": "Bosnia and Herzegovina",
    "bra": "Brazil",
    "bul": "Bulgaria",
    "cam": "Cameroon",
    "cmr": "Cameroon",
    "can": "Canada",
    "chi": "Chile",
    "col": "Colombia",
    "cro": "Croatia",
    "cze": "Czech Republic",
    "den": "Denmark",
    "egy": "Egypt",
    "est": "Estonia",
    "eth": "Ethiopia",
    "fin": "Finland",
    "fra": "France",
    "gab": "Gabon",
    "geo": "Georgia",
    "ger": "Germany",
    "gha": "Ghana",
    "gre": "Greece",
    "grn": "Grenada",
    "gua": "Guatemala",
    "hun": "Hungary",
    "isl": "Iceland",
    "isr": "Israel",
    "ita": "Italy",
    "jam": "Jamaica",
    "jpn": "Japan",
    "ken": "Kenya",
    "ltu": "Lithuania",
    "lux": "Luxembourg",
    "mar": "Morocco",
    "mex": "Mexico",
    "mli": "Mali",
    "mlt": "Malta",
    "mne": "Montenegro",
    "ned": "Netherlands",
    "nga": "Nigeria",
    "nor": "Norway",
    "pol": "Poland",
    "por": "Portugal",
    "rou": "Romania",
    "sen": "Senegal",
    "srb": "Serbia",
    "svk": "Slovakia",
    "svn": "Slovenia",
    "esp": "Spain",
    "swe": "Sweden",
    "sui": "Switzerland",
    "tun": "Tunisia",
    "tur": "Turkey",
    "türkiye": "Turkey",
    "tür": "Turkey",
    "ukr": "Ukraine",
    "zim": "Zimbabwe",

    # English name variants
    "saint kitts and nevis": "Saint Kitts and Nevis",
    "st. kitts & nevis": "Saint Kitts and Nevis",
}


def _clean_raw(value: str) -> str:
    return value.strip().lower()


@lru_cache(maxsize=None)
def _normalize_via_pycountry(raw: str) -> Optional[str]:
    if not raw:
        return None

    s = raw.strip()

    # alpha-3
    c = pycountry.countries.get(alpha_3=s.upper())
    if c:
        return c.name

    # alpha-2
    c = pycountry.countries.get(alpha_2=s.upper())
    if c:
        return c.name

    # direct lookup
    try:
        c = pycountry.countries.lookup(s)
        return c.name
    except Exception:
        pass

    # fuzzy
    try:
        matches = pycountry.countries.search_fuzzy(s)
        if matches:
            return matches[0].name
    except Exception:
        pass

    return None


def normalize_country(value: str) -> str:
    if value is None:
        return value

    if not isinstance(value, str):
        value = str(value)

    raw = value.strip()
    if not raw:
        return raw

    # 1) First-pass aliasing
    key = _clean_raw(raw)
    if key in RAW_TO_CANONICAL:
        return RAW_TO_CANONICAL[key]

    # 2) pycountry
    resolved = _normalize_via_pycountry(raw)
    if resolved is not None:
        resolved_key = _clean_raw(resolved)
        if resolved_key in RAW_TO_CANONICAL:
            return RAW_TO_CANONICAL[resolved_key]
        return resolved

    # 3) fallback
    return raw


def normalize_country_series(series):
    return series.apply(normalize_country)

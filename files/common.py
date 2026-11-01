"""Shared fake-GeoIP table. First octet -> country. (Fake data, not real GeoIP.)"""
COUNTRY_OCTETS = {
    "US": [34, 52, 98], "GB": [81, 86], "DE": [85, 88], "FR": [90, 92],
    "IN": [103, 117], "BR": [177, 179], "JP": [126, 133], "AU": [101, 144],
    "CA": [24, 142], "NL": [145, 146], "RU": [95, 178], "CN": [111, 223],
    "NG": [102, 105], "UA": [91, 176],
}
ISO3 = {
    "US": "USA", "GB": "GBR", "DE": "DEU", "FR": "FRA", "IN": "IND", "BR": "BRA",
    "JP": "JPN", "AU": "AUS", "CA": "CAN", "NL": "NLD", "RU": "RUS", "CN": "CHN",
    "NG": "NGA", "UA": "UKR",
}


def geo_rows():
    return [(o, c, ISO3[c]) for c, octs in COUNTRY_OCTETS.items() for o in octs]

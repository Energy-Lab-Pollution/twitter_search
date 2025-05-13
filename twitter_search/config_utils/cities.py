"""
Script with constants for the cities to look for
"""

CITIES = [
    "abuja",
    "accra",
    "bangalore",
    "bangkok",
    "bogota",
    "buenos aires",
    "cape town",
    "chennai",
    "chiang mai",
    "chicago",
    "delhi",
    "guatemala",
    "houston",
    "johannesburg",
    "kanpur",
    "kigali",
    "kinshasa",
    "kolkata",
    "london",
    "los angeles",
    "madrid",
    "mexico city",
    "melbourne",
    "mumbai",
    "nairobi",
    "new york",
    "paris",
    "phuket",
    "san salvador",
    "sydney",
    "tegucigalpa",
    "toronto",
]


PILOT_CITIES = ["chiang mai", "guatemala", "kanpur", "kigali", "kolkata"]


ALIAS_DICT = {
    "new york city": "new york",
    "nyc": "new york",
    "cdmx": "mexico city",
    "ciudad de mexico": "mexico city",
    "ciudad de méxico": "mexico city",
    "distrito federal": "mexico city",
    "mexico df": "mexico city",
    "bogotá": "bogota",
    "bengaluru": "bangalore",
    "ciudad de guatemala": "guatemala",
    "cd. de guatemala": "guatemala",
    "guatemala, ciudad": "guatemala",
    "guatemala ciudad": "guatemala",
    "guatemala city": "guatemala",
    "guatemala, guatemala": "guatemala",
    "new delhi": "delhi",
    "calcutta": "kolkata",
    "kolkatta": "kolkata",
    "calcuta": "kolkata",
    "bombay": "mumbai",
}


LOCATION_ALIAS_DICT = {
    'new york': ['new york city', 'nyc'],
    'mexico city': ['cdmx', 'ciudad de mexico', 'ciudad de méxico', 'distrito federal', 'mexico df'],
    'bogota': ['bogotá'],
    'bangalore': ['bengaluru'],
    'guatemala': ['ciudad de guatemala', 'cd. de guatemala', 'guatemala, ciudad', 'guatemala ciudad', 'guatemala city', 'guatemala, guatemala'],
    'delhi': ['new delhi'],
    'kolkata': ['calcutta', 'kolkatta', 'calcuta'],
    'mumbai': ['bombay']
}


CITIES_LANGS = {
    "abuja": "en",
    "accra": "en",
    "bangalore": "en",
    "bangkok": "en",
    "bogota": "es",
    "buenos aires": "es",
    "cape town": "en",
    "chennai": "en",
    "chiang mai": "en",
    "chicago": "en",
    "delhi": "en",
    "guatemala": "es",
    "houston": "en",
    "johannesburg": "en",
    "kanpur": "en",
    "kigali": "en",
    "kinshasa": "en",
    "kolkata": "en",
    "london": "en",
    "los angeles": "en",
    "madrid": "es",
    "melbourne": "en",
    "mexico city": "es",
    "mumbai": "en",
    "nairobi": "en",
    "new york": "en",
    "paris": "fr",
    "phuket": "en",
    "san salvador": "es",
    "sydney": "en",
    "tegucigalpa": "es",
    "toronto": "en",
}

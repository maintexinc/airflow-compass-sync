"""Table groupings and shared config."""

# Updated every 5 minutes - real-time inventory movement
TABLES_5MIN = [
    "icsw",    # inventory warehouse
    "icswu",   # inventory warehouse update
]

# Updated every 15 minutes - open-order and transaction data
TABLES_15MIN = [
    "inventory",
    "oeeh",    # order entry header
    "oeel",    # order entry line
    "poeh",    # purchase order header
    "poel",    # purchase order line
    "wteh",    # warehouse transfer header
    "wtel",    # warehouse transfer line
]

# Updated hourly - reference and master data
TABLES_60MIN = [
    "addon",
    "apet",
    "apss",
    "apsv",
    "aret",
    "arsc",
    "arss",
    "binmst",
    "carrier",
    "cartondtl",
    "cartonmst",
    "com",
    "contacts",
    "cycle_cnt",
    "empmst",
    "event_trans",
    "event_trans_sub",
    "glet",
    "glsa",
    "glsb",
    "icet",
    "icsc",
    "icsd",
    "icsef",
    "icseu",
    "icsl",
    "icsp",
    "icss",
    "item",
    "kpet",
    "kpsk",
    "kpskv",
    "movemst",
    "notes",
    "oeehch",
    "order_type",
    "orddtl",
    "ordhdr",
    "ordhdr_status",
    "pder",
    "pdsc",
    "pdsf",
    "pdsr",
    "pdst",
    "pick",
    "poelo",
    "pv_user",
    "rt_type",
    "rtdet",
    "rtmst",
    "sasc",
    "sasta",
    "sastaz",
    "sastc",
    "sastn",
    "smsn",
    "transactions",
    "venmst",
    "wh_zone",
    "whmst",
    "wtelo",
]

# Remove higher-freq tables from hourly to avoid duplicate runs
_higher_freq = set(TABLES_5MIN + TABLES_15MIN)
TABLES_60MIN = [t for t in TABLES_60MIN if t not in _higher_freq]

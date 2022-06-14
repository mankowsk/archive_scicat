
secrets = {}
secrets["swissfelaramis-alvra"]         = "/usr/local/etc/swissfelaramis-alvra.secret"
secrets["swissfelaramis-bernina"]       = "/usr/local/etc/swissfelaramis-bernina.secret"
secrets["swissfelaramis-cristallina"]   = "/usr/local/etc/swissfelaramis-cristallina.secret"
secrets["swissfelathos-maloja"]         = "/usr/local/etc/swissfelathos-maloja.secret"
secrets["swissfelathos-furka"]          = "/usr/local/etc/swissfelathos-furka.secret"

ingestsecrets = {}
ingestsecrets["swissfelaramis-alvra"]         = "/usr/local/etc/swissfelaramis-alvra-ingestion.secret"
ingestsecrets["swissfelaramis-bernina"]       = "/usr/local/etc/swissfelaramis-bernina-ingestion.secret"
ingestsecrets["swissfelaramis-cristallina"]   = "/usr/local/etc/swissfelaramis-cristallina-ingestion.secret"
ingestsecrets["swissfelathos-maloja"]         = "/usr/local/etc/swissfelathos-maloja-ingestion.secret"
ingestsecrets["swissfelathos-furka"]          = "/usr/local/etc/swissfelathos-furka-ingestion.secret"

client = {}
client["alvra"]         = "swissfelaramis-alvra"
client["bernina"]       = "swissfelaramis-bernina"
client["cristallina"]   = "swissfelaramis-cristallina"
client["maloja"]        = "swissfelathos-maloja"
client["furka"]         = "swissfelathos-furka"

scicat = {}
scicat["prod"]          = "https://dacat.psi.ch"
scicat["qa"]            = "https://dacat-qa.psi.ch"
scicat_authn_api        = "/api/v3/Users/login"
scicat_proposals_api    = "/api/v3/proposals"

ingestor_instance["prod"] = "sf-ingestor-2.psi.ch"
ingestor_instance["qa"]   = "sf-ingestor-1.psi.ch"

CLOSED_SEM_FILE     = "raw/run_info/CLOSED"
ARCHIVED_SEM_FILE   = "raw/run_info/ARCHIVED"
IGNORE_SEM_FILE     = "raw/run_info/IGNORE"

maxmountingestchecks = 6

#realingestion   = False
#autoarchive     = False
#debug           = False


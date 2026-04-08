## 06_link_returns.R
## Stub: link call-level signals to CRSP returns using existing helpers

library(dplyr)

devtools::load_all(".")

wrds <- connect_wharton()

call_signals <- readRDS("research/earnings-calls/data/processed/call_signals.rds")

# TODO: implement:
# 1) CIQ companyid -> gvkey (CIQ linking tables on WRDS)
# 2) gvkey -> permno (Compustat–CRSP link; see create_compustat_crsp_link())
# 3) Pull CRSP daily returns around call_date windows
# 4) Compute CARs and merge with call_signals

dbDisconnect(wrds)


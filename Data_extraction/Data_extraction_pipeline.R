library(data.table)
library(dplyr)
library(tidyr)
setDTthreads(threads = 4)

lppub_column_names   <- readLines("sf_column_names.txt")   # full schema, in order
lppub_column_classes <- readLines("sf_column_classes.txt") # optional; aligns to the above
# helper: "MMYYYY" -> integer YYYYMM (e.g., "012018" -> 201801)
to_yyyymm <- function(s) as.integer(sub("^([0-9]{2})([0-9]{4})$", "\\2\\1", s))

# --- Inputs ---------------------------------------------------------------
zip_path <- file.path("../raw_data", paste0(FileName, ".zip"))
csv_name <- paste0(FileName, ".csv")
cmd <- sprintf("unzip -p %s %s", shQuote(zip_path), shQuote(csv_name))

###################################### acquisition_data extraction ##################
# Define the list of Acquisition (Static) columns
acquisition_columns <- c(
  "LOAN_ID", "CHANNEL", "ACT_PERIOD", "SELLER", "ORIG_RATE", "ORIG_UPB", "ORIG_TERM", 
  "ORIG_DATE", "FIRST_PAY", "OLTV", "OCLTV", "NUM_BO", "DTI", "CSCORE_B", 
  "CSCORE_C", "FIRST_FLAG", "PURPOSE", "PROP", "NO_UNITS", "OCC_STAT", 
  "STATE", "ZIP", "MSA", "MI_PCT", "PRODUCT", "MI_TYPE", 
  "HOMEREADY_PROGRAM_INDICATOR", "RELOCATION_MORTGAGE_INDICATOR", 
  "PROPERTY_INSPECTION_WAIVER_INDICATOR", "HIGH_BALANCE_LOAN_INDICATOR", 
  "HIGH_LOAN_TO_VALUE_HLTV_REFINANCE_OPTION_INDICATOR", "DEAL_NAME"
)

# indices of those columns in the raw file
sel_idx <- match(acquisition_columns, lppub_column_names)
if (anyNA(sel_idx)) {
  stop("These requested columns were not found in sf_column_names.txt: ",
       paste(acq_cols[is.na(sel_idx)], collapse = ", "))
}

types_selected <- lppub_column_classes[sel_idx]     # e.g. "character","numeric",...
cc_list <- split(sel_idx, types_selected)    

# Select the acquisition columns from the original data table
DT <-  fread(
  cmd = cmd,
  sep = "|",
  header    = FALSE,          # raw file has no header
  select    = sel_idx,        # select by position to avoid name-mismatch
  colClasses  = cc_list,    # force types by those names
  na.strings = c("", "NA"),
  showProgress = TRUE
)
setnames(DT, names(DT), acquisition_columns)
#

# --- Rename columns in-place ----------------------------------------------
setnames(DT,
         old = c("CHANNEL","ORIG_RATE","ORIG_UPB","ORIG_TERM","OLTV","OCLTV","NUM_BO",
                 "DTI","FIRST_FLAG","PURPOSE","PROP","NO_UNITS","OCC_STAT","STATE","ZIP",
                 "MSA","MI_PCT","PRODUCT",
                 "RELOCATION_MORTGAGE_INDICATOR","HOMEREADY_PROGRAM_INDICATOR",
                 "PROPERTY_INSPECTION_WAIVER_INDICATOR","HIGH_BALANCE_LOAN_INDICATOR",
                 "HIGH_LOAN_TO_VALUE_HLTV_REFINANCE_OPTION_INDICATOR"),
         new = c("ORIG_CHN","orig_rt","orig_amt","orig_trm","oltv","ocltv","num_bo",
                 "dti","FTHB_FLG","purpose","PROP_TYP","NUM_UNIT","occ_stat","state","zip_3",
                 "msa","mi_pct","prod_type",
                 "relo_flg","hrp_ind",
                 "prop_ins_ind","hbl_ind",
                 "hltv_ref_ind")
)

DT[, `:=`(
  ACT_PERIOD = to_yyyymm(ACT_PERIOD),
  FIRST_PAY  = to_yyyymm(FIRST_PAY),
  ORIG_DATE  = to_yyyymm(ORIG_DATE)
)]

# --- Keep the latest record per LOAN_ID (max ACT_PERIOD) ------------------
setorder(DT, LOAN_ID, ACT_PERIOD)        # sort by id, then period
acquisition_data <- DT[, .SD[.N], by = LOAN_ID]  # last row per id = max period

rm(DT); gc()

#############################  performance_data extraction ############################################

# Add the remaining 9 variables some how here.
### Prepare the Performance variables
performance_columns <- c("LOAN_ID", "ACT_PERIOD", "SERVICER", "CURR_RATE", "CURRENT_UPB",
 "LOAN_AGE", "REM_MONTHS", "ADJ_REM_MONTHS", "MATR_DT",
 "DLQ_STATUS", "MOD_FLAG", "Zero_Bal_Code", "ZB_DTE", "LAST_PAID_INSTALLMENT_DATE",
 "FORECLOSURE_DATE", "DISPOSITION_DATE", "FORECLOSURE_COSTS", "PROPERTY_PRESERVATION_AND_REPAIR_COSTS", "ASSET_RECOVERY_COSTS",
 "MISCELLANEOUS_HOLDING_EXPENSES_AND_CREDITS", "ASSOCIATED_TAXES_FOR_HOLDING_PROPERTY", "NET_SALES_PROCEEDS", "CREDIT_ENHANCEMENT_PROCEEDS", "REPURCHASES_MAKE_WHOLE_PROCEEDS",
 "OTHER_FORECLOSURE_PROCEEDS", "NON_INTEREST_BEARING_UPB", "PRINCIPAL_FORGIVENESS_AMOUNT", "LAST_UPB")

# indices of those columns in the raw file
sel_idx <- match(performance_columns, lppub_column_names)
if (anyNA(sel_idx)) {
  stop("These requested columns were not found in sf_column_names.txt: ",
       paste(acq_cols[is.na(sel_idx)], collapse = ", "))
}

types_selected <- lppub_column_classes[sel_idx]     # e.g. "character","numeric",...
cc_list <- split(sel_idx, types_selected) 

# Read and Select the performance columns from the original data table
performanceData <-  fread(
  cmd = cmd,
  sep = "|",
  header    = FALSE,          # raw file has no header
  select    = sel_idx,        # select by position to avoid name-mismatch
  colClasses  = cc_list,    # force types by those names
  na.strings = c("", "NA"),
  showProgress = TRUE
)
setnames(performanceData, names(performanceData), performance_columns)
 
# assign the proper names to just the selected columns
setnames(performanceData, 
         old = c(
           "ACT_PERIOD", "SERVICER", "CURR_RATE", "CURRENT_UPB",
           "LOAN_AGE", "REM_MONTHS", "ADJ_REM_MONTHS", "MATR_DT",
           "DLQ_STATUS", "MOD_FLAG", "Zero_Bal_Code", "ZB_DTE",
           "LAST_PAID_INSTALLMENT_DATE", "FORECLOSURE_DATE", "DISPOSITION_DATE",
           "FORECLOSURE_COSTS", "PROPERTY_PRESERVATION_AND_REPAIR_COSTS", "ASSET_RECOVERY_COSTS",
           "MISCELLANEOUS_HOLDING_EXPENSES_AND_CREDITS", "ASSOCIATED_TAXES_FOR_HOLDING_PROPERTY",
           "NET_SALES_PROCEEDS", "CREDIT_ENHANCEMENT_PROCEEDS", "REPURCHASES_MAKE_WHOLE_PROCEEDS",
           "OTHER_FORECLOSURE_PROCEEDS", "NON_INTEREST_BEARING_UPB", "PRINCIPAL_FORGIVENESS_AMOUNT",
           "LAST_UPB"
         ),
          new = c(
          "period", "servicer", "curr_rte", "curr_upb",
           "loan_age", "rem_mths", "adj_rem_months", "maturity_date",
           "dlq_status", "mod_ind", "z_zb_code", "zb_date",
           "lpi_dte", "fcc_dte", "disp_dte",
           "FCC_COST", "PP_COST", "AR_COST",
           "IE_COST", "TAX_COST",
           "NS_PROCS", "CE_PROCS", "RMW_PROCS",
           "O_PROCS", "non_int_upb", "prin_forg_upb",
           "zb_upb"
         ),
         skip_absent = TRUE)

performanceData[, `:=`(
  period = to_yyyymm(period),
  zb_date = to_yyyymm(zb_date),
  lpi_dte  = to_yyyymm(lpi_dte),
  fcc_dte  = to_yyyymm(fcc_dte),
  disp_dte  = to_yyyymm(disp_dte)
)]

performanceData[, maturity_date := 
                  paste0(substr(maturity_date, 3, 6), "-",   # YYYY
                         substr(maturity_date, 1, 2), "-",   # MM
                         "01")]                              # fixed day

#############################  Other Feature extraction ############################################

### Create AQSN_DTE field from filename
acquisition_year <- substr(FileName, 1, 4)
acquisition_qtr <- substr(FileName, 5, 7)
if(acquisition_qtr == 'Q1'){
  acquisition_month <- '03'
} else if(acquisition_qtr == 'Q2'){
  acquisition_month <- '06'
} else if(acquisition_qtr == 'Q3'){
  acquisition_month <- '09'
} else {
  acquisition_month <- '12'
}
acquisition_date <- paste(acquisition_year, acquisition_month, '01', sep = "-")

### Examining the different base tables can be helpful for modifying and debugging this code
### Create first base table with a copy of acquisition fields plus AQSN_DTE field and recordes of MI_TYPE and OCLTV
acquisition_data <- acquisition_data %>%
  mutate(
    AQSN_DTE = acquisition_date,
    MI_TYPE = case_when(
      MI_TYPE == '1' ~ 'BPMI', #MI_TYPE is recorded to be more descriptive
      MI_TYPE == '2' ~ 'LPMI',
      MI_TYPE == '3' ~ 'IPMI',
      TRUE ~ 'None'
    ),
    ocltv = if_else(is.na(ocltv), oltv, ocltv) #If OCLTV is missing, we replace it with OLTV
  )

### Create the second base table with the latest-available or aggregated data from the Performance fields
last_upb_table <- performanceData %>% 
  group_by(LOAN_ID) %>% filter(period == max(period)) %>% ungroup() %>%   
  rename(LAST_ACTIVITY_DATE = period ) %>% 
  mutate(LAST_UPB = ifelse(!is.na(zb_upb), zb_upb, curr_upb)) %>%
  select(LAST_ACTIVITY_DATE, LOAN_ID, LAST_UPB)

last_rt_table <- performanceData %>%
  select(LOAN_ID, period, curr_rte) %>%
  filter(!is.na(curr_rte)) %>%
  group_by(LOAN_ID) %>% filter(period == max(period)) %>% ungroup() %>%
  rename(LAST_RT = curr_rte, LAST_RT_DATE = period) %>%
  mutate(LAST_RT = round(LAST_RT, 3))

zb_code_table <- performanceData %>%
  select(LOAN_ID, period, z_zb_code) %>%
  filter(z_zb_code != '') %>%
  group_by(LOAN_ID) %>% filter(period == max(period)) %>% ungroup() %>% 
  select(LOAN_ID, z_zb_code) %>%
  rename(zb_code = z_zb_code)

servicer_table <- performanceData %>%
  select(LOAN_ID, period, servicer) %>%
  filter(servicer != '') %>%
  group_by(LOAN_ID) %>% filter(period == max(period)) %>% ungroup() %>%
  select(LOAN_ID, SERVICER = servicer)

non_int_upb_table <- performanceData %>%
  group_by(LOAN_ID) %>%
  slice(n()-1) %>% 
  select(LOAN_ID, LS_NON_INT_UPB = non_int_upb) %>% 
  mutate(LS_NON_INT_UPB = if_else(is.na(LS_NON_INT_UPB), 0, LS_NON_INT_UPB))
  
baseTable2 <- acquisition_data %>%
  left_join(last_upb_table, by = c("LOAN_ID" = "LOAN_ID")) %>%
  left_join(last_rt_table, by = c("LOAN_ID" = "LOAN_ID")) %>%
  left_join(zb_code_table, by = c("LOAN_ID" = "LOAN_ID")) %>%
  left_join(servicer_table, by = c("LOAN_ID" = "LOAN_ID")) %>%
  left_join(non_int_upb_table, by = c("LOAN_ID" = "LOAN_ID"))

rm(last_upb_table)
rm(last_rt_table)
rm(zb_code_table)
rm(servicer_table)
rm(non_int_upb_table)
gc()

### Create the third base table with the latest-available forclosure/disposition data
fcc_table <- performanceData %>%
  filter(!is.na(lpi_dte) &  !is.na(fcc_dte) & !is.na(disp_dte)) %>%
  group_by(LOAN_ID) %>%
  summarize(
    LPI_DTE = max(lpi_dte),
    FCC_DTE = max(fcc_dte),
    DISP_DTE = max(disp_dte)
  )

baseTable3 <- baseTable2 %>%
  left_join(fcc_table, by = "LOAN_ID")

rm(baseTable2)
rm(fcc_table)

# taken care of - zb_upb, curr_rte, period, z_zb_code, servicer, non_int_upb, lpi_dte, fcc_dte, disp_dte

### computing loan status fields according to specific rules
costs_otherupbs_table <- performanceData %>% 
  group_by(LOAN_ID) %>% filter(period == max(period)) %>% ungroup() %>%
  select(LOAN_ID, prin_forg_upb, mod_ind, dlq_status, zb_date, FCC_COST,
         PP_COST, AR_COST, IE_COST, TAX_COST, NS_PROCS, CE_PROCS, RMW_PROCS, O_PROCS,) %>% 
  rename(PFG_COST = prin_forg_upb)

### Create the forth base table -Delinquency data, Final Closure Event data and modification data
slimperformanceData <- performanceData %>%
  select(LOAN_ID, period, dlq_status, z_zb_code, curr_upb, zb_upb, mod_ind, maturity_date, rem_mths) %>%
  mutate(
    dlq_status = if_else(dlq_status == 'XX', '999', dlq_status),
    dlq_status = as.numeric(dlq_status)
  )

rm(performanceData); gc();

consolidated_dlq_table <- slimperformanceData %>%
  mutate(dlq_status = if_else(dlq_status > 6, 6, dlq_status)) %>%  # Adjust dlq_status
  filter(dlq_status %in% c(1, 2, 3, 4, 6)) %>%  # Ensure only relevant dlq_status (1 to 4 and 6) 
  group_by(LOAN_ID, dlq_status) %>% filter(period == max(period)) %>% ungroup() %>%
  select(LOAN_ID, dlq_status, DTE = period, curr_upb) 

# Step 2: Pivot wider to create columns for each dlq_status and its corresponding DTE and curr_upb
# Note : This pivoting only concerns with inserting the data in 1 column, not all columns greater than
consolidated_dlq_table <- consolidated_dlq_table %>%
  pivot_wider(names_from = dlq_status, values_from = c(DTE, curr_upb), 
              names_glue = "F{dlq_status}_{.value}") 
# In the data analysis code , check whether each of the defaulted loans has at least 1 dlq code

fce_table <- slimperformanceData %>%
  filter((z_zb_code == '02' | z_zb_code == '03' | z_zb_code == '09' | z_zb_code == '15') | (dlq_status >= 6 & dlq_status < 999)) %>%
  group_by(LOAN_ID) %>% filter(period == min(period)) %>% ungroup() %>%
  select(LOAN_ID, FCE_DTE= period, curr_upb, zb_upb) %>%
  mutate(FCE_UPB = zb_upb + curr_upb) %>%
  select(LOAN_ID, FCE_DTE, FCE_UPB)

fmod_dte_table <- slimperformanceData %>%
  filter(mod_ind == "Y", is.na(z_zb_code)) %>%
  group_by(LOAN_ID) %>%
  summarize(FMOD_DTE =  min(period))

#this table contains maximum UPB in a period from 1st modification to 3 months after 1st modification
fmod_table <- slimperformanceData %>%
  filter(mod_ind == 'Y', is.na(z_zb_code)) %>%
  left_join(fmod_dte_table, by = c("LOAN_ID" = "LOAN_ID")) %>%
  mutate(period   = as.integer(period), FMOD_DTE = as.integer(FMOD_DTE)) %>%
  filter((period %/% 100)*12 + period %% 100  <=  (FMOD_DTE %/% 100)*12 + FMOD_DTE %% 100 + 3) %>%
  group_by(LOAN_ID) %>%
  slice_max(curr_upb, with_ties = FALSE) %>% ungroup() %>% # Keep only one row per LOAN_ID
  select(LOAN_ID, FMOD_DTE, FMOD_UPB = curr_upb, maturity_date)

rm(fmod_dte_table)

### Compute the number of months elapsed from origination to a loan becoming at least 120 days delinquent
num_120_table <- slimperformanceData %>%
  filter(dlq_status >= 4 & dlq_status < 999,is.na(z_zb_code)) %>%
  group_by(LOAN_ID) %>%
  summarize(F120_DTE = min(period)) %>%
  left_join(acquisition_data, by = 'LOAN_ID') %>%
  
  mutate(
    z_num_months_120 = ((F120_DTE %/% 100)*12 + F120_DTE %% 100 - ((FIRST_PAY %/% 100)*12 + FIRST_PAY %% 100) + 1)
  ) %>%
  select(LOAN_ID, z_num_months_120) 


### Compute MODTRM_CHNG field (a flag for whether the term(maturity) of a loan was changed as part of a loan modification)
orig_maturity_table <- slimperformanceData %>%
  select(LOAN_ID, period, maturity_date) %>%
  filter(!is.na(maturity_date)) %>%
  group_by(LOAN_ID) %>% filter(period == min(period)) %>% ungroup() %>%
  rename(orig_maturity_date = maturity_date)

## we can only get the change of maturity from remaining months
trm_chng_table <- slimperformanceData %>%
  group_by(LOAN_ID) %>%
  mutate(
    trm_chng = rem_mths - lag(rem_mths, order_by = period)  # Directly calculate the term change
  ) %>%
  filter(trm_chng >= 0) %>% 
  group_by(LOAN_ID) %>%  # because there might me multiple term changes, we want the first one
  summarize(trm_chng_dt = min(period))

modtrm_table <- fmod_table %>%
  left_join(orig_maturity_table, by = 'LOAN_ID') %>%
  left_join(trm_chng_table, by = 'LOAN_ID') %>%
  mutate(MODTRM_CHNG = if_else(maturity_date != orig_maturity_date | !is.na(trm_chng_dt), 1, 0)) %>%
  select(LOAN_ID, MODTRM_CHNG)

rm(orig_maturity_table)
rm(trm_chng_table)
rm(acquisition_data)

### Compute MODTRM_UPB field (a flag for whether the balance of a loan was changed as part of a loan modification)
pre_mod_upb_table <- slimperformanceData %>%
  select(LOAN_ID, period, curr_upb) %>%
  left_join(fmod_table, by = 'LOAN_ID') %>%
  filter(period < FMOD_DTE) %>%
  group_by(LOAN_ID) %>% filter(period == max(period)) %>% ungroup() %>%
  rename(pre_mod_period = period, pre_mod_upb = curr_upb)

rm(slimperformanceData)

#compute the change in UPB rather than a flag of whether it was changed or not.
modupb_table <- pre_mod_upb_table %>%
  mutate(MODUPB_CHNG = if_else(FMOD_UPB >= pre_mod_upb, 1, 0)) %>%
  select(LOAN_ID, MODUPB_CHNG )

rm(pre_mod_upb_table)

### Create the fourth base table by joining the first-DQ-occurence and loan modification tables
baseTable4 <- baseTable3 %>%
  left_join(consolidated_dlq_table, by = 'LOAN_ID') %>%
  left_join(fce_table, by = 'LOAN_ID') %>%
  left_join(fmod_table, by = 'LOAN_ID') %>%
  left_join(num_120_table, by = 'LOAN_ID') %>%
  left_join(modtrm_table, by = 'LOAN_ID') %>%
  left_join(modupb_table, by = 'LOAN_ID') %>%
  mutate(
    F1_curr_upb = if_else(!is.na(F1_DTE) & F1_curr_upb==0, orig_amt, F1_curr_upb),
    F2_curr_upb = if_else(!is.na(F2_DTE) & F2_curr_upb==0, orig_amt, F2_curr_upb),
    F3_curr_upb = if_else(!is.na(F3_DTE) & F3_curr_upb==0, orig_amt, F3_curr_upb),
    F4_curr_upb = if_else(!is.na(F4_DTE) & F4_curr_upb==0, orig_amt, F4_curr_upb),
    F6_curr_upb = if_else(!is.na(F6_DTE) & F6_curr_upb==0, orig_amt, F6_curr_upb),
    FCE_UPB = if_else(is.na(FCE_UPB) & !is.na(FCE_DTE), orig_amt, FCE_UPB)
  )

rm(baseTable3)
rm(consolidated_dlq_table)
rm(fce_table)
rm(fmod_table)
rm(num_120_table)
rm(modtrm_table)
rm(modupb_table)

# dlq_status, z_zb_code, curr_upb, zb_upb, mod_ind, maturity_date, rem_mths should be taken care of
# dlq_status - +10 rows, zb_upb, curr_upb - 1 row , mod_ind, curr_upb for max(3 mm..)


### Create the fifth base table
baseTable5 <- baseTable4 %>%
  left_join(costs_otherupbs_table, by = 'LOAN_ID') %>%
  mutate(
    LAST_DTE = if_else(!is.na(DISP_DTE), DISP_DTE, LAST_ACTIVITY_DATE),
    MOD_FLAG = if_else(!is.na(FMOD_DTE), 1, 0),
    MODFG_COST = if_else(mod_ind == 'Y' & PFG_COST > 0, PFG_COST, 0),
    MODTRM_CHNG = if_else(is.na(MODTRM_CHNG), 0, MODTRM_CHNG),
    MODUPB_CHNG = if_else(is.na(MODUPB_CHNG), 0, MODUPB_CHNG),
    CSCORE_MN = if_else(!is.na(CSCORE_C) & CSCORE_C < CSCORE_B, CSCORE_C, CSCORE_B),
    CSCORE_MN = if_else(is.na(CSCORE_MN), CSCORE_B, CSCORE_MN),
    CSCORE_MN = if_else(is.na(CSCORE_MN), CSCORE_C, CSCORE_MN),
    ORIG_VAL = round(orig_amt/(oltv/100), digits = 2),
    dlq_status = if_else(dlq_status == 'X' | dlq_status == 'XX', '999', dlq_status),
    dlq_status = as.numeric(dlq_status),
    
    FCC_DTE = if_else(FCC_DTE == 0 & (zb_code %in% c('09', '03', '02', '15')), zb_date, FCC_DTE),
    COMPLT_FLG = if_else(DISP_DTE != 0, 1, 0),
    COMPLT_FLG = if_else(!(zb_code %in% c('09', '03', '02', '15')), NA_real_, COMPLT_FLG),
    FCC_COST = if_else(COMPLT_FLG == 1 & is.na(FCC_COST), 0, FCC_COST),
    PP_COST = if_else(COMPLT_FLG == 1 & is.na(PP_COST), 0, PP_COST),
    AR_COST = if_else(COMPLT_FLG == 1 & is.na(AR_COST), 0, AR_COST),
    IE_COST = if_else(COMPLT_FLG == 1 & is.na(IE_COST), 0, IE_COST),
    TAX_COST = if_else(COMPLT_FLG == 1 & is.na(TAX_COST), 0, TAX_COST),
    PFG_COST = if_else(COMPLT_FLG == 1 & is.na(PFG_COST), 0, PFG_COST),
    CE_PROCS = if_else(COMPLT_FLG == 1 & is.na(CE_PROCS), 0, CE_PROCS),
    NS_PROCS = if_else(COMPLT_FLG == 1 & is.na(NS_PROCS), 0, NS_PROCS),
    RMW_PROCS = if_else(COMPLT_FLG == 1 & is.na(RMW_PROCS), 0, RMW_PROCS),
    O_PROCS = if_else(COMPLT_FLG == 1 & is.na(O_PROCS), 0, O_PROCS),
    INT_COST = round(if_else(COMPLT_FLG == 1 & LPI_DTE != '', (( (LAST_DTE %/% 100)*12 + LAST_DTE %% 100 - ((LPI_DTE %/% 100)*12 + LPI_DTE %% 100) ) * (((LAST_RT / 100) - 0.0035) / 12) * (LAST_UPB  - LS_NON_INT_UPB)), NA_real_), digits = 2),
    INT_COST = if_else(COMPLT_FLG == 1 & is.na(INT_COST), 0, INT_COST),
    NET_LOSS = round(if_else(COMPLT_FLG == 1, (LAST_UPB + FCC_COST + PP_COST + AR_COST + IE_COST + TAX_COST + PFG_COST + INT_COST -(NS_PROCS + CE_PROCS + RMW_PROCS + O_PROCS)), NA_real_), digits = 2)
  )

# unter the gun, DISP_DTE, LAST_ACTIVITY_DATE
rm(baseTable4)
rm(costs_otherupbs_table)

#check what all formatting needs to be done after basetable4 to create basetable5?
#date columns formatting -
baseTable5 <- baseTable5 %>%
  mutate(ORIG_DATE = if_else(!is.na(ORIG_DATE), paste(as.character(ORIG_DATE %/% 100), as.character(ORIG_DATE %% 100), '01', sep = '-'), ''),
     FIRST_PAY = if_else(!is.na(FIRST_PAY), paste(as.character(FIRST_PAY %/% 100), as.character(FIRST_PAY %% 100), '01', sep = '-'), ''),
     LPI_DTE = if_else(!is.na(LPI_DTE), paste(as.character(LPI_DTE %/% 100), as.character(LPI_DTE %% 100), '01', sep = '-'), ''),
     FCC_DTE = if_else(!is.na(FCC_DTE), paste(as.character(FCC_DTE %/% 100), as.character(FCC_DTE %% 100), '01', sep = '-'), ''),
     DISP_DTE = if_else(!is.na(DISP_DTE), paste(as.character(DISP_DTE %/% 100), as.character(DISP_DTE %% 100), '01', sep = '-'), ''),
     FCE_DTE = if_else(!is.na(FCE_DTE), paste(as.character(FCE_DTE %/% 100), as.character(FCE_DTE %% 100), '01', sep = '-'), ''),
     F1_DTE = if_else(!is.na(F1_DTE), paste(as.character(F1_DTE %/% 100), as.character(F1_DTE %% 100), '01', sep = '-'), ''),
     F2_DTE = if_else(!is.na(F2_DTE), paste(as.character(F2_DTE %/% 100), as.character(F2_DTE %% 100), '01', sep = '-'), ''),
     F3_DTE = if_else(!is.na(F3_DTE), paste(as.character(F3_DTE %/% 100), as.character(F3_DTE %% 100), '01', sep = '-'), ''),
     F4_DTE = if_else(!is.na(F4_DTE), paste(as.character(F4_DTE %/% 100), as.character(F4_DTE %% 100), '01', sep = '-'), ''),
     F6_DTE = if_else(!is.na(F6_DTE), paste(as.character(F6_DTE %/% 100), as.character(F6_DTE %% 100), '01', sep = '-'), ''),
     FMOD_DTE = if_else(!is.na(FMOD_DTE), paste(as.character(FMOD_DTE %/% 100), as.character(FMOD_DTE %% 100), '01', sep = '-'), ''),
     LAST_DTE = if_else(!is.na(LAST_DTE), paste(as.character(LAST_DTE %/% 100), as.character(LAST_DTE %% 100), '01', sep = '-'), '')
 ) %>% 
  select(-FCC_COST, -PP_COST, -AR_COST, -IE_COST, -TAX_COST, -NS_PROCS, -CE_PROCS, -RMW_PROCS, -O_PROCS, -PFG_COST, -LAST_ACTIVITY_DATE, -LAST_RT_DATE)


### Export the dataframe as a .csv
baseTable5 %>%
  dplyr::mutate_if(is.double, function(x) dplyr::if_else(is.na(x), NA_character_, format(x, scientific = FALSE, drop0trailing = TRUE, trim = TRUE))) %>%
  data.table::fwrite(paste0("../preprocessed_data/", FileName, "_stat.csv"), sep = ",", na = "NULL", logical01 = TRUE, quote = TRUE, scipen = 100, col.names = TRUE, row.names = FALSE)

rm(baseTable5)
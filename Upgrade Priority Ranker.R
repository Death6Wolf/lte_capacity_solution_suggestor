#-----------------------------------------------------------------------------#
#           Script to calculate upgrade priority                              #
#                      for 4G and 5G                                          #
#                       2023-06-07                                            #
#                                                                             #
#-----------------------------------------------------------------------------#

# Libraries --------------------------------------------------------------

library(tidyverse)
library(janitor)
library(glue)
library(lubridate)
library(vroom)
library(dtplyr)
library(tictoc)
library(DBI)
library(lubridate)

# Parameters --------------------------------------------------------------

tdd_adjustment <- 0.743
l2600_penalty <- 0.5
prb_congestion_threshold <- 0.6
nr_penetration_rate <- 0
days_in_year <- as.integer(ceiling_date(Sys.Date(), 'year')-floor_date(Sys.Date(), 'year'))
days_left <- as.integer(ceiling_date(Sys.Date(), 'year')- Sys.Date())
growth_rate <- 1.34
scaled_growth_rate <- (1.34^(1/days_in_year))^days_left #Yearly growth rate converted to monthly compound to the power of the number of months forecasted

# Get VW_MAPS Info --------------------------------------------------------

## Need to automate this!!!!!!!! --------------------------------------------
discrepancies_3g <- vroom("//mtndata/Network Group/OpsData/PlanOpt/Users/Rudi/R/Upgrade Recommendation/discrepancies_3g.csv") |> 
  clean_names() |> 
  select(cell_name = x3g_wcell_name) |> 
  mutate(discrepancy = TRUE)

discrepancies_4g <- vroom("//mtndata/Network Group/OpsData/PlanOpt/Users/Rudi/R/Upgrade Recommendation/discrepancies_4g.csv") |> 
  clean_names() |> 
  select(cell_name = x4g_cell_name) |> 
  mutate(discrepancy = TRUE)

discrepancies <- bind_rows(discrepancies_3g, discrepancies_4g)

nims_con <- dbConnect(odbc::odbc(), "NMSDB_MNS_FWA", UID = "gis", 
                      PWD = "gis")

nims_query <- "
Select 
  distinct
  cell_name, 
  site_id, 
  regexp_substr(cell_name, '[1-9][0-9]*[A-Z]*') as sector, 
  plan_status, 
  ops_status, 
  carrier, 
  frequency_band, 
  technology,
  carrier_owner,
  case when technology = 'UMTS' then '5' 
  else regexp_substr(carrier , '[0-9]*$')
  end as bandwidth,
  antenna_latitude as latitude,
  antenna_longitude as longitude
from 
  mtnis.vw_maps_db_interface
where
  technology IN ('UMTS', 'LTE', 'NR')
  --and plan_status = 'Operational'
"

nims_data <- dbGetQuery(nims_con, nims_query) |> 
  collect() |> 
  clean_names() |> 
  mutate(
    bandwidth = parse_integer(bandwidth),
    dl_bw = if_else(frequency_band == '2600', bandwidth * tdd_adjustment * l2600_penalty, bandwidth),
    dl_bw = if_else(frequency_band == '3500', bandwidth * tdd_adjustment, dl_bw)
  ) |> 
  left_join(discrepancies, by = "cell_name") |> 
  mutate(plan_status = if_else(!is.na(discrepancy), 'Operational', plan_status)) |> 
  filter(plan_status == 'Operational')

dbDisconnect(nims_con)

## sector planning info ------------------------------------------

# Sector total lte bw
sector_planned_info_lte <- 
  nims_data |> 
  lazy_dt() |> 
  filter(technology == 'LTE') |> 
  group_by(sector) |> 
  summarise(
    lte_dl_bw_total = sum(dl_bw, na.rm = TRUE),
    lte_dl_bw_800 = sum(dl_bw[frequency_band == '800'], na.rm = TRUE),
    lte_dl_bw_900 = sum(dl_bw[frequency_band == '900'], na.rm = TRUE),
    lte_dl_bw_1800 = sum(dl_bw[frequency_band == '1800'], na.rm = TRUE),
    lte_dl_bw_2100 = sum(dl_bw[frequency_band == '2100'], na.rm = TRUE),
    lte_dl_bw_2600 = sum(dl_bw[frequency_band == '2600'], na.rm = TRUE),
    lte_dl_bw_cellc =  sum(dl_bw[carrier_owner == 'CELLC'], na.rm = TRUE) + sum(carrier_owner == 'MTN_CELLC', na.rm = TRUE) * 5,
    lte_dl_bw_cellc_1800 = sum(dl_bw[frequency_band == '1800' & carrier_owner == 'CELLC'], na.rm = TRUE),
    lte_dl_bw_mtn =  sum(dl_bw[carrier_owner == 'MTN'], na.rm = TRUE) + 
      sum(carrier_owner == 'MTN_CELLC', na.rm = TRUE) * 15 +
      sum(carrier_owner == 'MTN_LIQUID', na.rm = TRUE) * 10,
    lte_dl_bw_mtn_2100 = sum(dl_bw[carrier_owner == 'MTN' & frequency_band == '2100'], na.rm = TRUE) + sum(carrier_owner == 'MTN_CELLC', na.rm = TRUE) * 15,
    lte_dl_bw_liquid = sum(dl_bw[carrier_owner == 'MTN_LIQUID'], na.rm = TRUE) -
      sum(carrier_owner == 'MTN_LIQUID', na.rm = TRUE) * 10 +
      sum(carrier_owner == 'LIQUID', na.rm = TRUE)
  ) |> 
  as_tibble()

# Sector umts bw
sector_planned_info_umts <- 
  nims_data |> 
  lazy_dt() |> 
  filter(technology == 'UMTS') |> 
  group_by(sector) |> 
  summarise(
    umts_dl_bw_total = sum(dl_bw, na.rm = TRUE),
    umts_dl_bw_900 = sum(dl_bw[frequency_band == '900'], na.rm = TRUE),
    umts_dl_bw_2100 = sum(dl_bw[frequency_band == '2100'], na.rm = TRUE),
    umts_dl_bw_cellc =  sum(dl_bw[carrier_owner == 'CELLC'], na.rm = TRUE),
    umts_dl_bw_cc_2100 =  sum(dl_bw[carrier_owner == 'CELLC' & frequency_band == '2100'], na.rm = TRUE),
    umts_dl_bw_mtn =  sum(dl_bw[carrier_owner == 'MTN'], na.rm = TRUE)
  ) |> 
  as_tibble()

# Sector nr bw
sector_planned_info_nr <- 
  nims_data |> 
  lazy_dt() |> 
  filter(technology == 'NR') |> 
  group_by(sector) |> 
  summarise(
    nr_dl_bw_total = sum(dl_bw, na.rm = TRUE),
    nr_dl_bw_1800 = sum(dl_bw[frequency_band == '1800'], na.rm = TRUE),
    nr_dl_bw_2100 = sum(dl_bw[frequency_band == '2100'], na.rm = TRUE),
    nr_dl_bw_3500 = sum(dl_bw[frequency_band == '3500'], na.rm = TRUE),
    nr_dl_bw_28000 = sum(dl_bw[frequency_band == '28000'], na.rm = TRUE)
  ) |> 
  as_tibble()

# Combine spectrum info
sector_planned_info <- sector_planned_info_lte |> 
  left_join(sector_planned_info_nr, by = 'sector') |> 
  left_join(sector_planned_info_umts, by = 'sector') |> 
  mutate(across(where(is.numeric), ~replace_na(., 0))) |> 
  mutate(
    lte_dl_bw_cc_shutdown = 5*(lte_dl_bw_900 >= 10) + lte_dl_bw_total - lte_dl_bw_cellc - lte_dl_bw_mtn_2100 + pmin(15 - umts_dl_bw_2100 - nr_dl_bw_2100, lte_dl_bw_mtn_2100),
  )

# Get performance data ---------------------------------------------------------------

x4g_path <- "D:/Radio_Network_Planning/bh_query_automation/lte/"
x4g_files <- list.files(x4g_path)

## Read 4G files -----------------------------------------------------------

setwd(x4g_path)
df_4g <- vroom(x4g_files,
               num_threads = 14)

date_stamp <- max(df_4g$startday)

## LTE Performance ------------

sector_prb <- df_4g |> 
  lazy_dt() |> 
  filter(startday >= as.numeric(ymd(date_stamp)- 180), startday <= 20230609) |> 
  select(cell_name, startday, sector, nr_used_pdsch_prbs, nr_avail_dl_prbs) |> 
  arrange(cell_name, startday) |> 
  mutate(
    nr_used_pdsch_prbs = if_else(str_sub(cell_name, -1, -1) %in% c("5", "6"), nr_used_pdsch_prbs*tdd_adjustment*l2600_penalty, nr_used_pdsch_prbs),
    nr_avail_dl_prbs   = if_else(str_sub(cell_name, -1, -1) %in% c("5", "6"), nr_avail_dl_prbs *tdd_adjustment*l2600_penalty, nr_avail_dl_prbs)
  ) |> 
  group_by(sector, startday) |> 
  summarise(
    #n = n(), 
    nr_used_pdsch_prbs = sum(nr_used_pdsch_prbs, na.rm = TRUE),
    nr_avail_dl_prbs = sum(nr_avail_dl_prbs, na.rm = TRUE),
    .groups = "drop"
  ) |> 
  mutate(
    daily_bh_prb_util = nr_used_pdsch_prbs/nr_avail_dl_prbs,
    prb21 = if_else(as.numeric(ymd(date_stamp) - ymd(startday)) <= 21, daily_bh_prb_util, NA_real_),
    prb42 = if_else(as.numeric(ymd(date_stamp) - ymd(startday)) <= 42, daily_bh_prb_util, NA_real_),
    prb64 = if_else(as.numeric(ymd(date_stamp) - ymd(startday)) <= 64, daily_bh_prb_util, NA_real_)
  ) |> 
  group_by(sector) |> 
  summarise(
    p75_all = quantile(daily_bh_prb_util, .75, na.rm = TRUE, names = FALSE),
    p75_21   = quantile(prb21, .75, na.rm = TRUE, names = FALSE),
    p75_42  = quantile(prb42, .75, na.rm = TRUE, names = FALSE),
    p75_64  = quantile(prb64, .75, na.rm = TRUE, names = FALSE),
    .groups = "drop"
  ) |> 
  mutate(
    p75_prb_utilisation = coalesce(p75_21, p75_42, p75_64, p75_all),
    p75_forecast_eoy = p75_prb_utilisation * scaled_growth_rate
  ) |> 
  select(
    sector, 
    p75_prb_utilisation,
    p75_forecast_eoy
  ) |> 
  as_tibble()


# Join planned and live data ----------------------------------------------

sector_info <- sector_planned_info |> 
  full_join(sector_prb, by = 'sector') 

# Calculate congestion and other metrics ----------------------------------------------------

# Normal ------------------------------------------------------------------

sector_info_c <- sector_info |> 
  mutate(prb_congestion_threshold = prb_congestion_threshold) |> 
  mutate(
    x4g_cong = (p75_prb_utilisation > prb_congestion_threshold),
    x4g_cong_eoy = (p75_forecast_eoy  > prb_congestion_threshold)
  )

# Find utilisation for available upgrades ---------------------------------

## Functions ---------------------------------------------------------------

congestion_recalculation <- function(total_dl_bw, 
                                     util_col, 
                                     cong_indicator, 
                                     band_col, 
                                     band_bw_thresh, 
                                     new_dl_bw){
  result <- if_else(
    (cong_indicator) & (band_col < band_bw_thresh),
    (util_col * total_dl_bw) / (total_dl_bw - band_col + new_dl_bw),
    util_col
  )
  
  return(result)
}

dl_bw_recalculation <- function(total_dl_bw, 
                                util_col, 
                                cong_indicator, 
                                band_col, 
                                band_bw_thresh, 
                                new_dl_bw){
  result <- if_else(
    (cong_indicator) & (band_col < band_bw_thresh),
    (total_dl_bw - band_col + new_dl_bw),
    total_dl_bw
  )
  
  return(result)
}

suggestion <- function(         cong_indicator, 
                                band_col, 
                                band_bw_thresh){
  result <- if_else(
    (cong_indicator) & (band_col < band_bw_thresh),
    TRUE,
    FALSE
  )
  
  return(result)
}

find_solutions <- function(sector_info, p75_forecast){
  solution_df <- sector_info |>  
    mutate(
      # Check if L1800 20MHz is deployed
      upg_l1800_20 = if_else(suggestion(1, lte_dl_bw_1800 - lte_dl_bw_cellc_1800, 20), 'L1800 20MHz', ''),
      upg_l1800_20_util = congestion_recalculation(lte_dl_bw_total, {{p75_forecast}}, 1, lte_dl_bw_1800 - lte_dl_bw_cellc_1800, 20, 20),
      new_lte_dl_bw_total = dl_bw_recalculation(lte_dl_bw_total, {{p75_forecast}}, 1, lte_dl_bw_1800 - lte_dl_bw_cellc_1800, 20, 20),
      
      # Check if L900 10MHz is deployed
      upg_l900_10 = if_else(suggestion(1, lte_dl_bw_900, 10), 'L900 10MHz', ''),
      upg_l900_10_util = congestion_recalculation(new_lte_dl_bw_total, upg_l1800_20_util, 1, lte_dl_bw_900, 10, 10),
      new_lte_dl_bw_total = dl_bw_recalculation(new_lte_dl_bw_total, upg_l1800_20_util, 1, lte_dl_bw_900, 10, 10),

      # Check if L2100 20MHz is deployed, make provision for DSS2100
      upg_l2100_20 = if_else(suggestion(1, lte_dl_bw_2100, 20 - nr_dl_bw_2100), str_glue('L2100 {20 - nr_dl_bw_2100}MHz'), '') ,
      upg_l2100_20_util = congestion_recalculation(new_lte_dl_bw_total, upg_l900_10_util, 1, lte_dl_bw_2100, 20 - nr_dl_bw_2100, 20 - nr_dl_bw_2100),
      new_lte_dl_bw_total = dl_bw_recalculation(new_lte_dl_bw_total, upg_l900_10_util, 1, lte_dl_bw_2100, 20 - nr_dl_bw_2100, 20 - nr_dl_bw_2100),

      # Check if L2600 is deployed
      upg_l2600_40 = if_else(suggestion(1, lte_dl_bw_2600, 40 * tdd_adjustment * l2600_penalty), 'L2600 40MHz', ''),
      upg_l2600_40_util = congestion_recalculation(new_lte_dl_bw_total, upg_l2100_20_util, 1, lte_dl_bw_2600, 40 * tdd_adjustment * l2600_penalty, 40 * tdd_adjustment * l2600_penalty),
      new_lte_dl_bw_total = dl_bw_recalculation(new_lte_dl_bw_total, upg_l2100_20_util, 1, lte_dl_bw_2600, 40 * tdd_adjustment * l2600_penalty, 40 * tdd_adjustment * l2600_penalty),

      # Check if L1800 MORAN is deployed
      upg_l1800_moran = if_else(suggestion(1, lte_dl_bw_cellc_1800, 10), 'L1800 MORAN 10MHz', ''),
      upg_l1800_moran_util = congestion_recalculation(new_lte_dl_bw_total, upg_l2600_40_util, 1, lte_dl_bw_cellc_1800, 10, 10),
      new_lte_dl_bw_total = dl_bw_recalculation(new_lte_dl_bw_total, upg_l2600_40_util, 1, lte_dl_bw_cellc_1800, 10, 10),

      # Check if L800 10MHz is deployed
      upg_l800_10 = if_else(suggestion(1, lte_dl_bw_800, 10), 'L800 10MHz', ''),
      upg_l800_10_util = congestion_recalculation(new_lte_dl_bw_total, upg_l1800_moran_util, 1, lte_dl_bw_800, 10, 10),
      new_lte_dl_bw_total = dl_bw_recalculation(new_lte_dl_bw_total, upg_l1800_moran_util, 1, lte_dl_bw_800, 10, 10),

      # Check if n3500 is deployed, assume only nr_penetration_rate % will be deloaded. Non atm so excluded
      upg_n3500_40 = if_else(suggestion(1, nr_dl_bw_3500, 40 * tdd_adjustment), 'N3500 40MHz', ''),

      # Check if additional spectrum is required
      add_more_spectrum = if_else(upg_l800_10_util > prb_congestion_threshold, 'Add more spectrum', '')
    ) |> 
    mutate(
      upg_l1800_20_action = upg_l1800_20 != "",
      upg_l900_10_action = upg_l900_10 != "",
      upg_l2100_20_action = upg_l2100_20 != "",
      upg_l2600_40_action = upg_l2600_40 != "",
      upg_l1800_moran_action = upg_l1800_moran != "",
      upg_l800_10_action = upg_l800_10 != "",
      upg_n3500_40_action = upg_n3500_40 != "",
      add_more_spectrum_action = add_more_spectrum != "",
    ) |> 
    arrange(-upg_l1800_20_util) |> 
    group_by(upg_l1800_20_action) |> 
    mutate(upg_l1800_20_rank = row_number()) |> 
    ungroup() |> 
    arrange(-upg_l900_10_util) |> 
    group_by(upg_l900_10_action) |> 
    mutate(upg_l900_10_rank = row_number()) |> 
    ungroup()  |> 
    arrange(-upg_l2100_20_util) |> 
    group_by(upg_l2100_20_action) |> 
    mutate(upg_l2100_20_rank = row_number()) |> 
    ungroup()   |> 
    arrange(-upg_l2600_40_util) |> 
    group_by(upg_l2600_40_action) |> 
    mutate(upg_l2600_40_rank = row_number()) |> 
    ungroup()  |> 
    arrange(-upg_l1800_moran_util) |> 
    group_by(upg_l1800_moran_action) |> 
    mutate(upg_l1800_moran_rank = row_number()) |> 
    ungroup()  |> 
    arrange(-upg_l800_10_util) |> 
    group_by(upg_l800_10_action) |> 
    mutate(upg_l800_10_rank = row_number()) |> 
    ungroup()   |> 
    group_by(upg_n3500_40_action) |> 
    mutate(upg_n3500_40_rank = row_number()) |> 
    ungroup()   |> 
    group_by(add_more_spectrum_action) |> 
    mutate(add_more_spectrum_rank = row_number()) |> 
    ungroup() |> 
    mutate(
      upg_l1800_20_rank = if_else(upg_l1800_20_action, upg_l1800_20_rank, NA_real_),
      upg_l900_10_rank = if_else(upg_l900_10_action, upg_l900_10_rank, NA_real_),
      upg_l2100_20_rank = if_else(upg_l2100_20_action, upg_l2100_20_rank, NA_real_),
      upg_l2600_40_rank = if_else(upg_l2600_40_action, upg_l2600_40_rank, NA_real_),
      upg_l1800_moran_rank = if_else(upg_l1800_moran_action, upg_l1800_moran_rank, NA_real_),
      upg_l800_10_rank = if_else(upg_l800_10_action, upg_l800_10_rank, NA_real_),
      upg_n3500_40_rank = if_else(upg_n3500_40_action, upg_n3500_40_rank, NA_real_),
      add_more_spectrum_rank = if_else(add_more_spectrum_action, add_more_spectrum_rank, NA_real_),
    )
    
  return(solution_df)
}

# Get solutions for sectors -----------------------------------------------

solution_df_eoy <- find_solutions(sector_info_c, p75_forecast_eoy)

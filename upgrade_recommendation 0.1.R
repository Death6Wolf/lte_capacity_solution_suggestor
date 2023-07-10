#-----------------------------------------------------------------------------#
#           Script to calculate site and sector upgrade requirements          #
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

# Parameters --------------------------------------------------------------

tdd_adjustment <- 0.743
l2600_penalty <- 0.6
# prb_congestion_threshold <- 0.6
# prb_congestion_threshold_excl_l2600 <- 0.6
# throughput_congestion_threshold <- 1e12
# throughput_congestion_threshold_excl_l2600 <- 1e12
nr_penetration_rate <- 0
# cc_closure_carry_over_factor <- 0.4
traffic_increase_upg_prop <- 1 # Not implemented, make it variable for congestion_recalculation()?
monthly_growth_rate_1 <- (1.34^(1/12))^6 #Yearly growth rate converted to monthly compound to the power of the number of months forecasted
monthly_growth_rate_2 <- (1.36^(1/12))^12 #to the power of the number of months forecasted
monthly_growth_rate_3 <- (1.36^(1/12))^12 #to the power of the number of months forecasted
monthly_growth_rate_4 <- (1.36^(1/12))^12 #to the power of the number of months forecasted

# Functions ---------------------------------------------------------------

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

# Set paths ---------------------------------------------------------------

x4g_path <- "D:/Radio_Network_Planning/bh_query_automation/lte/"
x4g_files <- list.files(x4g_path)

# Read 4G files -----------------------------------------------------------

setwd(x4g_path)
df_4g <- vroom(x4g_files,
               num_threads = 14)

date_stamp <- max(df_4g$startday)

# Get VW_MAPS Info --------------------------------------------------------

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

lte_operational_cells <- nims_data |> 
  filter(technology == 'LTE') |> 
  count(site_id, plan_status) |> 
  pivot_wider(id_cols = site_id, names_from = plan_status, values_from = n) |> 
  mutate(site_id = as.character(site_id))

# Calculate LTE Sector Performance ----------------------------------------

## Base Performance ------------

tictoc::tic()
sector_prb <- df_4g |> 
  lazy_dt() |> 
  filter(startday >= as.numeric(ymd(date_stamp)- 180)) |> 
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
    prb14 = if_else(as.numeric(ymd(date_stamp) - ymd(startday)) <= 14, daily_bh_prb_util, NA_real_),
    prb28 = if_else(as.numeric(ymd(date_stamp) - ymd(startday)) <= 28, daily_bh_prb_util, NA_real_),
    prb60 = if_else(as.numeric(ymd(date_stamp) - ymd(startday)) <= 60, daily_bh_prb_util, NA_real_)
  ) |> 
  group_by(sector) |> 
  summarise(
    p75_all = quantile(daily_bh_prb_util, .75, na.rm = TRUE, names = FALSE),
    p75_14   = quantile(prb14, .75, na.rm = TRUE, names = FALSE),
    p75_28 = quantile(prb28, .75, na.rm = TRUE, names = FALSE),
    p75_60  = quantile(prb60, .75, na.rm = TRUE, names = FALSE),
    .groups = "drop"
  ) |> 
  mutate(
    p75_prb_utilisation = coalesce(p75_14, p75_28, p75_60, p75_all),
    p75_forecast_eoy1 = p75_prb_utilisation * monthly_growth_rate_1,
    p75_forecast_eoy2 = p75_forecast_eoy1 * monthly_growth_rate_2,
    p75_forecast_eoy3 = p75_forecast_eoy2 * monthly_growth_rate_3,
    p75_forecast_eoy4 = p75_forecast_eoy3 * monthly_growth_rate_4
  ) |> 
  select(
    sector, 
    p75_prb_utilisation,
    p75_forecast_eoy1,
    p75_forecast_eoy2,
    p75_forecast_eoy3,
    p75_forecast_eoy4
    ) |> 
  as_tibble()
tictoc::toc()

## sector planning info ------------

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

#  Combine planned and live data ------------------------------------------

sector_info <- sector_planned_info |> 
  full_join(sector_prb, by = 'sector') 

# Calculate congestion and other metrics ----------------------------------------------------

# Normal ------------------------------------------------------------------

sector_info_60 <- sector_info |> mutate(prb_congestion_threshold = 0.6)
sector_info_60 <- sector_info_60 |> 
  mutate(
    x4g_cong = (p75_prb_utilisation > prb_congestion_threshold),
    x4g_cong_y1 = (p75_forecast_eoy1  > prb_congestion_threshold),
    x4g_cong_y2 = (p75_forecast_eoy2  > prb_congestion_threshold),
    x4g_cong_y3 = (p75_forecast_eoy3  > prb_congestion_threshold),
    x4g_cong_y4 = (p75_forecast_eoy4  > prb_congestion_threshold)
  )

# FLTE --------------------------------------------------------------------

# nims_con <- dbConnect(odbc::odbc(), "NMSDB_MNS_FWA", UID = "gis", 
#                       PWD = "gis")
# 
# flte_query <- "
# Select 
# sector, 
# MAX_ULTE_SPEED
# from 
#   GIS.PBI_MAX_SECTOR_U_SPEED
# "
# 
# flte_data <- dbGetQuery(nims_con, flte_query) |> 
#   collect() |> 
#   clean_names() |> 
#   mutate(prb_congestion_threshold = case_when(
#     max_ulte_speed >= 20 ~ 0.4, 
#     max_ulte_speed >= 10 ~ 0.5, 
#     TRUE ~ 0.6
#   ))
# 
# flte_data |> tabyl(prb_congestion_threshold, max_ulte_speed)
# 
# sector_info_flte <- sector_info |> 
#   left_join(flte_data, by = "sector") |>
#   mutate(prb_congestion_threshold = replace_na(prb_congestion_threshold, 0.6)) |> 
#   mutate(
#     x4g_cong = (p75_prb_utilisation > prb_congestion_threshold),
#     x4g_cong_y1 = (p75_forecast_eoy1  > prb_congestion_threshold),
#     x4g_cong_y2 = (p75_forecast_eoy2  > prb_congestion_threshold),
#     x4g_cong_y3 = (p75_forecast_eoy3  > prb_congestion_threshold),
#   )
# 
# rm(df_4g)
# gc()

# Find solutions ----------------------------------------------------------

find_solutions <- function(sector_info, x4g_cong, p75_forecast){
  solution_df <- sector_info |>  
    mutate(
      # Check if L1800 20MHz is deployed
      upg_l1800_20 = if_else(suggestion({{x4g_cong}}, lte_dl_bw_1800 - lte_dl_bw_cellc_1800, 20), 'L1800 20MHz', ''),
      new_x4g_util = congestion_recalculation(lte_dl_bw_total, {{p75_forecast}}, {{x4g_cong}}, lte_dl_bw_1800 - lte_dl_bw_cellc_1800, 20, 20),
      new_lte_dl_bw_total = dl_bw_recalculation(lte_dl_bw_total, {{p75_forecast}}, {{x4g_cong}}, lte_dl_bw_1800 - lte_dl_bw_cellc_1800, 20, 20),
      new_x4g_cong = new_x4g_util > prb_congestion_threshold,
      # Check if L900 10MHz is deployed
      upg_l900_10 = if_else(suggestion(new_x4g_cong, lte_dl_bw_900, 10), 'L900 10MHz', ''),
      new_x4g_util = congestion_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, lte_dl_bw_900, 10, 10),
      new_lte_dl_bw_total = dl_bw_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, lte_dl_bw_900, 10, 10),
      new_x4g_cong = new_x4g_util > prb_congestion_threshold,
      # Check if L2100 20MHz is deployed, make provision for DSS2100
      upg_l2100_20 = if_else(suggestion(new_x4g_cong, lte_dl_bw_2100, 20 - nr_dl_bw_2100), str_glue('L2100 {20 - nr_dl_bw_2100}MHz'), '') ,
      new_x4g_util = congestion_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, lte_dl_bw_2100, 20 - nr_dl_bw_2100, 20 - nr_dl_bw_2100),
      new_lte_dl_bw_total = dl_bw_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, lte_dl_bw_2100, 20 - nr_dl_bw_2100, 20 - nr_dl_bw_2100),
      new_x4g_cong = new_x4g_util > prb_congestion_threshold,
      # Check if L2600 is deployed
      upg_l2600_40 = if_else(suggestion(new_x4g_cong, lte_dl_bw_2600, 40 * tdd_adjustment * l2600_penalty), 'L2600 40MHz', ''),
      new_x4g_util = congestion_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, lte_dl_bw_2600, 40 * tdd_adjustment * l2600_penalty, 40 * tdd_adjustment * l2600_penalty),
      new_lte_dl_bw_total = dl_bw_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, lte_dl_bw_2600, 40 * tdd_adjustment * l2600_penalty, 40 * tdd_adjustment * l2600_penalty),
      new_x4g_cong = new_x4g_util > prb_congestion_threshold,
      # Check if L1800 MORAN is deployed
      upg_l1800_moran = if_else(suggestion(new_x4g_cong, lte_dl_bw_cellc_1800, 10), 'L1800 MORAN 10MHz', ''),
      new_x4g_util = congestion_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, lte_dl_bw_cellc_1800, 10, 10),
      new_lte_dl_bw_total = dl_bw_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, lte_dl_bw_cellc_1800, 10, 10),
      new_x4g_cong = new_x4g_util > prb_congestion_threshold,
      # Check if L800 10MHz is deployed
      upg_l800_10 = if_else(suggestion(new_x4g_cong, lte_dl_bw_800, 10), 'L800 10MHz', ''),
      new_x4g_util = congestion_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, lte_dl_bw_800, 10, 10),
      new_lte_dl_bw_total = dl_bw_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, lte_dl_bw_800, 10, 10),
      new_x4g_cong = new_x4g_util > prb_congestion_threshold,
      # Check if n3500 is deployed, assume only nr_penetration_rate % will be deloaded. Non atm so excluded
      upg_n3500_40 = if_else(suggestion(new_x4g_cong, nr_dl_bw_3500, 40 * tdd_adjustment), 'N3500 40MHz', ''),
      #new_x4g_util = congestion_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, nr_dl_bw_3500, 40 * tdd_adjustment ,  40 * tdd_adjustment ),
      #new_lte_dl_bw_total = dl_bw_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, nr_dl_bw_3500, 40 * tdd_adjustment,  40 * tdd_adjustment),
      # new_x4g_cong = new_x4g_util > prb_congestion_threshold,
      # Check if additional spectrum is required
      add_more_spectrum = if_else(new_x4g_cong, 'Add more spectrum', '')
    ) |>
    mutate(
      solution = str_c(upg_l1800_20,
                       upg_l900_10,
                       upg_l2100_20,
                       upg_l2600_40,
                       upg_l1800_moran,
                       upg_l800_10,
                       upg_n3500_40,
                       add_more_spectrum,
                       sep = '#') |>
        str_trim() |>
        str_replace_all('#+', '; ') |>
        str_replace_all('^; ', '')
    )

  return(solution_df)
}

solution_df_current <- find_solutions(sector_info_60, x4g_cong , p75_prb_utilisation )
solution_df_y1 <- find_solutions(sector_info_60, x4g_cong_y1 , p75_forecast_eoy1 )
solution_df_y2 <- find_solutions(sector_info_60, x4g_cong_y2 , p75_forecast_eoy2 )
solution_df_y3 <- find_solutions(sector_info_60, x4g_cong_y3 , p75_forecast_eoy3 )
solution_df_y4 <- find_solutions(sector_info_60, x4g_cong_y3 , p75_forecast_eoy4 )
# solution_df_current_flte <- find_solutions(sector_info_flte, x4g_cong , p75_prb_utilisation )
# solution_df_y1_flte <- find_solutions(sector_info_flte, x4g_cong_y1 , p75_forecast_eoy1 )
# solution_df_y2_flte <- find_solutions(sector_info_flte, x4g_cong_y2 , p75_forecast_eoy2 )
# solution_df_y3_flte <- find_solutions(sector_info_flte, x4g_cong_y3 , p75_forecast_eoy3 )

#  Sector Summary ----------------------------------------------------------------

solution_df_current |> 
  count(solution, sort = TRUE)

# Site level --------------------------------------------------------------

# SiteINFO -----------------------------------------------

scope <- vroom("D:/temp_rudi/2023_Scope.csv") |> 
  clean_names() |> 
  select(site_id, contains("scope"))

cat <- vroom("D:/temp_rudi/shesha_cat.csv") |> 
  select(site_id, CAT_NEW) |> 
  clean_names() 

mod <- vroom("D:/temp_rudi/Modernisation Site List.csv") |> 
  select(site_id, modernised) |> 
  clean_names()

cbu <- vroom("//mtndata/Network Group/NWG_Site_Data/current/cells/csv/OPERATIONAL_SITES.csv") |> 
  select(site_id, enumerator_area_code, territory, cbu_value_band) |> 
  clean_names()

library(readxl)
batch <- read_excel("//mtndata/Network Group/OpsData/PlanOpt/Users/Rudi/gis/aoc ea township and batch/GeoMaster_EA_Table.xlsx", 
                    sheet = "CENSUS_2017_ENUMERATOR_AREAS") |> 
  clean_names() |> 
  select(id, icasa_batch)

current_site_info <- cbu |> 
  left_join(cat, by = "site_id") |> 
  left_join(mod, by = "site_id") |> 
  left_join(scope, by = "site_id") |> 
  left_join(batch, by = c("enumerator_area_code" = "id")) |> 
  mutate(site_id = as.character(site_id)) |> 
  arrange(cat_new) |> 
  group_by(site_id) |> 
  mutate(rn = row_number()) |> 
  ungroup() |> 
  filter(rn ==1) |> 
  select(-rn) |> 
  left_join(lte_operational_cells, by = 'site_id')

find_site_solution <- function(solution_df){
  # solution_df |>
  #   filter(!is.na(lte_dl_bw_total )) |>
  #   mutate(site_id = str_extract(sector, '[0-9]*'),
  #          bw_added = new_lte_dl_bw_total - lte_dl_bw_total + (upg_n3500_40 != "")*40*tdd_adjustment ) |>
  #   arrange(-bw_added) |>
  #   group_by(site_id) |>
  #   mutate(rn = row_number()) |>
  #   ungroup() |>
  #   filter(rn == 1) |>
  #   select(site_id, everything(), -rn) |> 
  solution_df |>
    mutate(site_id = str_extract(sector, '[0-9]*')) |>
    select(site_id, everything(), -sector) |>
    group_by(site_id) |>
    summarise_all(~max(., na.rm = TRUE), .groups = 'drop') |>
    left_join(current_site_info, by = "site_id") |>
    mutate(
      cat_new = replace_na(cat_new, 9),
      upg_l1800_20 = if_else(!is.na(scope_mod) & cat_new != 4, "", upg_l1800_20),
      upg_l900_10 = if_else(!is.na(scope_mod), "", upg_l900_10),
      upg_l2100_20 = if_else(!is.na(scope_mod) & cat_new != 4, "", upg_l2100_20), 
      upg_l2600_40 = if_else(!is.na(scope_l2600), "", upg_l2600_40), 
      upg_l1800_moran = if_else(!is.na(scope_mod) & cat_new != 4, "", upg_l1800_moran),
      upg_l800_10 = if_else(!is.na(scope_mod) & icasa_batch == "BATCH 3", "", upg_l800_10),
      upg_n3500_40 =  if_else(!is.na(scope_5g), "", upg_n3500_40)
    )  |>
    mutate(
      solution = str_c(upg_l1800_20,
                       upg_l900_10,
                       upg_l2100_20,
                       upg_l2600_40,
                       upg_l1800_moran,
                       upg_l800_10,
                       upg_n3500_40,
                       add_more_spectrum,
                       sep = '#') |>
        str_trim() |>
        str_replace_all('#+', '; ') |>
        str_replace_all('^; ', '')
    ) |> 
    mutate(action = !is.na(solution) & solution != "")
}

tictoc::tic()
site_solution_current <- find_site_solution(solution_df_current)
site_solution_y1 <- find_site_solution(solution_df_y1)
site_solution_y2 <- find_site_solution(solution_df_y2)
site_solution_y3 <- find_site_solution(solution_df_y3)
site_solution_y4 <- find_site_solution(solution_df_y4)
# site_solution_current_flte <- find_site_solution(solution_df_current_flte)
# site_solution_y1_flte <- find_site_solution(solution_df_y1_flte)
# site_solution_y2_flte <- find_site_solution(solution_df_y2_flte)
# site_solution_y3_flte <- find_site_solution(solution_df_y3_flte)
tictoc::toc()

# Join all site solutions -------------------------------------------------

all_site_solutions <- 
site_solution_current |> 
  left_join(site_solution_y1 |> select(site_id, matches("(upg)|(add_more)|(new_)|(solution)|(action)")),
            by = "site_id", suffix = c("_current", "_EoY2023")) |> 
  left_join(site_solution_y2 |> select(site_id, matches("(upg)|(add_more)|(new_)|(solution)|(action)")),
            by = "site_id") |> 
  left_join(site_solution_y3 |> select(site_id, matches("(upg)|(add_more)|(new_)|(solution)|(action)")),
            by = "site_id", suffix = c("_EoY2024", "_EoY2025")) |> 
  left_join(site_solution_y4 |> select(site_id, matches("(upg)|(add_more)|(new_)|(solution)|(action)")) |> 
                                         rename_with(~paste0(., "_EoY2026"), matches("(upg)|(add_more)|(new_)|(solution)|(action)")),
            by = "site_id") |> 
  select(site_id, contains("bw"), contains("p75"), contains("cong"), contains("new_"), everything()) |> 
  relocate(contains("solution"), .after = last_col()) |> 
  relocate(contains("action"), .after = last_col())

vroom_write(
  all_site_solutions, 
  str_glue("//mtndata/Network Group/OpsData/PlanOpt/Users/Rudi/R/Upgrade Recommendation/site_solutions_{Sys.Date()}.csv"),
  delim = ",", 
  na = "",
  num_threads = 12
)

all_site_solutions |> 
  pivot_longer(matches('(upg)|(add_more)'), names_to = 'names', values_to = 'value') |> 
  filter(!is.na(value),value !="") |> 
  mutate(year = str_sub(names, -4, -1),
         names = str_remove(names, "(_EoY[0-9]*)|(_current)"),
         year = if_else(year == 'rent', 'Current', year)) |> 
  count(year, territory, names)

# Site summary ------------------------------------------------------------

get_site_solution_summary <- function(df, new_name){
  df |> select(site_id, contains('upg'), add_more_spectrum) |> pivot_longer(!site_id) |> 
  mutate(value = if_else(value == '', 0, 1),
         name = factor(name)) |> 
  filter(value == 1) |> 
  count(name) |> 
    rename('{new_name}' := name)
}

get_site_solution_summary(site_solution_current, "PRB60_Current")
get_site_solution_summary(site_solution_y1, "PRB60_Year1")
get_site_solution_summary(site_solution_y2, "PRB60_Year2")
get_site_solution_summary(site_solution_y3, "PRB60_Year3")
get_site_solution_summary(site_solution_y4, "PRB60_Year4")
# get_site_solution_summary(site_solution_current_flte, "PRB_FLTE_Current")
# get_site_solution_summary(site_solution_y1_flte, "PRB_FLTE_Year1")
# get_site_solution_summary(site_solution_y2_flte, "PRB_FLTE_Year2")
# get_site_solution_summary(site_solution_y3_flte, "PRB_FLTE_Year3")

# Export to Excel ---------------------------------------------------------

library(openxlsx)

all_site_solutions <- list(
  # site_solution_current,
  # site_solution_current_flte,
  PRB60_Year1 = site_solution_y1,
  PRB60_Year2 = site_solution_y2,
  PRB60_Year3 = site_solution_y3,
  # PRB_FLTE_Year1 = site_solution_y1_flte,
  # PRB_FLTE_Year2 = site_solution_y2_flte,
  # PRB_FLTE_Year3 = site_solution_y3_flte
)

openxlsx::write.xlsx(
  all_site_solutions, 
  str_glue("//mtndata/Network Group/OpsData/PlanOpt/Users/Rudi/R/Upgrade Recommendation/site_solutions_{Sys.Date()}.xlsx"),
  asTable = TRUE,
  firstRow = TRUE, 
  firstCol = TRUE
)


# Combine and report ------------------------------------------------------

bind_rows(
  site_solution_current |> mutate(dataset = "PRB60_Current"),
  site_solution_y1 |> mutate(dataset = "PRB60_EoY2023"),
  site_solution_y2 |> mutate(dataset = "PRB60_EoY2024"),
  site_solution_y3 |> mutate(dataset = "PRB60_EoY2025"),
  # site_solution_y1_flte |> mutate(dataset = "PRB_FLTE_Year1"),
  # site_solution_y2_flte |> mutate(dataset = "PRB_FLTE_Year2"),
  # site_solution_y3_flte |> mutate(dataset = "PRB_FLTE_Year3")
  ) |> 
  select(dataset, site_id, contains('upg'), add_more_spectrum) |> 
  pivot_longer(!dataset:site_id, names_to = "activity", values_to = "action") |> 
  filter(action != "" | is.na(action)) |> 
  mutate(activity = factor(activity, levels= c(
    "upg_l1800_20",
    "upg_l900_10",
    "upg_l2100_20",
    "upg_l2600_40",
    "upg_l1800_moran",
    "upg_l800_10",
    "upg_n3500_40",
    "add_more_spectrum"
  ))) |> 
  tabyl(dataset, activity)

bind_rows(
  site_solution_current |> count(x4g_cong) |> filter(x4g_cong) |> mutate(dataset = "PRB60_Current") |> rename(Congesting = x4g_cong, Sites = n),
site_solution_y1 |> count(x4g_cong_y1) |> filter(x4g_cong_y1) |> mutate(dataset = "PRB60_EoY2023") |> rename(Congesting = x4g_cong_y1, Sites = n),
site_solution_y2 |> count(x4g_cong_y2) |> filter(x4g_cong_y2) |> mutate(dataset = "PRB60_EoY2024") |> rename(Congesting = x4g_cong_y2, Sites = n),
site_solution_y3 |> count(x4g_cong_y3) |> filter(x4g_cong_y3) |> mutate(dataset = "PRB60_EoY2025") |> rename(Congesting = x4g_cong_y3, Sites = n),
# site_solution_y1_flte |> count(x4g_cong_y1) |> filter(x4g_cong_y1) |> mutate(dataset = "PRB_FLTE_Year1") |> rename(Congesting = x4g_cong_y1, Sites = n),
# site_solution_y2_flte |> count(x4g_cong_y2) |> filter(x4g_cong_y2) |> mutate(dataset = "PRB_FLTE_Year2") |> rename(Congesting = x4g_cong_y2, Sites = n),
# site_solution_y3_flte |> count(x4g_cong_y3) |> filter(x4g_cong_y3) |> mutate(dataset = "PRB_FLTE_Year3") |> rename(Congesting = x4g_cong_y3, Sites = n)
)











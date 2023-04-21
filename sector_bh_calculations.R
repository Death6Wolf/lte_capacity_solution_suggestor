#-----------------------------------------------------------------------------#
#           Script to calculate sector busy hour performance                  #
#                      For 4G and 5G                                          #
#                       2023-04-19                                            #
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
prb_congestion_threshold <- 0.7
prb_congestion_threshold_excl_l2600 <- 0.7
throughput_congestion_threshold <- 5000
throughput_congestion_threshold_excl_l2600 <- 5000
nr_penetration_rate <- 0.2
traffic_increase_upg_prop <- 0.0 # Not implemented, make it variable for congestion_recalculation()?

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
x5g_path <- "D:/Radio_Network_Planning/bh_query_automation/nr/"

# Read 4G files -----------------------------------------------------------

x4g_files <- 
  tibble(file_name = list.files(path = x4g_path, full.names = TRUE)) |> 
  mutate(date = ymd(str_sub(file_name, -12, -5))) |> 
  filter(date >= (Sys.Date()- 15) )

df_4g <- map(x4g_files$file_name, vroom) |> 
  list_rbind()

# Read 5G files -----------------------------------------------------------

x5g_files <- 
  tibble(file_name = list.files(path = x5g_path, full.names = TRUE)) |> 
  mutate(date = ymd(str_sub(file_name, -12, -5))) |> 
  filter(date >= (Sys.Date()- 31) )

df_5g <- map(x5g_files$file_name, vroom) |> 
  list_rbind()


# Get VW_MAPS Info --------------------------------------------------------

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
  and plan_status = 'Operational'
"

nims_data <- dbGetQuery(nims_con, nims_query) |> 
  collect() |> 
  clean_names() |> 
  mutate(
    bandwidth = parse_integer(bandwidth),
    dl_bw = if_else(frequency_band %in% c('2600', '3500'), bandwidth * tdd_adjustment, bandwidth)
  ) 

# Calculate LTE Sector Performance ----------------------------------------

## Base Performance ------------

sector_performance_4g <- 
  df_4g |> 
  group_by(sector) |> 
  summarise(
  x4g_availability = round((sum(x4g_rop_time, na.rm = TRUE) - sum(x4g_downtime, na.rm = TRUE))/ sum(x4g_rop_time, na.rm = TRUE), 4) ,
  x4g_prb_dl_util = sum(nr_used_pdsch_prbs, na.rm = TRUE)/ sum(nr_avail_dl_prbs, na.rm = TRUE) ,
  x4g_prb_dl_util_excl_l2600 = round(sum(nr_used_pdsch_prbs[!(carrier %in% c("40090_20", "40288_20"))], na.rm = TRUE)/ sum(nr_avail_dl_prbs[!(carrier %in% c("40090_20", "40288_20"))], na.rm = TRUE), 4),
  x4g_prb_dl_used_p50 = median(nr_used_pdsch_prbs, na.rm = TRUE),
  x4g_prb_dl_available_p50 = median(nr_avail_dl_prbs, na.rm = TRUE),
  x4g_dl_ue_thput = round(1000 * sum(pdcp_drb_vol_dl, na.rm = TRUE)/ sum(ue_thr_time_dl, na.rm = TRUE), 2) ,
  x4g_dl_ue_thput_excl_l2600 = 1000 * round(sum(pdcp_drb_vol_dl[!(carrier %in% c("40090_20", "40288_20"))], na.rm = TRUE)/ sum(ue_thr_time_dl[!(carrier %in% c("40090_20", "40288_20"))], na.rm = TRUE), 4) ,
  x4g_ue_distance_p50 = median(ue_dist_avg, na.rm = TRUE),
  x4g_rrc_con_ue_p50 = median(rrc_conn_ues_avg, na.rm = TRUE),
  x4g_cce_util = round(sum(nr_pdcch_cce_used, na.rm = TRUE)/ sum(nr_pdcch_cce_avail, na.rm = TRUE), 4),
  bh_time = median(starttime, na.rm = TRUE) %/% 10000,
  x4g_dl_data_vol = sum(x4g_data_vol_dl, na.rm = TRUE),
  x4g_plmn1_perc_dl_data_vol = sum(x4g_data_vol_dl_plmn1, na.rm = TRUE)/1000000 / sum(x4g_data_vol_dl, na.rm = TRUE)
    ) 

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

# Calculate NR Sector Performance ----------------------------------------

sector_performance_5g <- 
  df_5g |> 
  group_by(sector) |> 
  summarise(
    x5g_availability = round((sum(x5g_rop_time, na.rm = TRUE) - sum(x5g_downtime, na.rm = TRUE))/ sum(x5g_rop_time, na.rm = TRUE), 4) ,
    x5g_prb_dl_util = round(sum(nr_used_nr_dl_prb , na.rm = TRUE)/ sum(nr_avail_nr_dl_prb , na.rm = TRUE), 4) ,
    x5g_prb_dl_util_n3500 = round(sum(nr_used_nr_dl_prb[carrier == '634666_30_40'] , na.rm = TRUE)/ sum(nr_avail_nr_dl_prb[carrier == '634666_30_40'] , na.rm = TRUE), 4) ,
    x5g_prb_dl_used_p50 = median(nr_used_nr_dl_prb , na.rm = TRUE),
    x5g_prb_dl_available_p50 = median(nr_avail_nr_dl_prb , na.rm = TRUE),
    x5g_dl_ue_thput = 1000 * round(sum(rlc_drb_vol_dl , na.rm = TRUE)/ sum(drb_thr_time_dl , na.rm = TRUE), 4) ,
    x5g_dl_ue_thput_n3500 = 1000 * round(sum(rlc_drb_vol_dl[carrier == '634666_30_40'] , na.rm = TRUE)/ sum(drb_thr_time_dl[carrier == '634666_30_40'] , na.rm = TRUE), 4) ,
    x5g_ue_distance_p50 = median(nr_ue_dist_avg , na.rm = TRUE),
    x5g_rrc_con_ue_p50 = median(nr_rrc_conn_ues_avg , na.rm = TRUE)
  ) 
  
#  Combine planned and live data ------------------------------------------

sector_info <- sector_planned_info |> 
  full_join(sector_performance_4g, by = 'sector') |> 
  full_join(sector_performance_5g, by = 'sector')

# Calculate congestion ----------------------------------------------------

sector_info <- sector_info|> 
  mutate(
    x4g_cong = (x4g_prb_dl_util > prb_congestion_threshold) & (x4g_prb_dl_util_excl_l2600 > prb_congestion_threshold_excl_l2600), 
    x4g_cong_excl_l2600 = (x4g_prb_dl_util_excl_l2600 > prb_congestion_threshold_excl_l2600) & (x4g_dl_ue_thput_excl_l2600  < throughput_congestion_threshold_excl_l2600)
  ) 

# Find solutions ----------------------------------------------------------

solution_df <- sector_info |>  
  mutate(
    # Check if L1800 20MHz is deployed
    upg_l1800_20 = if_else(suggestion(x4g_cong, lte_dl_bw_1800, 20), 'L1800 20MHz', ''),
    new_x4g_util = congestion_recalculation(lte_dl_bw_total, x4g_prb_dl_util, x4g_cong, lte_dl_bw_1800, 20, 20),
    new_lte_dl_bw_total = dl_bw_recalculation(lte_dl_bw_total, x4g_prb_dl_util, x4g_cong, lte_dl_bw_1800, 20, 20),
    new_x4g_cong = new_x4g_util > prb_congestion_threshold,
    # Check if L2100 20MHz is deployed
    upg_l2100_20 = if_else(suggestion(new_x4g_cong, lte_dl_bw_2100 + nr_dl_bw_2100, 20), 'L2100 20MHz', ''),
    new_x4g_util = congestion_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, lte_dl_bw_2100 + nr_dl_bw_2100, 20, 20 - nr_dl_bw_2100),
    new_lte_dl_bw_total = dl_bw_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, lte_dl_bw_2100 + nr_dl_bw_2100, 20, 20 - nr_dl_bw_2100),
    new_x4g_cong = new_x4g_util > prb_congestion_threshold,
    # Check if L900 10MHz is deployed
    upg_l900_10 = if_else(suggestion(new_x4g_cong, lte_dl_bw_900, 10), 'L900 10MHz', ''),
    new_x4g_util = congestion_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, lte_dl_bw_900, 10, 10),
    new_lte_dl_bw_total = dl_bw_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, lte_dl_bw_900, 10, 10),
    new_x4g_cong = new_x4g_util > prb_congestion_threshold,
    # Check if L2600 is deployed
    upg_l2600_40 = if_else(suggestion(new_x4g_cong, lte_dl_bw_2600, 40 * tdd_adjustment), 'L2600 40MHz', ''),
    new_x4g_util = congestion_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, lte_dl_bw_2600, 40 * tdd_adjustment, 40 * tdd_adjustment),
    new_lte_dl_bw_total = dl_bw_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, lte_dl_bw_2600, 40 * tdd_adjustment, 40 * tdd_adjustment),
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
    # Check if n3500 is deployed, assume only nr_penetration_rate % will be deloaded.
    upg_n3500_40 = if_else(suggestion(new_x4g_cong, nr_dl_bw_3500, 40 * tdd_adjustment), 'N3500 40MHz', ''),
    new_x4g_util = congestion_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, nr_dl_bw_3500, 40 * tdd_adjustment,  40 * tdd_adjustment * nr_penetration_rate),
    new_lte_dl_bw_total = dl_bw_recalculation(new_lte_dl_bw_total, new_x4g_util, new_x4g_cong, nr_dl_bw_3500, 40 * tdd_adjustment,  40 * tdd_adjustment * nr_penetration_rate),
    new_x4g_cong = new_x4g_util > prb_congestion_threshold,
    # Check if additional spectrum is required
    add_more_spectrum = if_else(suggestion(new_x4g_cong, 0, 100), 'Add more spectrum', ''),
  ) |> 
  mutate(
    solution = str_c(upg_l1800_20, 
                     upg_l2100_20,
                     upg_l900_10, 
                     upg_l2600_40, 
                     upg_l1800_moran, 
                     upg_l800_10, 
                     upg_n3500_40, 
                     add_more_spectrum,
                     sep = '  ') |> 
      str_trim() |> 
      str_replace_all('\\s{2,}', '; ')
  )
  
solution_df |> 
  filter(x4g_cong) |> 
  select(solution) |> print(n = 100)

# Summary -----------------------------------------------------------------






#-----------------------------------------------------------------------------#
#           Script to calculate sector busy hour utilisation                  #
#                      For 4G and 5G                                          #
#                       2023-04-19                                            #
#                                                                             #
#-----------------------------------------------------------------------------#

# Libraries --------------------------------------------------------------

library(tidyverse)
library(DBI)
library(odbc)
library(janitor)
library(glue)
library(lubridate)
library(tictoc)

# connectors --------------------------------------------------------------

maps <- dbConnect(odbc::odbc(), "Maps", UID = "radio-tenant", PWD = "Maps@radio")

# Set parameters ---------------------------------------------------------------

days_ago <- 1


# Filenames -------------------------------------------------------------

date_stamp <- (Sys.Date() - days(days_ago)) |> format("%Y%m%d")

x4g_file_name <- str_c("D:/Radio_Network_Planning/bh_query_automation/lte/x4g_cell_ltedlprbbh_", date_stamp, ".csv")

x5g_file_name <- str_c("D:/Radio_Network_Planning/bh_query_automation/nr/x5g_cell_ltedlprbbh_", date_stamp, ".csv")

# Get 4G datas ---------------------------------------------------------------

print("Getting 4G data.")

maps_4g_query <- str_glue("
SELECT
    c.`4g cell_name` as cell_name ,
    regexp_extract(c.`4g cell_name` , '[1-9][0-9]*[A-Z]*' , 0) as sector ,
    c.startday ,
    c.starttime ,
    c.datetime, 
    c.`4g cell rop time (enhanced)_za` ,
    c.`4g cell downtime (enhanced)_za` ,
    c.`nr of used pdsch prbs (avg)_std` ,
    c.`nr of available downlink prbs (avg)_std` ,
    c.`rrc connected ues (avg)_std` ,
    c.`pdcp drb volume dl excluding last tti plmn0_za` ,
    c.`ue throughput time dl excluding last tti plmn0_za` ,
    c.`4g data volume dl_std` ,
    c.`4g data volume dl plmn0_kpi_za`,
    c.`4G Data Volume DL PLMN1_KPI_ZA`,
    --c.`4G Data Volume DL PLMN3_KPI_ZA`,
    c.`4g data volume ul_std` ,
    c.`4g data volume ul plmn0_kpi_za`,
    c.`4G Data Volume uL PLMN1_KPI_ZA`,
    --c.`4G Data Volume uL PLMN3_KPI_ZA`,
    --c.`4g data volume plmn3_za`,
    c.`ue distance (avg)_za` ,
    c.`Nr of PDCCH CCE Used_STD` ,
    c.`Nr of PDCCH CCE Available_STD`,
    c.`pdcp drb volume dl qci128_za`,
    c.carrier
FROM
    v_4g_cell_60 c
    WHERE
        c.startday == from_unixtime(unix_timestamp() - {days_ago} * 24 * 60 * 60, 'yyyyMMdd')
        AND
        regexp_extract(c.`4g cell_name` , '[1-9][0-9]*[A-Z]*' , 0) <> ''
    ")

tictoc::tic()
maps_4g_cell_bh <- dbGetQuery(maps, maps_4g_query) %>%
  collect() %>%
  as_tibble() %>%
  clean_names()
tictoc::toc()

print("4G data load completed.")

# Calculate sector bh ---------------------------------------------------------------
tic()
sector_max <- maps_4g_cell_bh |> 
  select(cell_name, sector, startday, starttime, nr_of_used_pdsch_prbs_avg_std) |> 
  group_by(sector, starttime, startday) |> 
  summarise(sector_prb = sum(nr_of_used_pdsch_prbs_avg_std, na.rm = TRUE)) |> 
  ungroup() |> 
  arrange(desc(sector_prb)) |> 
  group_by(sector, startday) |> 
  slice(1) |> 
  ungroup()
toc()

print("Sector BH calculation complete.")

# Get 4G bh data---------------------------------------------------------------

tic()
final_4g_df <- maps_4g_cell_bh |> 
  inner_join(sector_max, 
             by = c('sector', 'startday', 'starttime')) |> 
  rename(
    x4g_rop_time = x4g_cell_rop_time_enhanced_za,
    x4g_downtime = x4g_cell_downtime_enhanced_za,
    nr_used_pdsch_prbs = nr_of_used_pdsch_prbs_avg_std,
    nr_avail_dl_prbs = nr_of_available_downlink_prbs_avg_std,
    rrc_conn_ues_avg = rrc_connected_ues_avg_std,
    pdcp_drb_vol_dl = pdcp_drb_volume_dl_excluding_last_tti_plmn0_za,
    ue_thr_time_dl = ue_throughput_time_dl_excluding_last_tti_plmn0_za,
    x4g_data_vol_dl = x4g_data_volume_dl_std,
    x4g_data_vol_dl_plmn0 = x4g_data_volume_dl_plmn0_kpi_za,
    x4g_data_vol_dl_plmn1 = x4g_data_volume_dl_plmn1_kpi_za,
    x4g_data_vol_ul = x4g_data_volume_ul_std,
    x4g_data_vol_ul_plmn0 = x4g_data_volume_ul_plmn0_kpi_za,
    x4g_data_vol_ul_plmn1 = x4g_data_volume_u_l_plmn1_kpi_za,
    ue_dist_avg = ue_distance_avg_za,
    nr_pdcch_cce_used = nr_of_pdcch_cce_used_std,
    nr_pdcch_cce_avail = nr_of_pdcch_cce_available_std,
    qci128_drb_vol_dl = pdcp_drb_volume_dl_qci128_za
  ) |> 
  select(-sector_prb)
toc()

print("4G BH calculation complete.")


# Export 4G bh ------------------------------------------------------------

final_4g_df |> 
  write_csv(x4g_file_name, na = "")

print("4G BH write to csv complete.")

# Get 5G data -------------------------------------------------------------

print("Getting 5G data.")

maps_5g_query <- str_glue("
SELECT
c.`5g cell_name` as cell_name ,
regexp_extract(c.`5g cell_name` , '[1-9][0-9]*[A-Z]*' , 0) as sector ,
c.startday ,
c.starttime ,
c.datetime, 
c.`5g cell rop time (enhanced)_za` ,
c.`5g cell downtime (enhanced)_za` ,
c.`Nr of Used NR Downlink PRBs_ZA` ,
c.`Nr of Available NR Downlink PRBs_ZA` ,
c.`nr rrc connected ues (avg)_za` ,
c.`RLC DRB Volume DL Excluding Last TTI (gNB)_ZA` ,
c.`DRB Throughput Time DL Excluding Last TTI (gNB)_ZA` ,
c.`5g data volume dl_za` ,
c.`5g data volume ul_za` ,
c.`NR UE Distance (Avg)_ZA` ,
c.`nr rlc sdu data volume dl (qci128)_za`,
c.carrier
FROM
v_5g_cell_60 c
WHERE
c.startday == from_unixtime(unix_timestamp() - {days_ago} * 24 * 60 * 60, 'yyyyMMdd')
    ")

tictoc::tic()
maps_5g_cell_bh <- dbGetQuery(maps, maps_5g_query) %>%
  collect() %>%
  as_tibble() %>%
  clean_names()
tictoc::toc() 

print("5G data load completed.")

# Calculate 5G BH ---------------------------------------------------------

tic()
final_5g_df <- maps_5g_cell_bh |> 
  inner_join(sector_max, 
             by = c('sector', 'startday', 'starttime')) |> 
  rename(
    x5g_rop_time = x5g_cell_rop_time_enhanced_za,
    x5g_downtime = x5g_cell_downtime_enhanced_za,
    nr_used_nr_dl_prb = nr_of_used_nr_downlink_pr_bs_za,
    nr_avail_nr_dl_prb = nr_of_available_nr_downlink_pr_bs_za,
    nr_rrc_conn_ues_avg = nr_rrc_connected_ues_avg_za,
    rlc_drb_vol_dl = rlc_drb_volume_dl_excluding_last_tti_g_nb_za,
    drb_thr_time_dl = drb_throughput_time_dl_excluding_last_tti_g_nb_za,
    x5g_data_vol_dl = x5g_data_volume_dl_za,
    x5g_data_vol_ul = x5g_data_volume_ul_za,
    nr_ue_dist_avg = nr_ue_distance_avg_za,
    qci128_sdu_vol_dl = nr_rlc_sdu_data_volume_dl_qci128_za
  ) |> 
  select(-sector_prb)
toc()

print("5G BH calculation complete.")

# Export 5G BH data -------------------------------------------------------

final_5g_df |> 
  write_csv(x5g_file_name, na = "")

print("5G BH write to csv complete.")

print(str_glue("Script for {date_stamp} completed."))




  

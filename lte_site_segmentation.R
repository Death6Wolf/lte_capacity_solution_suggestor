library(tidyverse)
library(odbc)
library(janitor)
library(lubridate)

theme_set(theme_linedraw(base_family = 'MTN Brighter Sans'))

maps <- dbConnect(odbc::odbc(), "MapsRadioTenant", UID = "radio-tenant", 
                  PWD = "Maps@radio")

query <- "
select 
  site_name as site_id,
  startday,
`4g data volume_std` ,
`e-utran ip throughput ue dl plmn0_za` 
from 
  v_site_mo
  where
  startday = 20230601
"

df <- dbGetQuery(maps, query) |> 
    collect() |> 
    clean_names() 

dbDisconnect(maps)

totals <- df |> 
    filter(x4g_data_volume_std > 0) |> 
    summarise(sites = n(), 
              total_x4g_data_volume_std = sum(x4g_data_volume_std))

site_df <- df |> 
    filter(x4g_data_volume_std > 0) |> 
    arrange(-x4g_data_volume_std) |> 
    mutate(
        rn = row_number(),
        cum_x4g_data_volume_std = cumsum(x4g_data_volume_std),
        site_perc = 100*rn/max(totals$sites),
        perc_x4g_data_volume_std = 100*cum_x4g_data_volume_std/max(totals$total_x4g_data_volume_std)
    )|> 
    as_tibble()

site_df |> filter(perc_x4g_data_volume_std >= 10) |> head(3)
site_df |> filter(perc_x4g_data_volume_std >= 25) |> head(3)
site_df |> filter(perc_x4g_data_volume_std >= 50) |> head(3)
site_df |> filter(perc_x4g_data_volume_std >= 75) |> head(3)
site_df |> filter(perc_x4g_data_volume_std >= 90) |> head(3)

site_df |> 
    ggplot(aes(x = site_perc, 
               y = perc_x4g_data_volume_std)) +
    geom_line() +
    geom_segment(aes(x = 2.47, xend = 2.47, y = 0, yend = 10), lty = 2, colour = 'gray30', linewidth = 0.3) +
    geom_segment(aes(x = 7.76, xend = 7.76, y = 0, yend = 25), lty = 2, colour = 'gray30', linewidth = 0.3) +
    geom_segment(aes(x = 20.3, xend = 20.3, y = 0, yend = 50), lty = 2, colour = 'gray30', linewidth = 0.3) +
    geom_segment(aes(x = 39.6, xend = 39.6, y = 0, yend = 75), lty = 2, colour = 'gray30', linewidth = 0.3) +
    geom_segment(aes(x = 59, xend = 59, y = 0, yend = 90), lty = 2, colour = 'gray30', linewidth = 0.3) +
    theme_bw() +
    theme(text = element_text(family = 'MTN Brighter Sans')) +
    scale_x_continuous(breaks = seq(0,100, 10)) +
    labs(
        x = 'Percentage of Sites (%)',
        y = 'Percentage of National (%)',
        subtitle = 'Cumulative LTE Data Volume vs Site Proportion',
        caption = 'Based on monthly 4G data volume per site for the month of June 2023 for traffic carrying sites only.'
    )

ggsave(filename = "C:/Users/Laing_r/OneDrive - MTN Group/Documents/R/Projects/business_plan/exports/cumulative_lte_dv_vs_site_proportion.png", device = 'png', dpi = 720, height = 16, width = 26, units = 'cm')

site_df |> 
    ggplot(aes(x = x4g_data_volume_std/1000/30, 
               y = perc_x4g_data_volume_std)) +
    geom_line() +
    geom_hline(yintercept = 10, lty = 2, colour = 'gray30', linewidth = 0.4) +
    geom_hline(yintercept = 25, lty = 2, colour = 'gray30', linewidth = 0.4) +
    geom_hline(yintercept = 50, lty = 2, colour = 'gray30', linewidth = 0.4) +
    geom_hline(yintercept = 75, lty = 2, colour = 'gray30', linewidth = 0.4) +
    geom_hline(yintercept = 90, lty = 2, colour = 'gray30', linewidth = 0.4) +
    theme_bw() +
    theme(text = element_text(family = 'MTN Brighter Sans')) +
    scale_x_continuous(breaks = seq(0,2500, 100)) +
    labs(
        x = 'Daily LTE Data Volume (GB)',
        y = 'Percentage of National (%)',
        subtitle = 'Cumulative LTE Data Volume vs Per Site Daily LTE Data Volume',
        caption = 'Based on monthly 4G data volume per site for the month of June 2023 for traffic carrying sites only.'
    )

ggsave(filename = "C:/Users/Laing_r/OneDrive - MTN Group/Documents/R/Projects/business_plan/exports/data_volume_thresholds_segmentation.png", device = 'png', dpi = 720, height = 16, width = 26, units = 'cm')

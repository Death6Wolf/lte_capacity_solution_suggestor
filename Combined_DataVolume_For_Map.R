library(tidyverse)
library(odbc)
library(janitor)
library(lubridate)
library(DBI)
maps <- dbConnect(odbc::odbc(), "MapsRadioTenant", UID = "radio-tenant", 
                  PWD = "Maps@radio")

query <- "select 
startday, 
  `2g data volume (ps)_std` ,
    `3g data volume (ps)_std` ,
    `4g data volume_std` ,
    `5g data volume_za` ,
    site_name
from v_site_mo
where startday IN (20230601, 20220601, 20210601)
"

df <- dbGetQuery(maps, query) |> 
    collect() |> 
    clean_names()  |> 
    mutate(
        across(where(is.numeric), ~replace_na(., 0))
    ) |> 
    mutate(
        combined_data_vol = x2g_data_volume_ps_std + x3g_data_volume_ps_std + x4g_data_volume_std + x5g_data_volume_za,
        combined_data_vol = round(combined_data_vol / 30 / 1e3,0)
    ) |> 
    rename(site_id = site_name) 

read_csv("//mtndata/Network Group/NWG_Site_Data/current/cells/csv/OPERATIONAL_SITES.csv") |> 
    mutate(site_id = as.character(site_id)) |> 
    left_join(df |> filter(startday == 20230601),
              by = 'site_id') |> 
    as_tibble() |> 
    write_csv("C:/Users/Laing_r/OneDrive - MTN Group/Documents/R/Projects/business_plan/exports/combined_data_volume_for_map.csv" , na = '')


site_aoc_mapping <-read_csv("R/Projects/business_plan/exports/aoc_site_combined_data_vol.csv") |> 
    select(site_id, aoc_comb) |> 
    mutate(site_id = as.character(site_id))

#Decommissioned sites will be excluded....

read_csv("//mtndata/Network Group/NWG_Site_Data/current/cells/csv/OPERATIONAL_SITES.csv") |> 
    mutate(site_id = as.character(site_id)) |> 
    left_join(df ,by = 'site_id') |> 
    left_join(site_aoc_mapping, by = 'site_id') |> 
    group_by(aoc_comb, startday) |> 
    summarise(combined_data_vol = mean(combined_data_vol, na.rm = TRUE), .groups = 'drop') |> 
    mutate(yr = startday %/% 10000) |> 
    drop_na() |> 
    mutate(aoc_comb = factor(aoc_comb, levels = c('Metropolitan', 'Large Towns', 'Towns', 'Rural'))) |> 
    ggplot(aes(as.factor(aoc_comb), combined_data_vol, fill = factor(yr),label = round(combined_data_vol,0))) +
    geom_col(position = position_dodge()) +
    geom_text(position = position_dodge(width = 0.9), vjust = -0.5) +
    theme_classic()+
    theme(text = element_text(family = 'MTN Brighter Sans'),
          legend.position = 'top') +
    scale_fill_manual(values = c("2021" = "#757780", "2022" = '#070707', "2023" = "#ffc800")) +
    labs(
        x = '', 
        y = 'Data Volume (GB)', 
        title = 'Average Daily Combined Data Volume Per Site',
        fill = 'Year'
    )
    
ggsave(filename = "C:/Users/Laing_r/OneDrive - MTN Group/Documents/R/Projects/business_plan/exports/avg_aoc_comb_data_vol.png", device = 'png', dpi = 720, height = 15, width = 12, units = 'cm')
    

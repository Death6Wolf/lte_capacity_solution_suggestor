library(tidyverse)
library(odbc)
library(janitor)
library(lubridate)
library(DBI)
maps <- dbConnect(odbc::odbc(), "MapsRadioTenant", UID = "radio-tenant", 
                  PWD = "Maps@radio")

query <- "
select 
  startday,
  `2g data volume (ps)_std` ,
`3g data volume (ps)_std` ,
`4g data volume_std` ,
`5g data volume_za` ,
`2g erlang (tch)_std` ,
`3g erlang (cs)_std` ,
`3g erlang cs plmn0 (bestcell)_za`, --comment out
`3g erlang cs plmn1 (bestcell)_za`,
`3g erlang cs plmn3 (bestcell)_za`,
`3g data volume plmn0_za`,
`3g data volume plmn1_za`,
`3g data volume plmn3_za`,
`4g data volume plmn0_za`,
`4g data volume plmn1_za`,
`4g data volume plmn3_za`,
`speech erlang (volte)_std` ,
`e-utran ip throughput ue dl plmn0_za` ,
`nr of available downlink prbs (avg)_std`,
`prb utilisation dl_std`
from 
  v_country_mo
 where startday >= 20200101
 " 

df <- dbGetQuery(maps, query) |> 
    collect()

df |> 
    mutate(ds = ymd(startday)) |> 
    pivot_longer(!c(ds, startday), names_to = 'KPI') |> 
    filter(!str_detect(KPI, 'plmn')) |> 
    mutate(ind = case_when(str_detect(KPI, 'erlang') ~'Erlang (MErl)', 
                           str_detect(KPI, 'data') ~'Data Volume (PB)',
                           TRUE ~ 'other'),
           tech = case_when(
               str_detect(KPI, '2g') ~ '2G',
               str_detect(KPI, '3g') ~ '3G',
               str_detect(KPI, '(4g)|(volte)') ~ '4G',
               str_detect(KPI, '5g') ~ '5G',
               TRUE ~ 'other'
           ),
           value = case_when(
               ind == 'Data Volume (PB)'~ value/1e9,
               ind == 'Erlang (MErl)' ~ value/1e6,
               TRUE ~ value
           )
           ) |> 
    filter(ind != 'other') |> 
    ggplot(aes(ds, value, colour = tech)) +
    geom_line(line_width = 1.1) +
    facet_wrap(~ind, scales = 'free') +
    theme_bw() +
    theme(text = element_text(family = 'MTN Brighter Sans'),
          strip.background =element_rect(fill="black"),
          strip.text = element_text(colour = 'white')) +
    labs(x = '',
         y = '',
         colour = '') +
    ggsci::scale_colour_jco()

ggsave(filename = "C:/Users/Laing_r/OneDrive - MTN Group/Documents/R/Projects/business_plan/exports/traffic_trends.png", device = 'png', dpi = 720, height = 15, width = 28, units = 'cm')

df |>
    mutate(ds = ymd(startday)) |> 
    pivot_longer(!c(ds, startday), names_to = 'KPI') |> 
    filter(str_detect(KPI, 'plmn')) |> 
    mutate(ind = case_when(str_detect(KPI, 'erlang') ~'Erlang (MErl)', 
                           str_detect(KPI, 'data') ~'Data Volume (PB)',
                           TRUE ~ 'other'),
           tech = case_when(
               str_detect(KPI, '2g') ~ '2G',
               str_detect(KPI, '3g') ~ '3G',
               str_detect(KPI, '(4g)|(volte)') ~ '4G',
               str_detect(KPI, '5g') ~ '5G',
               TRUE ~ 'other'
           ),
           value = case_when(
               ind == 'Data Volume (MB)'~ value/1e6,
               ind == 'Erlang (MErl)' ~ value/1e6,
               TRUE ~ value
           ),
           plmn =str_extract(KPI, 'plmn[0-9]')
    ) |> 
    filter(ind != 'other') |> 
    ggplot(aes(ds, value, colour = KPI)) +
    geom_line(size = 1) +
    facet_grid(tech~ind, scales = 'free') +
    theme_bw() +
    theme(text = element_text(family = 'MTN Brighter Sans'),
          strip.background =element_rect(fill="black"),
          strip.text = element_text(colour = 'white')) +
    labs(x = '',
         y = '',
         colour = '') +
    ggsci::scale_colour_jco()


x3g_e <- df  |>
    mutate(ds = ymd(startday)) |> 
    filter(ds >= ymd(20210201)) |> 
    select(ds, `3g erlang cs plmn0 (bestcell)_za`, `3g erlang cs plmn1 (bestcell)_za`, `3g erlang cs plmn3 (bestcell)_za`) |> 
    pivot_longer(!c(ds), names_to = 'KPI') |> 
    mutate(value = value/1e6,
           plmn = str_extract(KPI, 'plmn[0-9]') |> str_to_upper()) |> 
    ggplot(aes(ds, value, colour = plmn)) +
    geom_line(show.legend = FALSE) +
    theme_bw() +
    theme(text = element_text(family = 'MTN Brighter Sans')) +
    scale_color_manual(values = c('PLMN0' = "#FFC800", "PLMN1" = "black", "PLMN3" = "blue")) +
    labs(x = '',
         y = '',
         subtitle = '3G Erlang (MErl)',
         colour = '')

x3g_d <- df  |> 
    mutate(ds = ymd(startday)) |> 
    filter(ds >= ymd(20210201)) |> 
    select(
        ds, 
        `3g data volume plmn0_za`, 
        `3g data volume plmn1_za`, 
        `3g data volume plmn3_za`
        ) |> 
    pivot_longer(!c(ds), names_to = 'KPI') |> 
    mutate(value = value/1e15,
           plmn = str_extract(KPI, 'plmn[0-9]') |> str_to_upper()) |> 
    ggplot(aes(ds, value, colour = plmn)) +
    geom_line(show.legend = FALSE) +
    theme_bw() +
    theme(text = element_text(family = 'MTN Brighter Sans')) +
    scale_color_manual(values = c('PLMN0' = "#FFC800", "PLMN1" = "black", "PLMN3" = "blue")) +
    labs(x = '',
         y = '',
         subtitle = '3G Data Volume (PB)',
         colour = '')

x4g_d <- df  |> 
    mutate(ds = ymd(startday)) |> 
    filter(ds >= ymd(20210201)) |> 
    select(
        ds, 
        `4g data volume plmn0_za`, 
        `4g data volume plmn1_za`, 
        `4g data volume plmn3_za`
    ) |> 
    pivot_longer(!c(ds), names_to = 'KPI') |> 
    mutate(value = value/1e15,
           plmn = str_extract(KPI, 'plmn[0-9]') |> str_to_upper()) |> 
    ggplot(aes(ds, value, colour = plmn)) +
    geom_line() +
    theme_bw() +
    theme(text = element_text(family = 'MTN Brighter Sans')) +
    scale_color_manual(values = c('PLMN0' = "#FFC800", "PLMN1" = "black", "PLMN3" = "blue")) +
    labs(x = '',
         y = '',
         subtitle = '4G Data Volume (PB)',
         colour = "")

library(patchwork)

x3g_e + x3g_d + x4g_d

ggsave(filename = "C:/Users/Laing_r/OneDrive - MTN Group/Documents/R/Projects/business_plan/exports/plmn_trends.png", device = 'png', dpi = 720, height = 15, width = 32, units = 'cm')


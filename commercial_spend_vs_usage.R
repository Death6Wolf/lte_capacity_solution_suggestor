library(tidyverse)
library(odbc)
library(janitor)
library(lubridate)
library(hrbrthemes)
library(patchwork)
library(DBI)
eva <- dbConnect(odbc::odbc(), "Trino", UID = "laing_r", PWD = "SDsdwe23@#q")

query <- "
 (
 select
    month
    ,'Postpaid' as payment_option
    ,business_unit
    ,case when pp_group='Fixed Internet Plans' then 'y' else 'n' end as flte
    ,sum(spend) as spend
    ,case
         when coalesce(ccn_data_usage_mb,0) >= (90e9/1024/1024) then '≥ 90GB'
         when coalesce(ccn_data_usage_mb,0) >= (60e9/1024/1024) then '< 90GB'
         when coalesce(ccn_data_usage_mb,0) >= (30e9/1024/1024) then '< 60GB'
         when coalesce(ccn_data_usage_mb,0) >= (10e9/1024/1024) then '< 30GB'
         when coalesce(ccn_data_usage_mb,0) >= (1e9/1024/1024) then '< 10GB'
         when coalesce(ccn_data_usage_mb,0) > 0 then '< 1GB'
         when coalesce(ccn_data_usage_mb,0) = 0 then '0GB'
         else 'Unknown'
         end as data_category
     ,sum(ccn_data_usage_mb)*1024*1024/1e9 as ccn_data_usage_gb
     ,count(msisdn) as msisdn_count
 from
        ws_ibro.ibro_postpaid
 where
        month=202305
group by 
    case
         when coalesce(ccn_data_usage_mb,0) >= (90e9/1024/1024) then '≥ 90GB'
         when coalesce(ccn_data_usage_mb,0) >= (60e9/1024/1024) then '< 90GB'
         when coalesce(ccn_data_usage_mb,0) >= (30e9/1024/1024) then '< 60GB'
         when coalesce(ccn_data_usage_mb,0) >= (10e9/1024/1024) then '< 30GB'
         when coalesce(ccn_data_usage_mb,0) >= (1e9/1024/1024) then '< 10GB'
         when coalesce(ccn_data_usage_mb,0) > 0 then '< 1GB'
         when coalesce(ccn_data_usage_mb,0) = 0 then '0GB'
         else 'Unknown'
         end,
    case when pp_group='Fixed Internet Plans' then 'y' else 'n' end,
    business_unit,
    month, 
    'Postpaid'
    )
union ALL
(    
 select
    month
    ,'Prepaid' as payment_option
    ,'CONSUMER' as business_unit
    ,'n' as flte
    ,sum(spend) as spend
    ,case
         when coalesce(ccn_data_usage_mb,0) >= (90e9/1024/1024) then '≥ 90GB'
         when coalesce(ccn_data_usage_mb,0) >= (60e9/1024/1024) then '< 90GB'
         when coalesce(ccn_data_usage_mb,0) >= (30e9/1024/1024) then '< 60GB'
         when coalesce(ccn_data_usage_mb,0) >= (10e9/1024/1024) then '< 30GB'
         when coalesce(ccn_data_usage_mb,0) >= (1e9/1024/1024) then '< 10GB'
         when coalesce(ccn_data_usage_mb,0) > 0 then '< 1GB'
         when coalesce(ccn_data_usage_mb,0) = 0 then '0GB'
         else 'Unknown'
         end as data_category
     ,sum(ccn_data_usage_mb)*1024*1024/1e9 as ccn_data_usage_gb
     ,count(msisdn) as msisdn_count
 from
        ws_ibro.ibro_prepaid
 where
        month=202305
group by 
    case
         when coalesce(ccn_data_usage_mb,0) >= (90e9/1024/1024) then '≥ 90GB'
         when coalesce(ccn_data_usage_mb,0) >= (60e9/1024/1024) then '< 90GB'
         when coalesce(ccn_data_usage_mb,0) >= (30e9/1024/1024) then '< 60GB'
         when coalesce(ccn_data_usage_mb,0) >= (10e9/1024/1024) then '< 30GB'
         when coalesce(ccn_data_usage_mb,0) >= (1e9/1024/1024) then '< 10GB'
         when coalesce(ccn_data_usage_mb,0) > 0 then '< 1GB'
         when coalesce(ccn_data_usage_mb,0) = 0 then '0GB'
         else 'Unknown'
         end,
    'n',
    'CONSUMER',
    month, 
    'Prepaid'
    )
"

df <- dbGetQuery(eva, query) |> 
    collect() |> 
    clean_names() |> 
    as_tibble()

df_summary <- df |> 
    group_by(data_category, payment_option, flte ) |> 
    summarise(
        ccn_data_usage_gb = sum(ccn_data_usage_gb),
        spend = sum(spend),
        msisdn_count = sum(msisdn_count) |> as.double(),
        .groups = 'drop'
    ) |> 
    mutate(
        spend_per_gb = spend/ccn_data_usage_gb,
        spend_per_msisdn = spend/msisdn_count,
        spend_per_gb = if_else(is.infinite(spend_per_gb), 0, spend_per_gb),
        data_category = factor(data_category, levels = c(
            '0GB', 
            '< 1GB', 
            '< 10GB', 
            '< 30GB', 
            '< 60GB', 
            '< 90GB', 
            '≥ 90GB'
        )),
        flte = if_else(flte == 'y', 'FLTE', ''),
        type = paste(payment_option, flte)
    ) 

df_summary |> 
    ggplot(aes(x = data_category, xend = data_category, y= spend_per_gb, yend = spend_per_msisdn)) +
    geom_segment(color="grey30") +
    geom_point( aes(x=data_category, y=spend_per_gb), color=rgb(0.2,0.7,0.1,0.5), size= 5) +
    geom_point( aes(x=data_category, y=spend_per_msisdn), color=rgb(0.7,0.2,0.1,0.5), size= 5) +
    #coord_flip()+
    theme_minimal()+
    xlab("Monthly Data Usage Category") +
    ylab("Spend") +
    labs(title =  "Comparison of Spend per GB vs Spend per MSISDN Count",
         subtitle = 'Red - Spend per MSISDN /nGreen - Spend per GB',
         caption = 'Source: ws_ibro.ibro_prepaid and ws_ibro.ibro_postpaid for May 2023') +
    theme(
        text = element_text(family = 'MTN Brighter Sans')
    ) +
    facet_wrap(~type)

theme_set(theme_minimal())

spend_per_gb_plot <- df_summary |> 
    ggplot(aes(data_category, spend_per_gb)) +
    geom_col(fill = "#30638E", alpha = 0.4) +
    geom_text(aes(label = round(spend_per_gb,2) ), family = 'MTN Brighter Sans', size = 3) +
    facet_grid(type~'Spend per GB') +
    labs(subtitle =  "Spend per GB",
         x = '', 
         y = 'Spend per GB') +
    theme(
        text = element_text(family = 'MTN Brighter Sans'),
        plot.margin = margin(0, 4, 0, 4, "pt")
    ) 

usage_plot <- df_summary |> 
    ggplot(aes(data_category, ccn_data_usage_gb/1e6)) +
    geom_col(fill = "#30638E", alpha = 0.4) +
    geom_text(aes(label = round(ccn_data_usage_gb/1e6,0) ), family = 'MTN Brighter Sans', size = 3) +
    facet_grid(type~'Usage') +
    labs(subtitle =  "Total Data Usage (PB)",
         x = '', 
         y = 'Data Volume (PB)') +
    theme(
        text = element_text(family = 'MTN Brighter Sans'),
        plot.margin = margin(0, 4, 0, 4, "pt")
    ) 

spend_per_msisdn_plot <- df_summary |> 
    ggplot(aes(data_category, spend_per_msisdn)) +
    geom_col(fill = "#D1495B", alpha = 0.4) +
    geom_text(aes(label = round(spend_per_msisdn,2) ), family = 'MTN Brighter Sans', size = 3) +
    facet_grid(type~'Spend per MSISDN') +
    labs(subtitle =  "Spend per MSISDN",
         x = '', 
         y = 'Spend per MSISDN') +
    theme(
        text = element_text(family = 'MTN Brighter Sans'),
        plot.margin = margin(0, 4, 0, 4, "pt")
    ) 

spend_plot <- df_summary |> 
    ggplot(aes(data_category, spend/1e6)) +
    geom_col(fill = "#D1495B", alpha = 0.4) +
    geom_text(aes(label = round(spend/1e6,0) ), family = 'MTN Brighter Sans', size = 3) +
    facet_grid(type~'Spend') +
    labs(subtitle =  "Total Spend",
         x = '', 
         y = 'Spend') +
    theme(
        text = element_text(family = 'MTN Brighter Sans'),
        plot.margin = margin(0, 4, 0, 4, "pt")
    ) 

usage_per_user <- 
    df_summary |> 
    mutate(usage_per_user = round(ccn_data_usage_gb/msisdn_count,2)) |> 
    ggplot(aes(data_category, usage_per_user)) +
    geom_col(fill = "#00798C", alpha = 0.4) +
    geom_text(aes(label = usage_per_user ), family = 'MTN Brighter Sans', size = 3) +
    facet_grid(type~'Usage per MSISDN') +
    labs(subtitle =  "Total Usage (GB)",
         x = '', 
         y = 'Data Volume (GB)') +
    theme(
        text = element_text(family = 'MTN Brighter Sans'),
        plot.margin = margin(0, 4, 0, 4, "pt")
    ) 

msisdn_per_category <- 
    df_summary |> 
    ggplot(aes(data_category, msisdn_count/1e3)) +
    geom_col(fill = "#00798C", alpha = 0.4) +
    geom_text(aes(label = round(msisdn_count/1e3,0) ), family = 'MTN Brighter Sans', size = 3) +
    facet_grid(type~'MSISDN Count') +
    labs(subtitle =  "MSISDN Count (Thousand)",
         x = '', 
         y = 'MSISDN # (K)') +
    theme(
        text = element_text(family = 'MTN Brighter Sans'),
        plot.margin = margin(0, 4, 0, 4, "pt")
    ) 

(msisdn_per_category | usage_plot | spend_plot) / (usage_per_user | spend_per_gb_plot | spend_per_msisdn_plot)

((spend_per_gb_plot / usage_plot) | (spend_per_msisdn_plot/spend_plot) | (usage_per_user/msisdn_per_category))

ggsave("C:/Users/Laing_r/OneDrive - MTN Group/Documents/R/Projects/business_plan/exports/user_usage_vs_spend.png", dpi = 720, device = 'png', width = 40, height = 20, units = "cm")


library(tidyverse)
library(odbc)
library(janitor)
library(lubridate)
library(DBI)
eva <- dbConnect(odbc::odbc(), "Trino", UID = "laing_r", PWD = "SDsdwe23@#q")

query <- "
select 
 tbl_dt,
 site_id, 
 sum(data_mb) as data_mb, 
 count(1) as data_users,
 sum(LTE_FDD_B1) as LTE_FDD_B1_CAPABLE_DATA_USERS,
 sum(LTE_FDD_B3) as LTE_FDD_B3_CAPABLE_DATA_USERS,
 sum(LTE_FDD_B8) as LTE_FDD_B8_CAPABLE_DATA_USERS,
 sum(LTE_TDD_B41) as LTE_TDD_B41_CAPABLE_DATA_USERS,
 sum(WCDMA_FDD_B1) as WCDMA_FDD_B1_CAPABLE_DATA_USERS,
 sum(WCDMA_FDD_B8) as WCDMA_FDD_B8_CAPABLE_DATA_USERS,
 sum(LTE_FDD_B1_CAPABLE_DATA) as LTE_FDD_B1_CAPABLE_DATA,
 sum(LTE_FDD_B3_CAPABLE_DATA) as LTE_FDD_B3_CAPABLE_DATA,
 sum(LTE_FDD_B8_CAPABLE_DATA) as LTE_FDD_B8_CAPABLE_DATA,
 sum(LTE_TDD_B41_CAPABLE_DATA) as LTE_TDD_B41_CAPABLE_DATA,
 sum(WCDMA_FDD_B1_CAPABLE_DATA) as WCDMA_FDD_B1_CAPABLE_DATA,
 sum(WCDMA_FDD_B8_CAPABLE_DATA) as WCDMA_FDD_B8_CAPABLE_DATA
from (
    select
        c.tbl_dt, 
        c.msisdn_key,
        c.tac, 
        c.data_kb*1024/1e6 as data_mb,
        v.site_id, 
        case when regexp_like(r.bands_lte, '(?:,|^)1(?:,|$)') then 1 else 0 end as LTE_FDD_B1,
        case when regexp_like(r.bands_lte, '(?:,|^)3(?:,|$)') then 1 else 0 end as LTE_FDD_B3,
        case when regexp_like(r.bands_lte, '(?:,|^)8(?:,|$)') then 1 else 0 end as LTE_FDD_B8,
        case when regexp_like(r.bands_lte, '(?:,|^)41(?:,|$)') then 1 else 0 end as LTE_TDD_B41,
        case when regexp_like(r.bands_wcdma, '(?:,|^)1(?:,|$)') then 1 else 0 end as WCDMA_FDD_B1,
        case when regexp_like(r.bands_wcdma, '(?:,|^)8(?:,|$)') then 1 else 0 end as WCDMA_FDD_B8,
        case when regexp_like(r.bands_lte, '(?:,|^)1(?:,|$)') then c.data_kb*1024/1e6 else 0 end as LTE_FDD_B1_CAPABLE_DATA,
        case when regexp_like(r.bands_lte, '(?:,|^)3(?:,|$)') then c.data_kb*1024/1e6 else 0 end as LTE_FDD_B3_CAPABLE_DATA,
        case when regexp_like(r.bands_lte, '(?:,|^)8(?:,|$)') then c.data_kb*1024/1e6 else 0 end as LTE_FDD_B8_CAPABLE_DATA,
        case when regexp_like(r.bands_lte, '(?:,|^)41(?:,|$)') then c.data_kb*1024/1e6 else 0 end as LTE_TDD_B41_CAPABLE_DATA,
        case when regexp_like(r.bands_wcdma, '(?:,|^)1(?:,|$)') then c.data_kb*1024/1e6 else 0 end as WCDMA_FDD_B1_CAPABLE_DATA,
        case when regexp_like(r.bands_wcdma, '(?:,|^)8(?:,|$)') then c.data_kb*1024/1e6 else 0 end as WCDMA_FDD_B8_CAPABLE_DATA
    from 
        hive.bsl.customersubject c
        left join hive.s_maps.vw_maps_db_interface v on c.bts_mu_site_id = v.cell_name
        left join (
            select * from hive.s_nms.rflab_gsma g inner join (select max(tbl_dt) as md from hive.s_nms.rflab_gsma) m on g.tbl_dt = m.md
        ) r on c.tac = r.tac  
    where 
        c.tbl_dt = cast(date_format(date_add('day', -1, date_trunc('month', date_add('month', 0, current_date))),'%Y%m%d') as integer)
        and 
        c.aggr = 'monthly'
        and 
        c.status = 'active'
        and 
        data_kb > 0
    )
group by 
 site_id, 
 tbl_dt"

df<- dbGetQuery(eva, query) |>
    collect() |>
    janitor::clean_names() |>
    as_tibble()

df |> 
    mutate(
        lte_b41_data_users_proportion = round(100 * lte_tdd_b41_capable_data_users/data_users, 2), 
        lte_b41_capable_data_proportion = round(100 * lte_tdd_b41_capable_data /data_mb, 2)
    ) |> 
    select(site_id, contains('prop')) |> 
    pivot_longer(!site_id) |> 
    mutate(name = paste(name, '(%)')) |> 
    ggplot(aes(value)) +
    geom_histogram(fill = 'midnightblue') +
    facet_wrap(~name) +
    hrbrthemes::theme_ipsum()


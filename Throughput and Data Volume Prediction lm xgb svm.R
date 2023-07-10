library(tidyverse)
library(odbc)
library(janitor)
library(tidymodels)
library(lubridate)

library(DBI)
maps <- dbConnect(odbc::odbc(), "MapsRadioTenant", UID = "radio-tenant", 
                  PWD = "Maps@radio")

query_op <- "select
startday,
`E-UTRAN IP Throughput UE DL PLMN0_ZA`,
`Availability Rate (Cell)_STD`, 
`4g cell availability (enhanced)_za`,
`nr of available downlink prbs (avg)_std`,
`4G Data Volume DL_STD`,
`4g cell number_std`
from 
v_country_op_mo
where startday >= 20191001"

# query <- "select
# c.startday,
# c.`4G Data Volume DL_STD`
# from 
# v_country_mo c
# where c.startday >= 20191001"
# 
# df <- dbGetQuery(maps, query) |> 
#     collect() |> 
#     clean_names()

df_op <- dbGetQuery(maps, query_op) |> 
    collect() |> 
    clean_names()

stats <- df_op |> 
    # left_join(df_op, by = 'startday') |> 
    mutate(ds = ymd(startday),
           days_months = days_in_month(ds),
           dv_scaled = x4g_data_volume_dl_std/days_months,
           availability = coalesce(x4g_cell_availability_enhanced_za, availability_rate_cell_std)) |> 
    as_tibble()


last_ds <- max(stats$ds)
dv <- as.numeric(stats$dv_scaled[stats$ds == last_ds])
mnths_left <- 12-month(last_ds)
dv_eoy <- dv*1.34^(mnths_left/12)
cells <- as.numeric(stats$x4g_cell_number_std[stats$ds == last_ds])
prbs <- as.numeric(stats$nr_of_available_downlink_prbs_avg_std[stats$ds == last_ds])

stats_predict <- tibble(ds = seq.Date(min(stats$ds), ymd(20241201), 'month')) |> 
    mutate(yr = year(ds)) |> 
    group_by(yr)|> 
    mutate(rn = row_number()) |> 
    ungroup() |> 
    left_join(stats, by = 'ds') |> 
    mutate(
        days_months = days_in_month(ds),
        availability = coalesce(availability, 95),
        mnth = month(ds),
        gr = case_when(
            yr == 2023 ~ 1.34^((mnth-6)/12),
            yr == 2024 ~ 1.36^((mnth)/12),
            TRUE ~ 0
        ),
        type = if_else(is.na(x4g_data_volume_dl_std), 'Predicted', 'Actual'),
        dv_scaled = case_when(
            is.na(dv_scaled) & yr == 2023 ~ dv*gr, 
            yr == 2024 ~ dv_eoy*gr,
            TRUE ~ dv_scaled
        ),
        x4g_data_volume_dl_std = coalesce(x4g_data_volume_dl_std , dv_scaled*days_months),
        x4g_cell_number_std = coalesce(x4g_cell_number_std, cells),
        nr_of_available_downlink_prbs_avg_std = coalesce(nr_of_available_downlink_prbs_avg_std, prbs)
    )


splits <- stats |> initial_split(strata = e_utran_ip_throughput_ue_dl_plmn0_za , prop = 0.9)


lm_m <- linear_reg(mode = 'regression', engine = 'glmnet', penalty = tune(), mixture = tune() )
# xgb_m <- boost_tree(mode = 'regression', engine = 'xgboost')
svm_m <- svm_linear(mode = 'regression', engine = 'kernlab', cost = tune(), margin = tune())

rec_lm <- recipe(e_utran_ip_throughput_ue_dl_plmn0_za ~ 
                     x4g_data_volume_dl_std   + 
                    availability +
                    ds +
                    nr_of_available_downlink_prbs_avg_std +
                     days_months +
                     x4g_cell_number_std , 
                data = training(splits)) |> 
    update_role(c(ds, nr_of_available_downlink_prbs_avg_std, days_months), new_role = 'id')  |> 
    step_mutate(dv_prb = x4g_data_volume_dl_std/(nr_of_available_downlink_prbs_avg_std*days_months),
                dv_prb_cell = x4g_data_volume_dl_std/(nr_of_available_downlink_prbs_avg_std * x4g_cell_number_std * days_months)) |> 
    step_normalize(all_numeric_predictors())

# rec_n <- rec_lm|> 
#     step_date(ds, features = c('month'), label = TRUE) |> 
#     step_dummy(all_nominal_predictors(),one_hot = TRUE)

prep(rec_lm) |> bake(new_data = stats) |> 
    summary()

lm_wf <- workflow() |> 
    add_recipe(rec_lm) |> 
    add_model(lm_m)

svm_wf <- workflow() |> 
    add_recipe(rec_lm) |> 
    add_model(svm_m)

# xgb_wf <- workflow() |> 
#     add_recipe(rec_n) |> 
#     add_model(xgb_m)

cores <- parallel::detectCores(logical = FALSE)
cores
cl <- parallel::makePSOCKcluster(cores)
doParallel::registerDoParallel(cl)
library(finetune)

lm_tune <- finetune::tune_race_anova(lm_wf, resamples = bootstraps(stats, times = 1000, strata = e_utran_ip_throughput_ue_dl_plmn0_za, apparent = TRUE), #not good but what can you do?
                                     grid = 50, control = control_race(verbose = TRUE))
lm_tune |> show_best()

svm_tune <- tune_race_win_loss(svm_wf, 
                               resamples = bootstraps(stats, times = 1000, strata = e_utran_ip_throughput_ue_dl_plmn0_za, apparent = TRUE), #not good but what can you do?
                               grid = 50, 
                               control = control_race(verbose = TRUE, allow_par = TRUE))
svm_tune |> show_best()

lm_wf <- workflow() |> 
    add_recipe(rec_lm) |> 
    add_model(linear_reg(mode = 'regression', engine = 'glmnet', penalty = 0.00000000227   , mixture = 0.125 ))

svm_wf <- workflow() |> 
    add_recipe(rec_lm) |> 
    add_model(svm_linear(mode = 'regression', engine = 'kernlab', cost = 19.5  , margin = 0.151  ))

lm_wf |> last_fit(split = splits) |> collect_metrics()    

# xgb_wf |> last_fit(split = splits) |>  collect_metrics()  |

svm_wf |> last_fit(split = splits) |> collect_metrics()  

results_df <- bind_cols(stats_predict,
lm_wf |> fit(stats) |> predict(stats_predict) |> rename(y_lm = .pred),
# xgb_wf |> fit(stats) |> predict(stats_predict) |> rename(y_xgb = .pred),
svm_wf |> fit(stats) |> predict(stats_predict) |> rename(y_svm = .pred)
)

results_df |> 
    ggplot(aes(x = ds)) +
    geom_line(aes(y = e_utran_ip_throughput_ue_dl_plmn0_za), colour = "black", linewidth = 2) +
    geom_line(aes(y = y_lm), colour = 'blue') +
    # geom_line(aes(y = y_xgb), colour = 'red') +
    geom_line(aes(y = y_svm), colour = 'green') +
    scale_y_continuous(breaks = seq(0,14000, 1000))

results_df |> 
    mutate(y = coalesce(e_utran_ip_throughput_ue_dl_plmn0_za, y_svm)) |> 
    select(ds, e_utran_ip_throughput_ue_dl_plmn0_za = y, availability, x4g_data_volume_dl_std, type) |> 
    pivot_longer(!c(ds, type)) |> 
    mutate(name = str_to_upper(name)) |> 
    ggplot(aes(ds, value, colour = type)) +
    geom_line() +
    facet_wrap(~name, scales = 'free') +
    theme_linedraw()+
    theme(#axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1),
        legend.position = 'bottom',
        text = element_text(family = 'MTN Brighter Sans')) +
    facet_wrap(~name, scales = 'free') +
    scale_color_manual(values = c('Actual' = 'black', 'Predicted' = '#FFC800')) +
    labs(
        x = '', 
        y = '',
        colour = '',
        subtitle = 'LTE Performance Trends',
        caption = 'Enhanced 4G availability values used where available, availability rate cell(std) used where KPI was not defined. Data from maps.v_country_op_mo'
    )

ggsave(filename = "C:/Users/Laing_r/OneDrive - MTN Group/Documents/R/Projects/business_plan/exports/predicted_throughput.png", device = 'png', dpi = 600, height = 14, width = 32, units = 'cm')
doParallel::stopImplicitCluster()


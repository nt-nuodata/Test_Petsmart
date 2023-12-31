# Databricks notebook source
# COMMAND ----------

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.PS2_HTL_FORECAST_PRE(FORECAST_DAY_DT DATE,
LOCATION_ID INT,
WEEK_DT DATE,
STORE_NBR INT,
DAY_OF_WK_NBR INT,
OVERNIGHT_GUEST_CNT INT,
OVERNIGHT_WITH_DDC_CNT INT,
DAY_GUEST_CNT INT,
DAY_CARE_CNT INT,
DAY_CAMP_CNT INT,
TOTAL_DDC_GUEST_CNT INT,
TOTAL_GUEST_CNT INT,
REQUIRED_PLAYROOM_CNT INT) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.PS2_HTL_FORECAST_SWLY_TOTAL_PRE(FORECAST_DAY_DT DATE,
DAY_DT DATE,
LOCATION_ID INT,
SWLY_OVERNIGHT_KITTY_GUEST INT,
SWLY_OVERNIGHT_DOG_GUEST INT,
SWLY_OVERNIGHT_WITH_DDC INT,
SWLY_DAY_CAMP_PLAYROOM INT,
SWLY_DAY_CARE INT) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.PS2_HTL_FORECAST_TY_LY_TOTAL_PRE(FORECAST_DAY_DT DATE,
DAY_DT DATE,
LOCATION_ID INT,
TY_OVERNIGHT_KITTY_GUEST INT,
LY_OVERNIGHT_KITTY_GUEST INT,
TY_OVERNIGHT_DOG_GUEST INT,
LY_OVERNIGHT_DOG_GUEST INT,
TY_OVERNIGHT_WITH_DDC INT,
LY_OVERNIGHT_WITH_DDC INT,
TY_DAY_CAMP_PLAYROOM INT,
LY_DAY_CAMP_PLAYROOM INT,
TY_DAY_CARE INT,
LY_DAY_CARE INT) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.PS2_HTL_WEIGHT(WEEK_NBR INT,
WEIGHT_QTY STRING,
LOAD_TSTMP DATE) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.PS2_HTL_LIMITS(LIMIT_ID INT,
LIMIT_DESC STRING,
LIMIT_QTY INT,
LOAD_TSTMP DATE) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.PS2_HTL_PLAYROOM_CAPACITY(LOCATION_ID INT,
DAY_OF_WEEK STRING,
PLAYROOM_CAP_EFF_DT DATE,
PLAYROOM_CAP_END_DT DATE,
STORE_NBR INT,
NORMAL_HRS_ROOM_QTY INT,
PLAYROOM_01_CAPACITY INT,
PLAYROOM_02_CAPACITY INT,
PLAYROOM_03_CAPACITY INT,
PLAYROOM_04_CAPACITY INT,
PLAYROOM_05_CAPACITY INT,
PLAYROOM_06_CAPACITY INT,
PLAYROOM_07_CAPACITY INT,
PLAYROOM_08_CAPACITY INT,
PLAYROOM_09_CAPACITY INT,
PLAYROOM_10_CAPACITY INT,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.PS2_HTL_OCCUPANCY_DATA_DEFAULT(DAY_DT DATE,
DAY_OF_WK_NBR INT,
OVERNIGHT_KITTY_GUEST INT,
OVERNIGHT_DOG_GUEST INT,
OVERNIGHT_WITH_DDC INT,
DAY_CAMP_CNT INT,
DAY_CARE_CNT INT) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.PS2_HTL_VARIABLES(STORE_NBR INT,
DEFAULT_STORE_FLAG STRING,
PS2_HTL_VAR_REC_EFF_DT DATE,
PS2_HTL_VAR_REC_END_DT DATE,
TY_WEIGHT_PCNT INT,
LY_WEIGHT_PCNT INT,
FRONT_DESK_FIXED_HRS INT,
FRONT_DESK_VAR_HRS INT,
PLAYROOM_VAR_HRS INT,
BACK_OF_HOUSE_FIXED_HRS INT,
BACK_OF_HOUSE_VAR_HRS INT,
OVERNIGHT_TRESHHOLD INT,
OVERNIGHT_UPPER_VALUE INT,
OVERNIGHT_LOWER_VALUE INT,
SUPERVISOR_HRS INT,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.PS2_HTL_FORECAST(FORECAST_DAY_DT DATE,
LOCATION_ID INT,
STORE_NBR INT,
WEEK_DT DATE,
FISCAL_WK INT,
FISCAL_MO INT,
FISCAL_YR INT,
OVERNIGHT_GUEST_CNT INT,
OVERNIGHT_WITH_DDC_CNT INT,
DAY_GUEST_CNT INT,
DAY_CARE_CNT INT,
DAY_CAMP_CNT INT,
TOTAL_DDC_GUEST_CNT INT,
TOTAL_GUEST_CNT INT,
REQUIRED_PLAYROOM_CNT INT,
FRONT_DESK_HRS INT,
PLAYROOM_HRS INT,
BACK_OF_HOUSE_HRS INT,
OVERNIGHT_HRS INT,
SUPERVISOR_HRS INT,
FORECAST_HRS INT,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.PS2_HTL_ETL_CONTROL(PS2_HTL_PROCESS_ID INT,
PS2_HTL_PROCESS_DESC STRING,
PS2_HTL_RUN_DT DATE,
UPDATE_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.PS2_DAYS_TY_LY(DAY_DT TIMESTAMP,
TRANS_DAY_DT TIMESTAMP,
WEEK_DT TIMESTAMP,
FISCAL_YR INT,
TY_LY_FLAG STRING,
COMP_IND INT) USING DELTA;
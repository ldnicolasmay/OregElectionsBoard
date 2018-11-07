# board_elex_spark.R

# |------------------------------| ----
# USEFUL VARS ----
`%>%` <- magrittr::`%>%`


# |------------------------------| ----
# SPARK CONNECTION ----
sc <- sparklyr::spark_connect(master = 'local')


# # |------------------------------| ----
# # # LOAD RAW DATA ----
# 
# reg_vtrs_path <- './OCT 2018 Statewide/RegisteredVoters/'
# cd1_sp <-
#   sparklyr::spark_read_csv(
#     sc = sc,
#     name = 'cd1_sp',
#     path = paste0(reg_vtrs_path, 'CD1_RegisteredVoters_OCT2018.txt'),
#     delimiter = '\t')
# cd1_sp <- cd1_sp %>% 
#   dplyr::mutate(BIRTH_DATE = as.integer(BIRTH_DATE)) %>% 
#   dplyr::mutate(EFF_REGN_DATE = to_date(EFF_REGN_DATE, format = "MM-dd-YYYY"))
# cd1_sp %>% dplyr::distinct(EFF_REGN_DATE)
# cd2_sp <-
#   sparklyr::spark_read_csv(
#     sc = sc,
#     name = 'cd2_sp',
#     path = paste0(reg_vtrs_path, 'CD2_RegisteredVoters_OCT2018.txt'),
#     delimiter = '\t')
# cd3_sp <-
#   sparklyr::spark_read_csv(
#     sc = sc,
#     name = 'cd3_sp',
#     path = paste0(reg_vtrs_path, 'CD3_RegisteredVoters_OCT2018.txt'),
#     delimiter = '\t')
# cd4_sp <-
#   sparklyr::spark_read_csv(
#     sc = sc,
#     name = 'cd4_sp',
#     path = paste0(reg_vtrs_path, 'CD4_RegisteredVoters_OCT2018.txt'),
#     delimiter = '\t')
# cd5_sp <-
#   sparklyr::spark_read_csv(
#     sc = sc,
#     name = 'cd5_sp',
#     path = paste0(reg_vtrs_path, 'CD5_RegisteredVoters_OCT2018.txt'),
#     delimiter = '\t')
# 
# reg_vtrs_sp <- sparklyr::sdf_bind_rows(cd1_sp, cd2_sp, cd3_sp, cd4_sp, cd5_sp)
# reg_vtrs_sp <- sparklyr::sdf_register(reg_vtrs_sp, name = 'reg_vtrs_sp')
# 
# 
# # |------------------------------| ----
# # WRITE DATA TO PARQUET ----
# sparklyr::spark_write_parquet(reg_vtrs_sp,
#                               path = './reg_vtrs_sp.parquet',
#                               mode = 'overwrite')


# |------------------------------| ----
# READ DATA FROM PARQUET ----
reg_vtrs_sp <- 
  sparklyr::spark_read_parquet(sc = sc, 
                               name = 'reg_vtrs_sp', 
                               path = 'reg_vtrs_sp.parquet/')
reg_vtrs_sp %>% dplyr::tbl_vars()
reg_vtrs_sp <- reg_vtrs_sp %>% 
  dplyr::select(-`_c38`)
reg_vtrs_sp %>% dplyr::tbl_vars()

reg_vtrs_sp_hd <- reg_vtrs_sp %>% head(n = 20) %>% dplyr::collect()


# |------------------------------| ----
# INVESTIGATE FIELDS ----

# _ `ABSENTEE_TYPE` ? ----
reg_vtrs_sp %>% 
  dplyr::distinct(ABSENTEE_TYPE)
reg_vtrs_sp %>%
  dplyr::group_by(ABSENTEE_TYPE) %>% 
  dplyr::tally() %>% 
  dplyr::collect()

# _ `CONFIDENTIAL` ? ----
reg_vtrs_sp %>% 
  dplyr::distinct(CONFIDENTIAL)
reg_vtrs_sp %>% 
  dplyr::group_by(CONFIDENTIAL) %>% 
  dplyr::tally() %>% 
  dplyr::collect()
reg_vtrs_df_conf <- reg_vtrs_sp %>% 
  dplyr::filter(CONFIDENTIAL == 'Confidential') %>% 
  dplyr::collect()


# |------------------------------| ----
# PROCESS DATA ----

reg_vtrs_sp %>% 
  dplyr::tally()

# _ Active vs Inactive users ----
reg_vtrs_sp %>% 
  dplyr::distinct(STATUS)
reg_vtrs_sp %>% 
  dplyr::filter(is.na(CONFIDENTIAL)) %>% 
  dplyr::group_by(STATUS) %>% 
  dplyr::tally()

# _ Active voters in 7-county Portland metro area ----
reg_vtrs_sp %>% 
  dplyr::distinct(COUNTY) %>% dplyr::collect() %>% print(., n = 37)
reg_vtrs_sp %>% 
  dplyr::filter(is.na(CONFIDENTIAL)) %>% 
  dplyr::filter(STATUS == 'A') %>% 
  dplyr::filter(COUNTY == 'CLACKAMAS' | 
                  COUNTY == 'COLUMBIA' |
                  COUNTY == 'MULTNOMAH' |
                  COUNTY == 'WASHINGTON' |
                  COUNTY == 'YAMHILL' |
                  COUNTY == 'SKAMANIA') %>%
  dplyr::tally()

# _ Active voters in metro area born 1938 or earlier ----
reg_vtrs_sp <- reg_vtrs_sp %>% 
  dplyr::mutate(BIRTH_DATE = as.integer(BIRTH_DATE))
birth_date_df <- 
  reg_vtrs_sp %>% dplyr::distinct(BIRTH_DATE) %>% dplyr::collect()
sort(birth_date_df$BIRTH_DATE)
class(birth_date_df$BIRTH_DATE)

reg_vtrs_sp %>% 
  dplyr::filter(is.na(CONFIDENTIAL)) %>% 
  dplyr::filter(STATUS == 'A') %>% 
  dplyr::filter(COUNTY == 'CLACKAMAS' | 
                  COUNTY == 'COLUMBIA' |
                  COUNTY == 'MULTNOMAH' |
                  COUNTY == 'WASHINGTON' |
                  COUNTY == 'YAMHILL' |
                  COUNTY == 'SKAMANIA') %>%
  dplyr::filter(BIRTH_DATE <= 1938) %>% 
  dplyr::filter(BIRTH_DATE >= 1900) %>% 
  dplyr::tally() %>% 
  dplyr::collect()

reg_vtrs_metro <- reg_vtrs_sp %>% 
  dplyr::filter(is.na(CONFIDENTIAL)) %>% 
  dplyr::filter(STATUS == 'A') %>% 
  dplyr::filter(COUNTY == 'CLACKAMAS' | 
                  COUNTY == 'COLUMBIA' |
                  COUNTY == 'MULTNOMAH' |
                  COUNTY == 'WASHINGTON' |
                  COUNTY == 'YAMHILL' |
                  COUNTY == 'SKAMANIA') %>%
  dplyr::filter(BIRTH_DATE <= 1938) %>% 
  dplyr::filter(BIRTH_DATE >= 1900) %>% 
  dplyr::collect()

reg_vtrs_metro <- reg_vtrs_metro %>% 
  dplyr::arrange(STREET_TYPE, STREET_NAME, PRE_DIRECTION, 
                 HOUSE_SUFFIX, HOUSE_NUM)

reg_vtrs_metro %>% 
  dplyr::select(STREET_TYPE, STREET_NAME, PRE_DIRECTION, 
                HOUSE_SUFFIX, HOUSE_NUM) %>% 
  dplyr::distinct() %>% 
  nrow()

# _ Active voters in ZIP 97239 born 1938 or earlier ----
reg_vtrs_sp %>% 
  dplyr::filter(is.na(CONFIDENTIAL)) %>% 
  dplyr::filter(STATUS == 'A') %>% 
  dplyr::filter(BIRTH_DATE <= 1938) %>% 
  dplyr::filter(BIRTH_DATE >= 1900) %>% 
  dplyr::filter(ZIP_CODE == '97239' | EFF_ZIP_CODE == '97239') %>% 
  dplyr::tally() %>% 
  dplyr::collect()

reg_vtrs_97239 <- reg_vtrs_sp %>% 
  dplyr::filter(is.na(CONFIDENTIAL)) %>% 
  dplyr::filter(STATUS == 'A') %>% 
  dplyr::filter(BIRTH_DATE <= 1938) %>% 
  dplyr::filter(BIRTH_DATE >= 1900) %>% 
  dplyr::filter(ZIP_CODE == '97239' | EFF_ZIP_CODE == '97239') %>% 
  dplyr::collect()

reg_vtrs_97239 <- reg_vtrs_97239 %>% 
  dplyr::arrange(STREET_TYPE, STREET_NAME, PRE_DIRECTION, 
                 HOUSE_SUFFIX, HOUSE_NUM) 
reg_vtrs_97239 %>% 
  dplyr::select(HOUSE_NUM, HOUSE_SUFFIX, PRE_DIRECTION, 
                STREET_NAME, STREET_TYPE) %>% 
  duplicated() %>% 
  sum()
nrow(reg_vtrs_97239)
reg_vtrs_97239 %>% 
  dplyr::select(HOUSE_NUM, HOUSE_SUFFIX, PRE_DIRECTION, 
                STREET_NAME, STREET_TYPE) %>% 
  dplyr::distinct() %>% 
  nrow()

reg_vtrs_97239 <- reg_vtrs_97239 %>% 
  dplyr::mutate(addr_short = paste(HOUSE_NUM, HOUSE_SUFFIX, PRE_DIRECTION,
                                   STREET_NAME, STREET_TYPE)) %>% 
  dplyr::mutate(addr_short = stringr::str_replace(addr_short, " NA ", " "))

reg_vtrs_97239 %>% 
  dplyr::group_by(addr_short) %>% 
  dplyr::tally() %>% 
  dplyr::arrange(desc(n)) %>% 
  print(n = 50)

reg_vtrs_97239 %>% 
  dplyr::group_by(addr_short) %>% 
  dplyr::tally() %>% 
  dplyr::arrange(desc(n)) %>% 
  dplyr::filter(n < 3) %>% 
  dplyr::summarize(sum = sum(n))

addr_short_lt_3 <- reg_vtrs_97239 %>% 
  dplyr::group_by(addr_short) %>% 
  dplyr::tally() %>% 
  dplyr::arrange(desc(n)) %>% 
  dplyr::filter(n < 3) %>% 
  dplyr::pull(addr_short)

reg_vtrs_97239_filter <- reg_vtrs_97239 %>% 
  dplyr::filter(addr_short %in% addr_short_lt_3) %>% 
  dplyr::select(FIRST_NAME, LAST_NAME, NAME_SUFFIX,
                EFF_ADDRESS_1, EFF_ADDRESS_2, EFF_ADDRESS_3, EFF_ADDRESS_4,
                EFF_CITY, EFF_STATE, EFF_ZIP_CODE, EFF_ZIP_PLUS_FOUR) %>% 
  dplyr::arrange(LAST_NAME, FIRST_NAME)

# readr::write_csv(reg_vtrs_97239_filter, "reg_vtrs_97239_filter.csv", na = "")
xlsx::write.xlsx(reg_vtrs_97239_filter,
                 file = "reg_vtrs_97239_filter.xlsx",
                 # row.names = FALSE, # throws error... why???
                 showNA = FALSE)


# _ Active voters in ZIP 97201 born 1938 or earlier ----
reg_vtrs_sp %>% 
  dplyr::filter(is.na(CONFIDENTIAL)) %>% 
  dplyr::filter(STATUS == 'A') %>% 
  dplyr::filter(BIRTH_DATE <= 1938) %>% 
  dplyr::filter(BIRTH_DATE >= 1900) %>% 
  dplyr::filter(ZIP_CODE == '97201' | EFF_ZIP_CODE == '97201') %>% 
  dplyr::tally() %>% 
  dplyr::collect()

reg_vtrs_97201 <- reg_vtrs_sp %>% 
  dplyr::filter(is.na(CONFIDENTIAL)) %>% 
  dplyr::filter(STATUS == 'A') %>% 
  dplyr::filter(BIRTH_DATE <= 1938) %>% 
  dplyr::filter(BIRTH_DATE >= 1900) %>% 
  dplyr::filter(ZIP_CODE == '97201' | EFF_ZIP_CODE == '97201') %>% 
  dplyr::collect()

reg_vtrs_97201 <- reg_vtrs_97201 %>% 
  dplyr::arrange(STREET_TYPE, STREET_NAME, PRE_DIRECTION, 
                 HOUSE_SUFFIX, HOUSE_NUM) 
reg_vtrs_97201 %>% 
  dplyr::select(HOUSE_NUM, HOUSE_SUFFIX, PRE_DIRECTION, 
                STREET_NAME, STREET_TYPE) %>% 
  duplicated() %>% 
  sum()
nrow(reg_vtrs_97201)
reg_vtrs_97201 %>% 
  dplyr::select(HOUSE_NUM, HOUSE_SUFFIX, PRE_DIRECTION, 
                STREET_NAME, STREET_TYPE) %>% 
  dplyr::distinct() %>% 
  nrow()

reg_vtrs_97201 <- reg_vtrs_97201 %>% 
  dplyr::mutate(addr_short = paste(HOUSE_NUM, HOUSE_SUFFIX, PRE_DIRECTION,
                                   STREET_NAME, STREET_TYPE)) %>% 
  dplyr::mutate(addr_short = stringr::str_replace(addr_short, " NA ", " "))

reg_vtrs_97201 %>% 
  dplyr::group_by(addr_short) %>% 
  dplyr::tally() %>% 
  dplyr::arrange(desc(n)) %>% 
  print(n = 50)

reg_vtrs_97201 %>% 
  dplyr::group_by(addr_short) %>% 
  dplyr::tally() %>% 
  dplyr::arrange(desc(n)) %>% 
  dplyr::filter(n < 3) %>% 
  dplyr::summarize(sum = sum(n))

addr_short_lt_3 <- reg_vtrs_97201 %>% 
  dplyr::group_by(addr_short) %>% 
  dplyr::tally() %>% 
  dplyr::arrange(desc(n)) %>% 
  dplyr::filter(n < 3) %>% 
  dplyr::pull(addr_short)

reg_vtrs_97201_filter <- reg_vtrs_97201 %>% 
  dplyr::filter(addr_short %in% addr_short_lt_3) %>% 
  dplyr::select(FIRST_NAME, LAST_NAME, NAME_SUFFIX,
                EFF_ADDRESS_1, EFF_ADDRESS_2, EFF_ADDRESS_3, EFF_ADDRESS_4,
                EFF_CITY, EFF_STATE, EFF_ZIP_CODE, EFF_ZIP_PLUS_FOUR) %>% 
  dplyr::arrange(LAST_NAME, FIRST_NAME)

# readr::write_csv(reg_vtrs_97201_filter, "reg_vtrs_97201_filter.csv", na = "")
xlsx::write.xlsx(reg_vtrs_97201_filter,
                 file = "reg_vtrs_97201_filter.xlsx",
                 # row.names = FALSE, # throws error... why???
                 showNA = FALSE)


# _ Active voters in ZIP 97214 born 1938 or earlier ----

# _ Active voters in ZIP 97202 born 1938 or earlier ----

# _ Active voters in ZIP 97219 born 1938 or earlier ----

# _ Active voters in ZIP 97221 born 1938 or earlier ----

# _ Active voters in ZIP 97205 born 1938 or earlier ----


# |------------------------------ ----
# | US CENSUS DATA                ----
# 2010 CENSUS DATA ----

# Get data
census <- readr::read_csv("./aff_download/DEC_10_DP_DPDP1_with_ann.csv")
census <- census[-1, ] # eliminate subheader row
head(census[, 1:5]) 
census_hd <- head(census)

# Mutate ZCTA5 (ZIP) from `GEO.id`
census <- census %>% 
  dplyr::mutate(ZCTA5 = stringr::str_sub(GEO.id, -5))
census_hd <- head(census)

# Load metadata (subheaders too cumbersome to use)
census_meta <- readr::read_csv("./aff_download/DEC_10_DP_DPDP1_metadata.csv")

# Get column names that correspond to race population data
# Filter out rows with `(X)` for African American percentage column
col_vars <- census_meta[153:200, "GEO.id"] %>% dplyr::pull(GEO.id)
census_race <- census[, c("GEO.id", "GEO.id2", "GEO.display-label", "ZCTA5",
                          col_vars)]
census_race_aa <- census_race %>% 
  dplyr::select(ZCTA5, HD01_S079, HD02_S079) %>% 
  dplyr::filter(HD02_S079 != "(X)") %>% 
  dplyr::mutate(HD01_S079 = as.integer(HD01_S079),
                HD02_S079 = as.numeric(HD02_S079)) %>% 
  dplyr::arrange(desc(HD02_S079), desc(HD01_S079))


# DATA JOIN ----
# X = counties, Y = census_race_aa, by = `zip`
reg_vtrs_sp %>% dplyr::distinct(ZIP_CODE) %>% dplyr::pull()
unique(census_race_aa$ZCTA5)
census_race_aa <- census_race_aa %>% 
  dplyr::rename(ZIP_CODE = ZCTA5)
census_race_aa_tbl <- dplyr::copy_to(sc,
                                     df = census_race_aa,
                                     name = "census_race_aa_tbl",
                                     overwrite = TRUE)
counties_aa_tbl <-
  dplyr::left_join(reg_vtrs_sp, census_race_aa_tbl, by = c("ZIP_CODE"))
dplyr::glimpse(counties_aa_tbl)

counties_aa_tbl %>% 
  dplyr::group_by(COUNTY) %>% 
  dplyr::summarise(N = n())

counties_aa_tbl %>% dplyr::tbl_vars()


# FILTER DATA ----
# Get all voters in ZIP codes with 
# >= XX.X% AfrAmer population
# >= 80 years old
aa_perc_thresh <- 0.00
nearby_zips <- c(97239L, 97201L, 97214L, 97202L, 97219L, 97221L, 97205L)
counties_aa_filter_tbl <- counties_aa_tbl %>%
  dplyr::mutate(BIRTH_DATE = as.integer(BIRTH_DATE),
                HD02_S079 = as.numeric(HD02_S079)) %>%
  # dplyr::filter(BIRTH_DATE <
  #                 as.integer(stringr::str_sub(Sys.Date(), 1, 4)) - 80) %>%
  # dplyr::filter(HD02_S079 >= aa_perc_thresh) %>% 
  # dplyr::filter(EFF_REGN_DATE >= lubridate::as_date("2000-01-01")) %>%
  dplyr::filter(ZIP_CODE %in% nearby_zips) 

counties_aa_filter <- counties_aa_filter_tbl %>% dplyr::collect()

dplyr::glimpse(counties_aa_filter_tbl)
counties_aa_filter_tbl %>% 
  dplyr::distinct(BIRTH_DATE) %>%
  dplyr::arrange(BIRTH_DATE) %>% 
  dplyr::collect() %>% 
  print(n = 100)
counties_aa_filter_tbl %>% 
  dplyr::distinct(EFF_REGN_DATE) %>% 
  dplyr::arrange(EFF_REGN_DATE) %>% 
  dplyr::collect() %>% 
  print(n = 50)
dplyr::glimpse(counties_aa_tbl)


# |------------------------------ ----
# DECISION TREE MODEL PREDICTING PARTY AFFILIATION ----
model_fields <- c('BIRTH_DATE', 'COUNTY', 'STREET_TYPE', 'CITY', 'PRECINCT')

reg_vtrs_sp_sample <- reg_vtrs_sp %>%
  dplyr::sample_n(size = 10000) %>% 
  dplyr::select(PARTY_CODE, BIRTH_DATE, COUNTY, 
                STREET_TYPE, CITY, PRECINCT) %>% 
  sparklyr::ft_string_indexer("PARTY_CODE", "PARTY_CODE_IDX") %>%
  sparklyr::ft_string_indexer("COUNTY", "COUNTY_IDX") %>%
  # sparklyr::ft_string_indexer("STREET_TYPE", "STREET_TYPE_IDX") %>%
  sparklyr::ft_string_indexer("CITY", "CITY_IDX") %>%
  dplyr::mutate(BIRTH_DATE = as.numeric(BIRTH_DATE)) %>% 
  dplyr::mutate(PRECINCT = as.numeric(PRECINCT)) %>% 
  sparklyr::sdf_copy_to(sc = sc, x = ., 
                        name = 'reg_vtrs_sp_sample', overwrite = TRUE)
partitions <- reg_vtrs_sp_sample %>%  
  sparklyr::sdf_partition(training = 0.7, test = 0.3, seed = 1111)

reg_vtrs_sp_training <- partitions$training
reg_vtrs_sp_test <- partitions$test

dt_model <- reg_vtrs_sp_training %>%
  sparklyr::ml_decision_tree(
    formula = PARTY_CODE ~ COUNTY_IDX + CITY_IDX + PRECINCT + STREET_TYPE)

pred <- sparklyr::sdf_predict(x = reg_vtrs_sp_test, model = dt_model)

sparklyr::ml_multiclass_classification_evaluator(x = pred)
# sparklyr::ml_binary_classification_evaluator(x = pred)

# |------------------------------ ----
# DISCONNECT SPARK ----
sparklyr::spark_disconnect(sc)


















nids_xipamanine <- filter(df_pat_old, mainclinicname=="CS Xipamanine") %>% select(patientid)
vec_nids_xipamanine <- vec.to.sql.in.clause(nids_xipamanine)
con_postgres_xipamanine <- get.postgres.conection("xipamanine")
df_prescriptions_xipamanine  <- get.patient.last.prescription(con_postgres_xipamanine,vec_nids_xipamanine)
df_prescriptions_xipamanine  <- distinct(df_prescriptions_xipamanine, patientid, .keep_all = TRUE)
df_prescriptions_xipamanine$mainclinicname <- "CS Xipamanine"


nids_altomae <- filter(df_pat_old, mainclinicname=="CS Alto Mae")  %>% select(patientid)
vec_nids_altomae <- vec.to.sql.in.clause(nids_altomae)
con_postgres_altomae <- get.postgres.conection("altomae")
df_prescriptions_altomae  <- get.patient.last.prescription(con_postgres_altomae,vec_nids_altomae)
df_prescriptions_altomae  <- distinct(df_prescriptions_altomae, patientid, .keep_all = TRUE)
df_prescriptions_altomae$mainclinicname ="CS Alto Mae"





nids_bagamoio <- filter(df_pat_old, mainclinicname=="CS Bagamoio")  %>% select(patientid)
vec_nids_bagamoio <- vec.to.sql.in.clause(nids_bagamoio)
con_postgres_bagamoio <- get.postgres.conection("bagamoio")
df_prescriptions_bagamoio  <- get.patient.last.prescription(con_postgres_bagamoio,vec_nids_bagamoio)
df_prescriptions_bagamoio  <- distinct(df_prescriptions_bagamoio, patientid, .keep_all = TRUE)
df_prescriptions_bagamoio$mainclinicname = "CS Bagamoio"



nids_chamanculo <- filter(df_pat_old, mainclinicname=="CS Chamanculo")  %>% select(patientid)
vec_nids_chamanculo <- vec.to.sql.in.clause(nids_chamanculo)
con_postgres_chamanculo  <- get.postgres.conection("chamanculo")
df_prescriptions_chamanculo   <- get.patient.last.prescription(con_postgres_chamanculo ,vec_nids_chamanculo )
df_prescriptions_chamanculo  <- distinct(df_prescriptions_chamanculo, patientid, .keep_all = TRUE)
df_prescriptions_chamanculo$mainclinicname <- "CS Chamanculo"

nids_porto <- filter(df_pat_old, mainclinicname=="CS PORTO")  %>% select(patientid)
vec_nids_porto <- vec.to.sql.in.clause(nids_porto)
con_postgres_porto <- get.postgres.conection("porto")
df_prescriptions_porto  <- get.patient.last.prescription(con_postgres_porto ,vec_nids_porto )
df_prescriptions_porto  <- distinct(df_prescriptions_porto, patientid, .keep_all = TRUE)
df_prescriptions_porto$mainclinicname <- "CS PORTO"


nids_albasine <- filter(df_pat_old, mainclinicname=="CS ALBASINE")  %>% select(patientid)
vec_nids_albasine <- vec.to.sql.in.clause(nids_albasine)
con_postgres_albasine <- get.postgres.conection("albasine")
df_prescriptions_albasine   <- get.patient.last.prescription(con_postgres_albasine ,vec_nids_albasine )
df_prescriptions_albasine  <- distinct(df_prescriptions_albasine, patientid, .keep_all = TRUE)
df_prescriptions_albasine$mainclinicname <- "CS ALBASINE"


df_all_prescription <- rbind.fill(df_prescriptions_albasine,df_prescriptions_chamanculo,df_prescriptions_porto,
                                  df_prescriptions_bagamoio,df_prescriptions_altomae,df_prescriptions_xipamanine)

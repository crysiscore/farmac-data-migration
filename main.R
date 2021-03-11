
require(RPostgreSQL)
require(plyr)
require(dplyr)
setwd('/home/agnaldo/Git/iDART/farmac_data_migration/')
# Configuracao de Parametros  #

postgres.user ='postgres'        
postgres.password='postgres' 
postgres.db.name='central'                  
postgres.host='172.18.0.3'                  
postgres.port=5432                        


postgres.old.user ='postgres'        
postgres.old.password='postgres' 
postgres.old.db.name='pharm'                  
postgres.old.host='172.18.0.3'                  
postgres.old.port=5432                        

source('generic_functions.R')          

# Objecto de connexao com a bd openmrs postgreSQL
con_postgres_old <-  dbConnect(PostgreSQL(),user = postgres.old.user,password = postgres.old.password, dbname = postgres.old.db.name,host = postgres.old.host)

con_postgres_new <-  dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)

df_pat_new <- get.all.patients.farmac(con_postgres_new)
df_pat_old <- get.all.patients.farmac(con_postgres_old)

df_dispense_old <- dbGetQuery( con_postgres_old,   paste0(    "SELECT *  FROM sync_temp_dispense ; "  )  )
last_id <-  dbGetQuery( con_postgres_new,   paste0(    "SELECT id  FROM sync_temp_patients order by id desc limit 1 "  )  )
last_id <- last_id$id


# formatacao de datas

df_dispense_old$dispensedate <- as.Date(as.POSIXct(df_dispense_old$dispensedate, 'GMT'))
df_dispense_old$dateexpectedstring <- as.Date(df_dispense_old$dateexpectedstring, "%d %b %Y")

df_pat_old$insert_result <- ""
df_pat_old$prescriptiondate <- Sys.time()
df_pat_old$prescriptionenddate <- Sys.time() + 90*24*60*60
df_pat_old$regimenome <- ""
df_pat_old$duration <- 12
df_pat_old$dispensatrimestral <- ""
df_pat_old$prescriptionid <- ""


# prenche dados em falta a partir da sync_temp_dispense

for (i in 1:nrow(df_pat_old)) {
  
  nid <- df_pat_old$patientid[i]
    
  dispenses <-  filter(df_dispense_old, patientid == nid) %>% arrange(desc(dispensedate))
  if(nrow(dispenses) > 0){
    prescriptiondate <- dispenses$pickupdate[1]
    prescriptionenddate <- prescriptiondate + 90*24*60*60
    regimenome <- dispenses$regimeid[1]
    dispensatrimestral <- dispenses$dispensatrimestral[1]
    year <- substr(as.character(prescriptiondate), 3 , 4)
    month <- substr(as.character(prescriptiondate), 6 , 7)
    day <- substr(as.character(prescriptiondate), 9 , 10)
    prescriptionid <- paste0(year,month,day,"A-",nid)
    
    df_pat_old$prescriptiondate[i] <- prescriptiondate
    df_pat_old$prescriptionenddate[i] <- prescriptionenddate
    df_pat_old$regimenome[i] <- regimenome
    df_pat_old$dispensatrimestral[i] <- dispensatrimestral
    df_pat_old$prescriptionid[i] <- prescriptionid
    
    last_id        <- last_id + 3
    cellphone      <- df_pat_old$cellphone[i]
    dateofbirth    <- df_pat_old$dateofbirth[i] 
    clinicname     <- df_pat_old$clinicname[i]
    mainclinic     <- df_pat_old$mainclinic[i] 
    mainclinicname <- df_pat_old$mainclinicname[i] 
    firstnames     <- df_pat_old$firstnames[i] 
    homephone      <- df_pat_old$homephone[i] 
    lastname       <- df_pat_old$lastname[i]   
    modified       <- df_pat_old$modified[i]
    patientid      <- df_pat_old$patientid[i]   
    #province       <- df_pat_old$province[i]   #  update using script
    sex            <- df_pat_old$sex[i] 
    workphone      <- df_pat_old$workphone[i]
    address1       <- df_pat_old$address1[i]  
    address2       <- df_pat_old$address2[i]  
    address3       <- df_pat_old$address3[i]  
    nextofkinname  <- df_pat_old$nextofkinname[i]  
    nextofkinphone <- df_pat_old$nextofkinphone[i] 
    race           <- df_pat_old$race[i] 
    uuidopenmrs    <- df_pat_old$uuid[i] 
    datainiciotarv <- df_pat_old$datainiciotarv[i]   
    #syncstatus     <- df_pat_old$syncstatus[i]      
    #syncuuid       <- df_pat_old$syncuuid[i]  
    #clinicuuid     <- df_pat_old$clinicuuid[i]  
    #mainclinicuuid <- df_pat_old$mainclinicuuid[i] 
    #jsonprescribeddrugs <- df_pat_old$jsonprescribeddrugs[i]   
    prescriptiondate <- df_pat_old$prescriptiondate[i]  
    #duration       <- df_pat_old$duration[i] 
    prescriptionenddate <- df_pat_old$prescriptionenddate[i]
    regimenome     <- df_pat_old$regimenome[i]  
    #linhanome      <- df_pat_old$linhanome[i]  
    dispensatrimestral <- df_pat_old$dispensatrimestral[i] 
    #dispensasemestral <- df_pat_old$dispensasemestral[i] 
    prescriptionid <- df_pat_old$prescriptionid[i]   
    #prescricaoespecial <- df_pat_old$prescricaoespecial[i]  
    #motivocriacaoespecial <- df_pat_old$motivocriacaoespecial[i]  
    
    sql_insert <- "INSERT INTO public.sync_temp_patients(
    id, accountstatus, cellphone, dateofbirth, clinic, clinicname, 
    mainclinic, mainclinicname, firstnames, homephone, lastname, 
    modified, patientid,  sex, workphone, address1, address2, 
    address3, nextofkinname, nextofkinphone, race, uuidopenmrs, datainiciotarv,syncstatus,
    prescricaoespecial,prescriptiondate,prescriptionenddate,regimenome,dispensatrimestral,prescriptionid)
    VALUES ("
    
    temp_sql <- paste0( last_id,  " ,",  " FALSE , '" , cellphone, "' , '", dateofbirth, "' , " , 0 ,  " , '" , clinicname, "' , ",mainclinic , " , '",
                        mainclinicname , "' , '", firstnames, "' , '", homephone , "' , '", lastname , "' , '", modified ,  "' , '" , patientid , "' , '" , sex, "' , '",
                        workphone, "' , '", address1 , "' , '" , address2 , "' , '", address3 ,  "' , '", nextofkinname , "' , '" , nextofkinphone, "' , '" , race , "' , '" ,
                        uuidopenmrs , "' , '" , datainiciotarv,  "' , '", "I" ,  "' , '", "F", "' , '", prescriptiondate , "' , '",
                        prescriptionenddate, "' , '" ,regimenome, "' , " ,dispensatrimestral, " , '" ,prescriptionid,  "' ) ;" )
    sql <- paste0(sql_insert,temp_sql)
    df_pat_old$insert_result[i] <- dbExecute(con_postgres_new,sql)
    #print(sql)
    write(sql,file='sql_farmac_insert_old_patients.sql',append=TRUE)
    
    
  } else {
    
    last_id        <- last_id + 3
    cellphone      <- df_pat_old$cellphone[i]
    dateofbirth    <- df_pat_old$dateofbirth[i] 
    clinicname     <- df_pat_old$clinicname[i]
    mainclinic     <- df_pat_old$mainclinic[i] 
    mainclinicname <- df_pat_old$mainclinicname[i] 
    firstnames     <- df_pat_old$firstnames[i] 
    homephone      <- df_pat_old$homephone[i] 
    lastname       <- df_pat_old$lastname[i]   
    modified       <- df_pat_old$modified[i]
    patientid      <- df_pat_old$patientid[i]   
    #province       <- df_pat_old$province[i]   #  update using script
    sex            <- df_pat_old$sex[i] 
    workphone      <- df_pat_old$workphone[i]
    address1       <- df_pat_old$address1[i]  
    address2       <- df_pat_old$address2[i]  
    address3       <- df_pat_old$address3[i]  
    nextofkinname  <- df_pat_old$nextofkinname[i]  
    nextofkinphone <- df_pat_old$nextofkinphone[i] 
    race           <- df_pat_old$race[i] 
    uuidopenmrs    <- df_pat_old$uuid[i] 
    datainiciotarv <- df_pat_old$datainiciotarv[i]   
    #syncstatus     <- df_pat_old$syncstatus[i]      
    #syncuuid       <- df_pat_old$syncuuid[i]  
    #clinicuuid     <- df_pat_old$clinicuuid[i]  
    #mainclinicuuid <- df_pat_old$mainclinicuuid[i] 
    #jsonprescribeddrugs <- df_pat_old$jsonprescribeddrugs[i]   
    #prescriptiondate <- df_pat_old$prescriptiondate[i]  
    #duration       <- df_pat_old$duration[i] 
    #prescriptionenddate <- df_pat_old$prescriptionenddate[i]
    #regimenome     <- df_pat_old$regimenome[i]  
    #linhanome      <- df_pat_old$linhanome[i]  
    #dispensatrimestral <- df_pat_old$dispensatrimestral[i] 
    #dispensasemestral <- df_pat_old$dispensasemestral[i] 
    #prescriptionid <- df_pat_old$prescriptionid[i]   
    #prescricaoespecial <- df_pat_old$prescricaoespecial[i]  
    #motivocriacaoespecial <- df_pat_old$motivocriacaoespecial[i]  
    sql_insert <- "INSERT INTO public.sync_temp_patients(
    id, accountstatus, cellphone, dateofbirth, clinic, clinicname, 
    mainclinic, mainclinicname, firstnames, homephone, lastname, 
    modified, patientid,  sex, workphone, address1, address2, 
    address3, nextofkinname, nextofkinphone, race, uuidopenmrs, datainiciotarv,syncstatus  )
    VALUES ("
    temp_sql <- paste0( last_id,  " ,",  " FALSE , '" , cellphone, "' , '", dateofbirth, "' , " , 0 ,  " , '" , clinicname, "' , ",mainclinic , " , '",
                        mainclinicname , "' , '", firstnames, "' , '", homephone , "' , '", lastname , "' , '", modified ,  "' , '" , patientid , "' , '" , sex, "' , '",
                        workphone, "' , '", address1 , "' , '" , address2 , "' , '", address3 ,  "' , '", nextofkinname , "' , '" , nextofkinphone, "' , '" , race , "' , '" ,
                        uuidopenmrs , "' , '" , datainiciotarv,  "' , '", "I" ,   "' ) ;" )
    sql <- paste0(sql_insert,temp_sql)
    df_pat_old$insert_result[i] <- dbExecute(con_postgres_new,sql)
    #print(sql)
    write(sql,file='sql_farmac_insert_old_patients.sql',append=TRUE)
  }
}



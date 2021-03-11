
require(RPostgreSQL)
#' Busca todos pacientes referidos para FARMAC
#' 
#' @param con.postgres  obejcto de conexao com BD iDART
#' @return tabela/dataframe/df com pacientes ref as FARMAC
#' @examples pat_farmac <- getAllPatientsFarmac(con_idart)
get.all.patients.farmac <- function(con.postgres) {
  
  tryCatch({
    
    patients  <-
      dbGetQuery(
        con.postgres,
        paste0(
          "SELECT  *   FROM sync_temp_patients; "
        )
      )
    
    return(patients)
    
  },error = function(cond) {
    
    message(cond)
    #imprimir a mgs a consola
    # Choose a return value in case of error
    return(0)
  },warning = function(cond) {
    message("Here's the original warning message:")
    message(cond)
    # Choose a return value in case of warning
    return(patients)
  },finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # regardless of success or error.
    # If you want more than one expression to be executed, then you
    # need to wrap them in curly brackets ({...}); otherwise you could
    # just have written 'finally=<expression>'
    
  })
  
  
  return(patients)
  
}


get.patient.last.prescription <- function(con.postgres, vec.nids) {
   # main_sql <- "select  main.patientid,main.firstnames, main.lastname, main.uuidopenmrs,  main.prescriptionid,  main.date , main.enddate, main.duration, main.current,  main.dispensatrimestral ,last_pdt.dispensedate, last_pdt.drugname,last_pdt.prescriptionduration as last_pdt_duration from ( select  p.patientid,firstnames, lastname, uuidopenmrs,  prescriptionid,  date , enddate, duration, current,  dispensatrimestral from patient p inner join prescription pr on p.id = pr.patient where current = 'T' and endDate is null ) main 
   #             left join (  select last.patientid, last.dispensedate, pdt.drugname,pdt.prescriptionduration 
   #             from  ( select patientid, max(dispensedate) as dispensedate       from packagedruginfotmp  group by patientid  ) last     
   #             inner join packagedruginfotmp pdt  on pdt.dispensedate = last.dispensedate and pdt.patientid=last.patientid ) last_pdt on last_pdt.patientid = main.patientid where main.patientid in "

  main_sql <- "select  main.patientid,main.firstnames, main.lastname, main.uuidopenmrs,  main.prescriptionid,  main.date , main.enddate, main.duration, main.current,  main.dispensatrimestral ,last_pdt.dispensedate, last_pdt.drugname,last_pdt.prescriptionduration as last_pdt_duration
 from ( select  p.patientid,firstnames, lastname, uuidopenmrs,  pr.prescriptionid,  pr.date , pr.enddate, duration, current,  dispensatrimestral
	from
         ( select patient, max(date) as dispensedate
                from prescription  where current = 'T' and endDate is null   group by patient  ) last_pr
                inner join prescription pr on last_pr.dispensedate =pr.date and pr.patient =last_pr.patient 
                inner join patient p on p.id = pr.patient
                  where current = 'T' and endDate is null

              ) main              
	
               left join (  select last.patientid, last.dispensedate, pdt.drugname,pdt.prescriptionduration 
               from  ( select patientid, max(dispensedate) as dispensedate       from packagedruginfotmp  group by patientid  ) last     
               inner join packagedruginfotmp pdt  on pdt.dispensedate = last.dispensedate and pdt.patientid=last.patientid ) last_pdt on last_pdt.patientid = main.patientid where main.patientid in "
  
    tryCatch({
    
    patients  <-    dbGetQuery(   con.postgres,  paste0(main_sql, vec.nids)   )
    
    return(patients)
    
  },error = function(cond) {
    
    message(cond)
    #imprimir a mgs a consola
    # Choose a return value in case of error
    return(0)
  },warning = function(cond) {
    message("Here's the original warning message:")
    message(cond)
    # Choose a return value in case of warning
    return(patients)
  },finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # regardless of success or error.
    # If you want more than one expression to be executed, then you
    # need to wrap them in curly brackets ({...}); otherwise you could
    # just have written 'finally=<expression>'
    
  })
  
  
  return(patients)
  
}


vec.to.sql.in.clause <- function(vec){
  vec_item <- "("
  
  for (i in 1:dim(vec)[1]) {
    
    item <- vec$patientid[i]
    
    if (i==1) {
      
      vec_item <- paste0(vec_item, " '",item, "' ")
      
    } else if(i==dim(vec)[1]){
      
      vec_item <- paste0(vec_item, ", '",item, "' )")
    }else{
      
      vec_item <- paste0(vec_item, ", '",item, "' ")
      
    }
    
  }
  
  vec_item
}


get.postgres.conection <- function(db.name) {
  
  if(db.name=="central"){
    
    # Configuracao de Parametros  #
    # Central
    postgres.user ='postgres'        
    postgres.password='postgres' 
    postgres.db.name='central'                  
    postgres.host='172.18.0.3'                  
    postgres.port=5432                        
    con <-  dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)
    return(con)
    
  } else if (db.name=="pharm"){
    # Old pharm
    postgres.old.user ='postgres'        
    postgres.old.password='postgres' 
    postgres.old.db.name='pharm'                  
    postgres.old.host='172.18.0.3'                  
    postgres.old.port=5432                        
    con <- dbConnect(PostgreSQL(),user = postgres.old.user,password = postgres.old.password, dbname = postgres.old.db.name,host = postgres.old.host)
    return(con)
  }else if (db.name=="albasine"){

    # albasine 
    postgres.user ='postgres'        
    postgres.password='postgres' 
    postgres.db.name='albasine'                  
    postgres.host='172.18.0.3'                  
    postgres.port=5432                        
    con <-dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)
    return(con)
  }else if (db.name=="porto"){
   
    # porto 
    postgres.user ='postgres'        
    postgres.password='postgres' 
    postgres.db.name='porto'                  
    postgres.host='172.18.0.3'                  
    postgres.port=5432                        
    con <-dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)
    return(con)
  }else if (db.name=="bagamoio"){
    
    
    # bagamoio 
    postgres.user ='postgres'        
    postgres.password='postgres' 
    postgres.db.name='bagamoio'                  
    postgres.host='172.18.0.3'                  
    postgres.port=5432                          
    con <- dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)
    return(con)
  }else if (db.name=="xipamanine"){

    # xipamanine 
    postgres.user ='postgres'        
    postgres.password='postgres' 
    postgres.db.name='xipamanine'                  
    postgres.host='172.18.0.3'                  
    postgres.port=5432                  
    con <- dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)
    return(con)
  }else if (db.name=="chamanculo"){
    
    # chamanculo 
    postgres.user ='postgres'        
    postgres.password='postgres' 
    postgres.db.name='chamanculo'                  
    postgres.host='172.18.0.3'                  
    postgres.port=5432                
    con <- dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)
    return(con)
  }else if (db.name=="altomae"){
    # altomae 
    postgres.user ='postgres'        
    postgres.password='postgres' 
    postgres.db.name='altomae'                  
    postgres.host='172.18.0.3'                  
    postgres.port=5432                   
    con <- dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)
    return(con)
  }
  
  
}

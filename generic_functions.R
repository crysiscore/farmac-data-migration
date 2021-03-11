

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

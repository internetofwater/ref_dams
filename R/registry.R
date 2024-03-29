build_registry <- function(gl, registry, providers) {
  
  reg <- read_csv(registry)
  
  gl <- left_join(gl, select(providers, provider_int = id, provider), by = "provider")
  
  gl <- select(convert_coords(gl), provider = provider_int, provider_id)
  
  if(nrow(reg) == 1) {
    message("initialize")
    
    reg$id <- as.numeric(reg$id)
    reg$provider <- as.numeric(reg$provider)
    reg$provider_id <- as.character(reg$provider_id)
    gl$id <- 1000000 + c(1:nrow(gl))
    
  } else if(nrow(reg) < nrow(gl)) {
    
    gl <- distinct(gl)
    
    if(any(duplicated(gl$provider_id))) stop("found duplicate ids")
    
    gl <- dplyr::filter(gl, 
                        !paste0(gl$provider, gl$provider_id) %in% 
                          paste0(reg$provider, reg$provider_id))
    
    gl$id <- seq(max(reg$id), (max(reg$id) + nrow(gl) - 1))
    
    message(paste("Adding", nrow(gl), "to the registry."))
    
  } else if(nrow(reg) == nrow(gl)) {
    return(reg)
  } else {
    if(all(paste0(gl$provider, gl$provider_id) %in% 
           paste0(reg$provider, reg$provider_id))) {
      return(reg)
    } else {
      stop("gl should not be shorter than reg.")
    }
  }
  
  bind_rows(reg, gl)
  
}

convert_coords <- function(gl) {
  coords <- st_coordinates(gl)
  gl$lon <- coords[, 1]
  gl$lat <- coords[, 2]
  
  st_drop_geometry(gl)
}

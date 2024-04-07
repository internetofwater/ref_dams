write_reference <- function(dam_locations, registry, providers, reference_file, nldi_file) {
  
  if(is.numeric(dam_locations$nhdpv2_COMID)) dam_locations$nhdpv2_COMID <-
      vapply(dam_locations$nhdpv2_COMID, \(x) {
        if(is.na(x)) NA_character_ else paste0("https://geoconnex.us/nhdplusv2/comid/", x)
      }, FUN.VALUE = c(""))
  
  if(all(!grepl("https", dam_locations$nhdpv2_REACHCODE))) {
    dam_locations$nhdpv2_REACHCODE <-
      vapply(dam_locations$nhdpv2_REACHCODE, \(x) {
        if(is.na(x)) NA_character_ else paste0("https://geoconnex.us/nhdplusv2/reachcode/", x)
      }, FUN.VALUE = c(""))
  }
  
  out <- dam_locations %>%
    mutate(identifier = paste0(provider, provider_id)) %>%
    left_join(select(convert_provider_id(registry, providers), 
                     uri, identifier, id), by = "identifier") %>%
    select(id, uri, name, description, subjectOf, 
           provider, provider_id, 
           nhdpv2_COMID, nhdpv2_REACHCODE, nhdpv2_REACH_measure,
           drainage_area_sqkm, drainage_area_sqkm_nhdpv2, index_type,
           mainstem_uri, feature_data_source) %>%
    mutate(id = as.integer(id))
  
  write_sf(out, reference_file)
  
  unlink(nldi_file)
  write_sf(out, nldi_file)
  
  out
}

write_registry <- function(registry, registry_file) {
  write_csv(registry, registry_file)
  
  registry_file
}

convert_provider_id <- function(registry, providers) {
  rename(registry, prov_id = provider) %>%
    left_join(select(providers, prov_id = id, provider), 
              by = "prov_id") %>%
    mutate(identifier = paste0(provider, provider_id)) %>%
    mutate(uri = paste0("https://geoconnex.us/ref/dams/", id)) %>%
    select(-prov_id)
}



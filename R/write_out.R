write_reference <- function(dam_locations, registry, providers, reference_file, nldi_file) {
  
  out <- dam_locations %>%
    mutate(identifier = paste0(provider, provider_id)) %>%
    left_join(select(convert_provider_id(registry, providers), 
                     uri, identifier, id), by = "identifier") %>%
    select(id, uri, name, description, subjectOf, 
           provider, provider_id, nhdpv2_COMID) %>%
    mutate(id = as.integer(id),
           nhdpv2_COMID = paste0("https://geoconnex.us//nhdplusv2/comid/", nhdpv2_COMID))
  
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



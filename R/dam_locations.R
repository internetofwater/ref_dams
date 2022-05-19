get_dam_locations <- function(dams, nid) {
  
  acre_to_sqkm <- 0.00404686
  
  dams <- dams %>%
    select(NIDID, comid = FlowLcomid, J_DAM_DAcr) %>%
    group_by(NIDID) %>% arrange(J_DAM_DAcr) %>%
    filter(n() == 1) %>% ungroup() %>%
    mutate(name =paste0("National Inventory of Dams: ", NIDID),
           description = paste0("Reference feature for USACE National Inventory of Dams: ", NIDID),
           subjectOf = paste0(NIDID),
           provider = "https://nid.usace.army.mil",
           provider_id = NIDID,
           drainage_area_sqkm = (as.numeric(J_DAM_DAcr) * acre_to_sqkm)) %>%
    select(name,
           description,
           subjectOf,
           provider,
           provider_id,
           drainage_area_sqkm,
           nhdpv2_COMID = comid)
  
  dams <- left_join(dams, 
                    dplyr::rename(sf::st_drop_geometry(nid), 
                                  nid_name = name),
                                  by = c("provider_id" = "federalId"))
  
  dams <- mutate(dams, 
                 name = ifelse(!is.na(nid_name), nid_name, name),
                 subjectOf = ifelse(!is.na(id),
                                    paste0("https://nid.usace.army.mil/#/dams/system/", 
                                           id, "/summary"),
                                    subjectOf)) %>%
    select(-id, -nid_name)
  
  nid <- filter(nid, !federalId %in% dams$provider_id) %>%
    mutate(description = paste0("Reference feature for USACE National Inventory of Dams: ", federalId),
           subjectOf = paste0("https://nid.usace.army.mil/#/dams/system/", 
                              id, "/summary"),
           provider = "https://nid.usace.army.mil",
           provider_id = federalId,
           drainage_area_sqkm = NA_real_,
           nhdpv2_COMID = NA_real_) %>%
    sf::st_transform(sf::st_crs(dams)) %>%
    select(name,
           description,
           subjectOf,
           provider,
           provider_id,
           drainage_area_sqkm,
           nhdpv2_COMID)
  
  nid <- nhdplusTools::st_compatibalize(nid, dams)
  
  bind_rows(dams, nid)
  
}

# TODO: work out precise hydrologic locations.
get_hydrologic_locations <- function(dams, hydrologic_locations, nhdpv2_fline,
                                     da_diff_thresh = 0.5, search_radius_m = 500,
                                     max_matches_in_radius = 5) {
  
  v2_area <- select(nhdplusTools::get_vaa(), 
                    nhdpv2_COMID = comid, 
                    nhdpv2_totdasqkm = totdasqkm)
  
  all_gages$nhdpv2_REACHCODE <- NA
  all_gages$nhdpv2_REACH_measure <- NA
  all_gages$nhdpv2_COMID <- NA
  
  for(hl in hydrologic_locations) {
    
    hl$locations <- hl$locations[hl$locations$provider_id %in% all_gages$provider_id, ]
    
    provider_selector <- all_gages$provider %in% hl$provider
    
    matcher <- match(hl$locations$provider_id,
                     all_gages$provider_id[provider_selector]
                     )
    
    all_gages$nhdpv2_REACHCODE[provider_selector][matcher] <- 
      hl$locations$nhdpv2_REACHCODE
    all_gages$nhdpv2_REACH_measure[provider_selector][matcher] <- 
      hl$locations$nhdpv2_REACH_measure
    all_gages$nhdpv2_COMID[provider_selector][matcher] <- 
      hl$locations$nhdpv2_COMID
    
    # Some gages missing reachcode/measure but have COMID
    update_index <- is.na(all_gages$nhdpv2_REACH_measure & !is.na(all_gages$nhdpv2_COMID))
    
    if(any(update_index)) {
      linked_gages <- select(all_gages[update_index, ], provider_id, nhdpv2_COMID) %>%
        left_join(select(sf::st_drop_geometry(nhdpv2_fline), COMID, FromMeas), 
                  by = c("nhdpv2_COMID" = "COMID"))
      
      all_gages$nhdpv2_REACH_measure[update_index] <- 
        linked_gages$FromMeas
    }
  }
  
  all_gages <- left_join(all_gages, v2_area, by = "nhdpv2_COMID")
  
  diff_da <- abs(all_gages$nhdpv2_totdasqkm -
                   all_gages$drainage_area_sqkm) / 
    all_gages$drainage_area_sqkm
  
  bad_da <- all_gages[!is.na(diff_da) & diff_da > da_diff_thresh, ]
  
  update_index <- which(is.na(all_gages$nhdpv2_COMID) | 
                          all_gages$provider_id %in% bad_da$provider_id)
  
  no_location <- all_gages[update_index, ]
  
  no_location <- st_transform(no_location, 5070)
  
  new_hl <- nhdplusTools::get_flowline_index(nhdpv2_fline, 
                                             no_location, 
                                             search_radius = units::set_units(
                                               search_radius_m, "m"),
                                             max_matches = max_matches_in_radius)
  
  
  linked_gages <- st_drop_geometry(select(no_location, provider_id)) %>%
    mutate(id = seq_len(nrow(.))) %>%
    left_join(new_hl, by = "id") %>%
    left_join(select(st_drop_geometry(all_gages), 
                     provider_id, drainage_area_sqkm), 
              by = "provider_id") %>%
    left_join(v2_area, by = c("COMID" = "nhdpv2_COMID")) %>%
    mutate(da_diff = abs(drainage_area_sqkm - nhdpv2_totdasqkm))
  
  linked_gages_dedup <- bind_rows(
    linked_gages %>%
      group_by(provider_id) %>%
      filter(is.na(da_diff)) %>%
      filter(offset == min(offset)) %>%
      ungroup(), 
    linked_gages %>%
      group_by(provider_id) %>%
      filter(!is.na(da_diff)) %>%
      filter(da_diff == min(da_diff)) %>%
      ungroup()) %>%
    group_by(provider_id) %>%
    filter(n() == 1) %>%
    ungroup()
  
  linked_gages <- select(no_location, provider_id) %>%
    mutate(id = seq_len(nrow(.))) %>%
    left_join(select(linked_gages_dedup, 
                     id, COMID, REACHCODE, REACH_meas), 
              by = "id")
  
  all_gages$nhdpv2_REACHCODE[update_index] <- linked_gages$REACHCODE
  all_gages$nhdpv2_REACH_measure[update_index] <- linked_gages$REACH_meas
  all_gages$nhdpv2_COMID[update_index] <- linked_gages$COMID
  
  all_gages
}

add_mainstems <- function(gage_hydrologic_locations, mainstems, vaa) {
  mainstems <- mainstems[,c("id", "uri"), drop = TRUE]
  mainstems$id <- as.integer(mainstems$id)
  vaa <- right_join(vaa, mainstems, by = c("levelpathi" = "id"))
  
  vaa <- vaa[,c("comid", "uri")]
  
  names(vaa) <- c("comid", "mainstem_uri")
  
  left_join(gage_hydrologic_locations, vaa, 
            by = c("nhdpv2_COMID" = "comid"))
}
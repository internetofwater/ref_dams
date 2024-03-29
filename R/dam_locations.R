get_dam_locations <- function(dams, nid) {
  
  acre_to_sqkm <- 0.00404686
  sqmi_to_sqkm <- 2.58999
  
  dams <- dams %>%
    group_by(NIDID) %>% arrange(J_DAM_DAcr) %>%
    filter(n() == 1) %>% ungroup() %>%
    mutate(drainage_area_sqkm_nawqa = (as.numeric(J_DAM_DAcr) * acre_to_sqkm)) %>%
    select(provider_id = NIDID,
           drainage_area_sqkm_nawqa,
           nhdpv2_COMID = FlowLcomid,
           year_completed = YEAR_COMPL,
           max_storage = MAX_STORAG,
           normal_storage = NORMAL_STO,
           nid_storage = NID_STORAG)
  
  nid <- select(nid, id, 
                provider_id = federalId, 
                name_nid = `Dam Name`, 
                other_names = `Other Names`,
                year_completed_nid = `Year Completed`,
                max_storage_nid = `Max Storage (Acre-Ft)`,
                normal_storage_nid = `Normal Storage (Acre-Ft)`,
                nid_storage_nid = `NID Storage (Acre-Ft)`,
                last_inspected = `Last Inspection Date`,
                last_updated = `Data Last Updated`,
                drainage_area_sqmi_nid = `Drainage Area (Sq Miles)`) %>%
    mutate(drainage_area_sqkm_nid = drainage_area_sqmi_nid * sqmi_to_sqkm)
  
  check <- inner_join(
    bind_cols(st_drop_geometry(dams), 
              dams %>%
                st_transform(5070) %>%
                st_coordinates()),
    bind_cols(st_drop_geometry(nid), 
              nid %>%
                st_transform(5070) %>%
                st_coordinates()) %>%
      rename(X_nid = X, Y_nid = Y),
  by = "provider_id") %>%
    mutate(dist = round(sqrt((X - X_nid)^2 + (Y - Y_nid)^2))) %>%
    mutate(pref_nawqa = ifelse(
      # if 500m or less apart, use NAWQA
      dist < 500, TRUE, ifelse(
        # if both drainage area esimates are large and distance is
        # greater than 500m use NAWQA. isTrue converts NA to FALSE
        isTRUE(drainage_area_sqkm_nawqa > 100 & drainage_area_sqkm_nid > 100
               & dist > 500), TRUE, 
        # Otherwise, use the modern NID
        FALSE)
    )) %>%
    mutate(pref_nawqa = ifelse(
      # if the year completed is different, use NID.
      !is.na(year_completed) & !is.na(year_completed_nid) &
        year_completed != year_completed_nid, FALSE, pref_nawqa)) %>%
    mutate(max_storage = ifelse(is.na(max_storage), -1, max_storage),
           nid_storage = ifelse(is.na(nid_storage), -1, nid_storage),
           normal_storage = ifelse(is.na(normal_storage), -1, normal_storage),
           max_storage_nid = ifelse(is.na(max_storage_nid), -1, max_storage_nid),
           nid_storage_nid = ifelse(is.na(nid_storage_nid), -1, nid_storage_nid),
           normal_storage_nid = ifelse(is.na(normal_storage_nid), -1, normal_storage_nid)) %>%
    mutate(pref_nawqa = ifelse(
      # if none of the storage terms match, use NID.
      max_storage != max_storage_nid |  
        nid_storage != nid_storage_nid | 
        normal_storage != normal_storage, FALSE, pref_nawqa)) %>%
    mutate(last_inspected = ifelse(is.na(last_inspected), as.Date("1900-01-01"), last_inspected),
           last_updated = ifelse(is.na(last_updated), as.Date("1900-01-01"), last_updated)) %>%
    mutate(pref_nawqa = ifelse(
      # if the dam was last inspected and the record was updated
      # since the NAWQA data was pulled. 
      # isTrue converts NA to FALSE. 
      last_inspected > as.Date("2018-01-01") & 
        last_updated > as.Date("2018-01-01"), FALSE,
      pref_nawqa))
  
  # Now we'll do a search for points that are close to eachother and have different IDs
  find_close <- function(x, check) {
    
    dist <- sqrt((check$X[x] - check$X_nid)^2 + (check$Y[x] - check$Y_nid)^2)
    
    o <- list()  
    
    # filter for dams close than 200m but not the same row
    filt <- dist < 200 #m
    filt[x] <- FALSE
    
    o[[check$provider_id[x]]] <- check$provider_id[filt]
    
    o
  }
  
  if(file.exists(temp_file <- "temp/check_points.rds")) {
    check_points <- readRDS(temp_file)
  } else {
    cl <- parallel::makeCluster(12)
    check_points <- pbapply::pblapply(1:nrow(check), find_close, check = check, cl = cl)
    parallel::stopCluster(cl)
    saveRDS(check_points, temp_file)
  }

  # get this converted to a data.frame and just find all ids
  ch <- check_points[sapply(check_points, function(x) length(x[[1]])) > 0] %>%
    unlist(recursive = FALSE)
  ch <- as.data.frame(cbind(rep(names(ch), time = lengths(ch)), unlist(ch)))
  ch <- unique(c(ch[, 1], ch[, 2]))
  
  # We will avoid using the slightly out of date definitions on these.
  check$pref_nawqa[check$provider_id %in% ch] <- FALSE
  
  nawqa <- check[check$pref_nawqa, ]
  
  nawqa <- dams[dams$provider_id %in% nawqa$provider_id, ]

  extra <- st_drop_geometry(nid) %>%
    mutate(name = ifelse(!is.na(other_names) & !is.na(name_nid), 
                         paste(name_nid, "-", other_names), 
                         ifelse(!is.na(name_nid), name_nid,
                                ifelse(!is.na(other_names), other_names,
                                       "name unknown")))) %>%
    select(id, provider_id, name)
  
  nawqa_only <- dams[!dams$provider_id %in% nid$provider_id, ]
  
  nawqa <- select(nawqa, provider_id, drainage_area_sqkm_nawqa, nhdpv2_COMID) %>%
    bind_rows(select(nawqa_only, provider_id, drainage_area_sqkm_nawqa, nhdpv2_COMID)) %>%
    left_join(extra, by = "provider_id") %>%
    mutate(feature_data_source = "https://doi.org/10.5066/P92S9ZX6")
  
  usace <- select(nid, provider_id, drainage_area_sqkm_nid) %>%
    left_join(extra, by = "provider_id") %>%
    filter(!provider_id %in% nawqa$provider_id) %>%
    sf::st_transform(sf::st_crs(nawqa)) %>%
    mutate(feature_data_source = "https://nid.usace.army.mil/#/downloads")
  
  usace <- st_compatibalize(usace, nawqa)

  bind_rows(nawqa, usace) %>%
    mutate(description = paste0("Reference feature for USACE National Inventory of Dams: ", provider_id),
           subjectOf = ifelse(!is.na(id), paste0("https://nid.usace.army.mil/#/dams/system/",
                                                 provider_id, "/summary"), NA),
           provider = "https://nid.usace.army.mil", 
           nhdpv2_COMID = ifelse(!is.na(nhdpv2_COMID), 
                  paste0("https://geoconnex.us/nhdplusv2/comid/", nhdpv2_COMID), NA),
           id = as.integer(id)) %>%
    select(name,
           description,
           subjectOf,
           provider,
           provider_id,
           drainage_area_sqkm_nid,
           drainage_area_sqkm_nawqa,
           nhdpv2_COMID, 
           feature_data_source)
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
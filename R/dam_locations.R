get_dam_locations <- function(dams, nid) {
  
  acre_to_sqkm <- 0.00404686
  sqmi_to_sqkm <- 2.58999
  
  dams$NIDID <- trimws(dams$NIDID)
  nid$federalId <- trimws(nid$federalId)
  
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
    mutate(feature_data_source = "https://nid.sec.usace.army.mil#/downloads")
  
  usace <- st_compatibalize(usace, nawqa)

  bind_rows(nawqa, usace) %>%
    mutate(description = paste0("Reference feature for USACE National Inventory of Dams: ", provider_id),
           subjectOf = ifelse(!is.na(id), paste0("https://nid.sec.usace.army.mil#/dams/system/",
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

utility_link_dams <- function(to_link, dams, nhdpv2_fline, vaa, search_radius_m, max_matches_in_radius) {
  new_hl <- nhdplusTools::get_flowline_index(nhdpv2_fline, 
                                             to_link, 
                                             search_radius = units::set_units(
                                               search_radius_m, "m"),
                                             max_matches = max_matches_in_radius)
  
  sf::st_drop_geometry(select(to_link, provider_id)) %>%
    mutate(id = seq_len(nrow(.))) %>%
    left_join(new_hl, by = "id") %>%
    left_join(select(sf::st_drop_geometry(dams), 
                     provider_id, drainage_area_sqkm), 
              by = "provider_id") %>%
    left_join(select(sf::st_drop_geometry(nhdpv2_fline),
                     COMID, drainage_area_sqkm_nhdpv2 = TotDASqKM), 
              by = "COMID") %>%
    left_join(select(vaa, COMID = comid, hydroseq), by = "COMID") %>%
    mutate(da_diff = abs(drainage_area_sqkm - drainage_area_sqkm_nhdpv2) / drainage_area_sqkm)
}

get_dam_network_location <- function(dams, update_index, nhdpv2_fline, vaa, 
                                     search_radius_m, max_matches_in_radius, index_type) {
  linked_dams_base <- select(dams[update_index, ], provider_id, nhdpv2_COMID)
  
  linked_dams <- linked_dams_base %>%
    left_join(select(sf::st_drop_geometry(nhdpv2_fline), COMID, REACHCODE, FromMeas), 
              by = c("nhdpv2_COMID" = "COMID"))
  
  # get precise network location
  
  fline_sites <- dplyr::filter(nhdpv2_fline, COMID %in% linked_dams$nhdpv2_COMID) |>
    sf::st_transform(sf::st_crs(linked_dams)) |>
    sf::st_cast("LINESTRING")
  
  linked_dams <- utility_link_dams(linked_dams, dams, fline_sites, vaa, 
                                   search_radius_m, max_matches_in_radius)
  
  linked_dams_dedup <- linked_dams %>%
    left_join(select(sf::st_drop_geometry(dams), provider_id, nhdpv2_COMID), by = "provider_id") %>%
    group_by(provider_id) %>%
    filter(COMID == nhdpv2_COMID) %>%
    ungroup()
  
  if(!any(duplicated(linked_dams_dedup$provider_id))) {
    linked_dams_base <- linked_dams_base %>%
      left_join(select(linked_dams_dedup, provider_id, REACHCODE, REACH_meas, drainage_area_sqkm_nhdpv2),
                by = "provider_id")
  } else {
    stop()
  }
  
  if(!nrow(linked_dams_base) == sum(update_index)) stop()
  
  dams$nhdpv2_REACHCODE[update_index] <- 
    linked_dams_base$REACHCODE
  dams$nhdpv2_REACH_measure[update_index] <- 
    linked_dams_base$REACH_meas
  
  dams$index_type[update_index] <- index_type 
  
  dams
}

#' get dam hydrolocations
#' @param dams sf data.frame table of dams with best estimate of comid and drainage area
#' @param nhdpv2_fline sf data.frame of flowlines to index to
#' @param da_diff_thresh numeric between 0 an 1
#' if the normalized difference between prior estimate 
#' drainage area and the NHDPlusV2 modeled drainage area is greater than this, 
#' the dam is not indexed to the network.
#' @param search_radius_m numeric distance to search from dam location to flowline
#' @param max_matches_in_radius maximum flowines to consider within search radius
#' @description
#' Given input with estimates of NHDPlusV2 comid and prior estimates of drainage area,
#' this function attempts to figure out which dams should be indexed to the network
#' vs which should be left associated to a comid because they drain a very small area
#' vs which should be left un indexed because uncertainty is too high.
#'
get_dam_hydrolocations <- function(dams, nhdpv2_fline, vaa,
                                   da_diff_thresh = 0.1, 
                                   search_radius_m = 500,
                                   max_matches_in_radius = 5) {
  
  dams$nhdpv2_COMID <- as.integer(gsub("https://geoconnex.us/nhdplusv2/comid/", "",
                                       dams$nhdpv2_COMID))
  
  dams$nhdpv2_REACHCODE <- NA
  dams$nhdpv2_REACH_measure <- NA
  
  dams$drainage_area_sqkm <- dams$drainage_area_sqkm_nawqa
  dams$drainage_area_sqkm[is.na(dams$drainage_area_sqkm)] <- 
    dams$drainage_area_sqkm_nid[is.na(dams$drainage_area_sqkm)]
  
  dams <- left_join(dams, select(sf::st_drop_geometry(nhdpv2_fline),
                                 nhdpv2_COMID = COMID, drainage_area_sqkm_nhdpv2 = TotDASqKM),
                    by = "nhdpv2_COMID")
  
  dams$drainage_area_sqkm[dams$drainage_area_sqkm == 0] <- NA
  
  # normalized difference in drainage area using NiD/NAWQA best estimate to normalize
  # we can use this to evaluate whether what the COMID models is reasonable compared
  # to what the NID and NAWQA has as an estimate.
  dams$norm_diffda <- (dams$drainage_area_sqkm - dams$drainage_area_sqkm_nhdpv2) / 
    dams$drainage_area_sqkm
  
  # these are where we have a reasonable network-drainage area match
  update_index <- is.na(dams$nhdpv2_REACH_measure) & !is.na(dams$nhdpv2_COMID) & 
    (!is.na(dams$norm_diffda) & abs(dams$norm_diffda) < da_diff_thresh)
  
  dams$index_type <- rep(NA_character_, nrow(dams))
  
  if(any(update_index)) {
    
    dams <- get_dam_network_location(dams, update_index, nhdpv2_fline, vaa, 
                                     search_radius_m, max_matches_in_radius, 
                                     "nawqa_on_network_da_match")
  }
  
  # these are where we have a small catchment worth indexing to despite a da mismatch
  update_index <- is.na(dams$nhdpv2_REACH_measure) & # no reachcode measure yet
    !is.na(dams$nhdpv2_COMID) & # has a comid
    (!is.na(dams$norm_diffda) & # has a normalized area diff
       abs(dams$norm_diffda) > da_diff_thresh & # diff is larger than thresh
       dams$drainage_area_sqkm_nhdpv2 < 10) # but this is near a headwater
  
  if(any(update_index)) {
    
    dams <- get_dam_network_location(dams, update_index, nhdpv2_fline, vaa, 
                                     search_radius_m, max_matches_in_radius, 
                                     "nawqa_<10sqkm_da_mismatch")
  }
  
  # these are where we don't have a network drainage area match but NAWQA assigned a COMID
  update_index <- is.na(dams$nhdpv2_REACH_measure) & !is.na(dams$nhdpv2_COMID) & 
    (!is.na(dams$norm_diffda) & abs(dams$norm_diffda) > da_diff_thresh)
  
  if(any(update_index)) {
    dams$index_type[update_index] <- "nawqa_off_network_da_mismatch"
  }
  
  # now look at everything where there is no prior COMID estimate
  update_index <- which(is.na(dams$nhdpv2_COMID))
  
  no_location <- dams[update_index, ]
  
  no_location <- sf::st_transform(no_location, 5070)
  
  linked_dams <- utility_link_dams(no_location, dams, nhdpv2_fline, vaa, 
                                   search_radius_m, max_matches_in_radius) %>%
    filter(is.na(da_diff) | da_diff < da_diff_thresh)
  
  linked_dams_dedup <- bind_rows(
    linked_dams %>%
      group_by(provider_id) %>%
      filter(is.na(da_diff)) %>%
      filter(offset == min(offset)) %>%
      ungroup() %>%
      mutate(index_type = "automatic_network_closest_flowline_no_da_check"), 
    linked_dams %>%
      group_by(provider_id) %>%
      filter(!is.na(da_diff)) %>%
      filter(da_diff == min(da_diff)) %>%
      ungroup() %>%
      mutate(index_type = "automatic_network_closest_drainage_area")) %>%
    group_by(provider_id) %>%
    filter(hydroseq == min(hydroseq)) %>%
    filter(n() == 1) %>%
    ungroup()
  
  linked_dams <- select(no_location, provider_id) %>%
    mutate(id = seq_len(nrow(.))) %>%
    left_join(select(linked_dams_dedup, 
                     id, COMID, REACHCODE, REACH_meas, index_type, drainage_area_sqkm_nhdpv2), 
              by = "id")
  
  dams$nhdpv2_REACHCODE[update_index] <- linked_dams$REACHCODE
  dams$nhdpv2_REACH_measure[update_index] <- linked_dams$REACH_meas
  dams$nhdpv2_COMID[update_index] <- linked_dams$COMID
  dams$index_type[update_index] <- linked_dams$index_type
  dams$drainage_area_sqkm_nhdpv2[update_index] <- linked_dams$drainage_area_sqkm_nhdpv2
  
  dams <- select(dams, -norm_diffda)
  
  dams
}

add_mainstems <- function(dam_hydrologic_locations, mainstems, vaa) {
  mainstems <- mainstems[,c("head_nhdpv2_COMID", "uri"), drop = TRUE]
  
  mainstems$head_nhdpv2_COMID <- as.integer(gsub("https://geoconnex.us/nhdplusv2/comid/", "", 
                                                 mainstems$head_nhdpv2_COMID))
  
  # stolen from ref_gages
  mainstem_lookup <- group_by(vaa, levelpathi) |>
    filter(hydroseq == max(hydroseq)) |>
    ungroup() |>
    select(head_nhdpv2_COMID = comid, levelpathi) |>
    distinct() |>
    left_join(mainstems, by = "head_nhdpv2_COMID") |>
    filter(!is.na(uri)) |>
    select(-head_nhdpv2_COMID) |>
    right_join(select(vaa, comid, levelpathi), 
               by = "levelpathi") |>
    select(-levelpathi, comid, mainstem_uri = uri)
  
  dplyr::left_join(dam_hydrologic_locations, mainstem_lookup, 
                   by = c("nhdpv2_COMID" = "comid"))
  
}

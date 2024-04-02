library(targets)

tar_option_set(packages = c("nhdplusTools", "sf", "dplyr", "dataRetrieval", 
                            "sbtools", "readr", "knitr", "mapview", "data.table"),
               memory = "transient", garbage_collection = TRUE)

reference_file <- "out/ref_dams.gpkg"

pid_file <- "out/ref_dams_pid.csv"
nldi_file <- "out/nldi_dams.geojson"

index_dir <- "docs/"

sourced <- sapply(list.files("R", pattern = "*.R$", full.names = TRUE), source)

list(
  # NHDPlusV2 downloaded with nhdplusTools
  tar_target("nat_db", download_nhdplusv2("data/nhdp")),
  
  # Only the network flowlines for now -- non-network could be pulled in.
  tar_target("nhdpv2_fline", read_sf(nat_db, "NHDFlowline_Network")),
  tar_target("nhdpv2_fline_proc", select(st_transform(nhdpv2_fline, 5070),
                                         COMID, REACHCODE, ToMeas, FromMeas)),
  
  
  tar_target("dams", get_dam_data("data/dams/", 
                                  sb = "5fb7e483d34eb413d5e14873",
                                  f = "Final_NID_2018.zip")),
  
  tar_target("nid_gpkg", get_nid_gpkg("data/nation.gpkg",
                                      "https://nid.sec.usace.army.milapi/nation/gpkg")),
  
  tar_target("nid_meta", get_nid_csv("data/nation.csv",
                                     "https://nid.sec.usace.army.milapi/nation/csv")),
  
  tar_target("nid", left_join(nid_gpkg, nid_meta, by = c("federalId" = "Federal ID"))),
  

  
  # this function filters and renames gage locations to a common table
  # there is a significant ammount of logic to prefer the older NAWQA-based QC 
  # data source (dams) over the newer nid data source. See function internals
  # for details.
  tar_target("dam_locations", get_dam_locations(dams, nid)),
  
  # TODO: Add specific hydrologic locations
  # tar_target("vaa", get_vaa(atts = c("comid", "levelpathi", "hydroseq"),
  #                           updated_network = TRUE)),  
  # # This function takes a table of all NWIS and more in the future gage
  # # locations and a list of provided hydrologic locations. The provider
  # # is a way to join on provider and provider_id in the all_gages input.
  # # The order that hydrologic locations sources are provided will determine
  # # precidence -- last defined wins.
  # gage_hydrologic_locations = get_hydrologic_locations(
  #   all_gages = gage_locations,
  #   hydrologic_locations = list(
  #     list(provider = "https://waterdata.usgs.gov",
  #          locations = nwis_gage_hydro_locatons),
  #     list(provider = "https://cdec.water.ca.gov",
  #          locations = cdec_gage_address)),
  #   nhdpv2_fline = nhdpv2_fline_proc),
  # 
  # gage_hydrologic_locations_with_mainstems = add_mainstems(gage_hydrologic_locations,
  #                                                          mainstems, vaa),
  # 
  ### Registry ###
  # Each entry will have a provider and provider_id that acts as a unique
  # primary key. The existing registry file will have a unique attribute
  # that contains that primary key. 
  tar_target("providers_csv", command = "reg/providers.csv", format = "file"),
  tar_target("providers", read_csv(providers_csv)),
  
  
  tar_target("registry", build_registry(dam_locations,
                                        registry = "reg/ref_dams.csv",
                                        providers = providers)),
  
  tar_target("reference_out", write_reference(dam_locations,
                                              registry, providers, reference_file,
                                              nldi_file)),
  
  tar_target("registry_out", write_registry(registry, "reg/ref_dams %>% .csv"))
)
library(drake)
library(dplyr)
library(sf)
library(nhdplusTools)
library(dataRetrieval)
library(sbtools)
library(readr)
library(knitr)
library(mapview)
library(sbtools)

reference_file <- "out/ref_dams.gpkg"

pid_file <- "out/ref_dams_pid.csv"
nldi_file <- "out/nldi_dams.geojson"

index_dir <- "docs/"

sourced <- sapply(list.files("R", pattern = "*.R$", full.names = TRUE), source)

plan <- drake_plan(
  # NHDPlusV2 downloaded with nhdplusTools
  nat_db = download_nhdplusv2("data/nhdp"),
  
  # Only the network flowlines for now -- non-network could be pulled in.
  nhdpv2_fline = read_sf(nat_db, "NHDFlowline_Network"),
  nhdpv2_fline_proc = select(st_transform(nhdpv2_fline, 5070),
                             COMID, REACHCODE, ToMeas, FromMeas),
  
  
  dams = get_dam_data("data/dams/", 
                     sb = "5fb7e483d34eb413d5e14873",
                     f = "Final_NID_2018.zip"),
  
  nid_gpkg = get_nid_gpkg("data/nation.gpkg",
                      "https://nid.usace.army.mil/api/nation/gpkg"),
  
  nid_meta = get_nid_csv("data/nation.csv",
                         "https://nid.usace.army.mil/api/nation/csv"),
  
  nid = left_join(nid_gpkg, nid_meta, by = c("federalId" = "Federal ID")),
  
  vaa = get_vaa(atts = c("comid", "levelpathi"),
                updated_network = TRUE),

  # this function filters and renames gage locations to a common table
  dam_locations = get_dam_locations(dams, nid),
  
  # TODO: Add specific hydrologic locations
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
  # Each entry will have a provider and provider_id that acts as a unique
  # primary key. The existing registry file will have a unique attribute
  # that contains that primary key.
  providers = read_csv(file_in("reg/providers.csv")),

  
  registry = build_registry(dam_locations,
                            registry = "reg/ref_dams.csv",
                            providers = providers),

  reference_out = write_reference(dam_locations,
                                  registry, providers, reference_file,
                                  nldi_file),
  
  registry_out = write_registry(registry, "reg/ref_dams.csv"),
)

make(plan, memory_strategy = "autoclean", garbage_collection = TRUE, lock_envir = FALSE)

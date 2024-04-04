get_dam_data <- function(out_dir, sb, f) {
  
  
  item <- sbtools::item_get(sb)
  
  if(!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

  f <- "Final_NID_2018.zip"
  d <- file.path(out_dir, "Final_NID_2018.zip")
  
  if(!file.exists(d)) {
    sbtools::item_file_download(item, names = f, destinations = d)
    
    zip::unzip(d, exdir = out_dir)
  }

  shp <- list.files(out_dir)
  
  shp <- shp[grepl("shp$", shp)]
  
  sf::read_sf(file.path(out_dir, shp))
  
}

get_nid_gpkg <- function(out_file = "data/nation.gpkg", 
                         url = "https://nid.sec.usace.army.milapi/nation/gpkg") {
  
  if(!file.exists(out_file)) {
    download.file(url, out_file, mode = "wb")
  }
  
  sf::read_sf(out_file)
  
}

get_nid_csv <- function(out_file = "data/nation.csv",
                        url = "https://nid.sec.usace.army.milapi/nation/csv") {
  if(!file.exists(out_file)) {
    download.file(url, out_file, mode = "wb")
  }
  
  readr::read_csv(out_file, skip = 1)
}


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


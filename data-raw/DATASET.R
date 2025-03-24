start <- c(2e6, -1e6)
end <- c(1.8e6, -800000)
incr <- seq(0, 1, length = 10000)
track <- cbind(approxfun(c(0, 1), c(start[1], end[1]))(incr), approxfun(c(0, 1), c(start[2], end[2]))(incr))

longlat <- gdalraster::transform_xy(track, srs_to = "EPSG:4326", srs_from = "EPSG:3031")

arrow::write_parquet(tibble::tibble(lon = longlat[,1, drop = TRUE], lat  = longlat[,2, drop = TRUE]), "longlat_points.parquet")

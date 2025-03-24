extracted <- !file.exists("longlat_points_dem.parquet")

if (!extracted) {
  track <- arrow::read_parquet("longlat_points.parquet")
library(xml2)
library(gdalraster)
dsn <- "/vsicurl/https://raw.githubusercontent.com/mdsumner/rema-ovr/main/REMA-2m_dem_ovr.vrt"
url <- gsub("/vsicurl/", "", dsn)
xml <- read_xml(url)
dst <- xml |> xml_find_all(".//DstRect")
src <- xml |> xml_find_all(".//SrcRect")
xOff <- as.integer(xml_attr(dst, "xOff"))
yOff <- as.integer(xml_attr(dst, "yOff"))
xSize <- as.integer(dst |> xml_attr("xSize"))
ySize <- as.integer(dst |> xml_attr("ySize"))

ds <- new(GDALRaster, dsn)
dm <- ds$dim()[1:2]
bbox <- ds$bbox()

ds$close()

x_from_col <- function(dimension, bbox, col) {
  col[col < 1] <- NA
  col[col > dimension[1L]] <- NA
  xres <- diff(bbox[c(1, 3)]) / dimension[1]
  bbox[1] - xres/2 + col * xres
}
y_from_row <- function(dimension, bbox, row) {
  row[row < 1] <- NA
  row[row > dimension[2]] <- NA
  yres <- diff(bbox[c(2, 4)]) / dimension[2]
  bbox[4] + yres/2 - row * yres
}
xmin <- x_from_col(dm, bbox, xOff + 1)
xmax <- x_from_col(dm, bbox, xOff + xSize)
ymin <- y_from_row(dm, bbox, yOff + 1)
ymax <- y_from_row(dm, bbox, yOff + ySize)

## all bbox of ComplexSource in VRT:
bb <- cbind(xmin = xmin,  ymin = ymin, xmax = xmax, ymax = ymax)

## so, then we can do
rc <- wk::rct(bb[,1], bb[,2], bb[,3], bb[,4])
#plot(rc)
tree <- geos::geos_strtree(rc)

xy <- gdalraster::transform_xy(cbind(track$lon, track$lat ), srs_to = ds$getProjectionRef(), srs_from = "EPSG:4326")

## tile for every point
tile <- unlist(lapply(geos::geos_strtree_query(tree, wk::xy(track[,1], track[,2])), "[", 1L))   ## make sure only one tile per point
# plot(rc)
# plot(rc[unique(tile)], add = TRUE, col = hcl.colors(length(unique(tile))))


## now we can crop the dataset to each tile and look up the appropriate points
extract_pt <- function(dsn, bbox, pts) {
  tf <- tempfile(fileext = ".vrt")
  translate(dsn, tf, cl_arg = c("-projwin", bbox), quiet = TRUE)
  pixel_extract(new(GDALRaster, tf), pts)
}

v <- vector("list", length(unique(tile)))
for (i in seq_along(v)) {
  v[[i]] <- extract_pt(dsn, bb[unique(tile)[i], ], track[tile == unique(tile)[i], , drop = FALSE])
}

track$elev <- unlist(v)
arrrow::write_parquet(track, "longlat_points_dem.parquet")

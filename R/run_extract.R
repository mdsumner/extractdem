extracted <- file.exists("longlat_points_dem.parquet")
Sys.setenv("AWS_NO_SIGN_REQUEST" = "YES")  ## shouldn't be necessary, added in desperation during ghactions testing


if (!extracted) {
  track <- nanoparquet::read_parquet("longlat_points.parquet")
library(xml2)
library(gdalraster)
#dsn <- "/vsicurl/https://raw.githubusercontent.com/mdsumner/rema-ovr/main/REMA-2m_dem_ovr.vrt"
dsn <-  "/vsicurl/https://opentopography.s3.sdsc.edu/raster/COP30/COP30_hh.vrt"
url <- gsub("/vsicurl/", "", dsn)
xml <- read_xml(url)
dst <- xml |> xml_find_all(".//DstRect")
src <- xml |> xml_find_all(".//SrcRect")
xOff <- as.integer(xml_attr(dst, "xOff"))
yOff <- as.integer(xml_attr(dst, "yOff"))
xSize <- as.integer(dst |> xml_attr("xSize"))
ySize <- as.integer(dst |> xml_attr("ySize"))

ds <- try(new(GDALRaster, dsn))
if (inherits(ds, "try-error"))  stop("cannot open dataset")
dm <- ds$dim()[1:2]
bbox <- ds$bbox()

xy <- gdalraster::transform_xy(cbind(track$lon, track$lat ), srs_to = ds$getProjectionRef(), srs_from = "EPSG:4326")

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
ymax <- y_from_row(dm, bbox, yOff + 1)
ymin <- y_from_row(dm, bbox, yOff + ySize)

## all bbox of ComplexSource in VRT:
bb <- cbind(xmin = xmin,  ymin = ymin, xmax = xmax, ymax = ymax)

## so, then we can do
rc <- wk::rct(bb[,1], bb[,2], bb[,3], bb[,4])
#plot(rc)
tree <- geos::geos_strtree(rc)


## tile for every point
tile <- unlist(lapply(geos::geos_strtree_query(tree, wk::xy(xy[,1, drop = TRUE], xy[,2, drop = TRUE])), "[", 1L))   ## make sure only one tile per point
# plot(rc)
# plot(rc[unique(tile)], add = TRUE, col = hcl.colors(length(unique(tile))))


## now we can crop the dataset to each tile and look up the appropriate points
extract_pt <- function(x) {
  dsn <- x$dsn[1]
  bbox <- x$bbox[[1]]
  if (x$tile[1] == 0) return(rep(NA_real_, length(x$X)))
  pts <- cbind(x$X, x$Y)
  tf <- tempfile(fileext = ".vrt")

  translate(dsn, tf, cl_arg = c("-projwin", unname(bbox[c(1, 4, 3, 2)])), quiet = TRUE)
  ds <- new(GDALRaster, tf)
  on.exit(ds$close(), add = TRUE)
  pixel_extract(ds, pts)
}


v <- vector("list", length(unique(tile)))
## create the payload
for (i in seq_along(v)) {
  tile_index <- unique(tile)[i]
  X <- xy[!is.na(tile) & tile == tile_index,1 , drop = TRUE]
  Y <- xy[!is.na(tile) & tile == tile_index,2 , drop = TRUE]
  ## when we don't have a tile call it zero
  if (is.na(tile_index)) {
    tile_index <- 0
    Y <- X <- rep(NA, sum(is.na(tile)))
    print(sum(is.na(tile)))
  }
  tib <- tibble::tibble(dsn = dsn, X = X,
                        Y = Y,
                        bbox = list(bb[tile_index,, drop = TRUE]),
                        tile = tile_index)
  v[[i]] <- tib
}

options(parallelly.fork.enable = TRUE, future.rng.onMisuse = "ignore")
library(furrr);
plan(multicore)
v1 <- future_map(v, extract_pt)
plan(sequential)

# for (i in seq_along(v)) {
#   v[[i]] <- extract_pt(dsn, bb[unique(tile)[i],, drop = TRUE], xy[tile == unique(tile)[i], , drop = FALSE])
# }



ds$close()

track$elev <- unlist(v1)
nanoparquet::write_parquet(track, "longlat_points_dem.parquet")
}

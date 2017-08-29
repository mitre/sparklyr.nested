.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}

spark_dependencies <- function(spark_version, scala_version, ...) {
  # do nothing because no additional dependencies required
}
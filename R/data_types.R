#' Spark Data Types
#'
#' These function support supplying a spark read schema. This is particularly useful
#' when reading data with nested arrays when you are not interested in several of
#' the nested fields.
#'
#' @param sc A \code{spark_connection}
#' @param struct_fields A vector or fields obtained from \code{struct_field()}
#' @export
struct_type <- function(sc, struct_fields) {
  struct <- sparklyr::invoke_new(sc,
                       class="org.apache.spark.sql.types.StructType")

  if (is.list(struct_fields)) {
    for (i in 1:length(struct_fields))
      struct <- sparklyr::invoke(struct, "add", struct_fields[[i]])
  } else {
    struct <- sparklyr::invoke(struct, "add", struct_fields)
  }

  return(struct)
}

#' @rdname struct_type
#' @param name A field name to use in the output struct type
#' @param data_type A (java) data type (e.g., \code{string_type()} or \code{double_type()})
#' @param nullable Logical. Describes whether field can be missing for some rows.
#' @export
struct_field <- function(sc, name, data_type, nullable=FALSE) {
  metadata <- sparklyr::invoke_static(sc,
                            class="org.apache.spark.sql.types.Metadata",
                            method="empty")
  sparklyr::invoke_new(sc,
             class="org.apache.spark.sql.types.StructField",
             name, data_type, nullable, metadata)
}

#' @rdname struct_type
#' @export
array_type <- function(sc, data_type, nullable=FALSE) {
  sparklyr::invoke_new(sc,
             class="org.apache.spark.sql.types.ArrayType",
             data_type, nullable)
}

#' @rdname struct_type
#' @export
binary_type <- function(sc) {
  sparklyr::invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "binary")
}

#' @rdname struct_type
#' @export
boolean_type <- function(sc) {
  sparklyr::invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "boolean")
}

#' @rdname struct_type
#' @export
byte_type <- function(sc) {
  sparklyr::invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "byte")
}

#' @rdname struct_type
#' @export
date_type <- function(sc) {
  sparklyr::invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "date")
}

#' @rdname struct_type
#' @export
double_type <- function(sc) {
  sparklyr::invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "double")
}

#' @rdname struct_type
#' @export
float_type <- function(sc) {
  sparklyr::invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "float")
}

#' @rdname struct_type
#' @export
integer_type <- function(sc) {
  sparklyr::invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "integer")
}

#' @rdname struct_type
#' @export
numeric_type <- function(sc) {
  sparklyr::invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "numeric")
}

#' @rdname struct_type
#' @export
long_type <- function(sc) {
  sparklyr::invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "long")
}

#' @rdname struct_type
#' @param key_type A (java) data type describing the map keys (usually \code{string_type()})
#' @param value_type A (java) data type describing the map values
#' @export
map_type <- function(sc, key_type, value_type, nullable=FALSE) {
  sparklyr::invoke_new(sc,
             class="org.apache.spark.sql.types.MapType",
             key_type, value_type, nullable)
}

#' @rdname struct_type
#' @export
string_type <- function(sc) {
  sparklyr::invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "string")
}

#' @rdname struct_type
#' @export
character_type <- function(sc) {
  sparklyr::invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "character")
}

#' @rdname struct_type
#' @export
timestamp_type <- function(sc) {
  sparklyr::invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "timestamp")
}

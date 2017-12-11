#' Spark Data Types
#' 
#' These function support supplying a spark read schema. This is particularly useful
#' when reading data with nested arrays when you are not interested in several of 
#' the nested fields.
#' 
#' @importFrom sparklyr invoke_new
#' @importFrom sparklyr invoke
#' @export
struct_type <- function(sc, struct_fields) {
  struct <- invoke_new(sc,
                       class="org.apache.spark.sql.types.StructType")
  
  if (is.list(struct_fields)) {
    for (i in 1:length(struct_fields))
      struct <- invoke(struct, "add", struct_fields[[i]])
  } else {
    struct <- invoke(struct, "add", struct_fields)
  }
  
  return(struct)
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
struct_field <- function(sc, name, data_type, nullable=FALSE) {
  metadata <- invoke_static(sc, 
                            class="org.apache.spark.sql.types.Metadata", 
                            method="empty")
  invoke_new(sc,
             class="org.apache.spark.sql.types.StructField",
             name, data_type, nullable, metadata)
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
array_type <- function(sc, data_type, nullable=FALSE) {
  invoke_new(sc,
             class="org.apache.spark.sql.types.ArrayType", 
             data_type, nullable)
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
binary_type <- function(sc) {
  invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "binary")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
boolean_type <- function(sc) {
  invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "boolean")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
byte_type <- function(sc) {
  invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "byte")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
date_type <- function(sc) {
  invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "date")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
double_type <- function(sc) {
  invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "double")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
float_type <- function(sc) {
  invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "float")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
integer_type <- function(sc) {
  invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "integer")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
numeric_type <- function(sc) {
  invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "numeric")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
map_type <- function(sc, key_type, value_type, nullable=FALSE) {
  invoke_new(sc,
             class="org.apache.spark.sql.types.MapType",
             key_type, value_type, nullable)
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
string_type <- function(sc) {
  invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "string")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
character_type <- function(sc) {
  invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "character")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
timestamp_type <- function(sc) {
  invoke_static(sc, "sparklyr.SQLUtils", "getSQLDataType", "timestamp")
}
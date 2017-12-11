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
  invoke_new(sc,
             class="org.apache.spark.sql.types.BinaryType")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
boolean_type <- function(sc) {
  invoke_new(sc,
             class="org.apache.spark.sql.types.BooleanType")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
byte_type <- function(sc) {
  invoke_new(sc,
             class="org.apache.spark.sql.types.ByteType")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
calendar_interval_type <- function(sc) {
  invoke_new(sc,
             class="org.apache.spark.sql.types.CalendarIntervalType")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
char_type <- function(sc, length) {
  invoke_new(sc,
             class="org.apache.spark.sql.types.CharType",
             as.integer(length))
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
date_type <- function(sc) {
  invoke_new(sc,
             class="org.apache.spark.sql.types.DateType")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
double_type <- function(sc) {
  invoke_new(sc,
             class="org.apache.spark.sql.types.DoubleType")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
float_type <- function(sc) {
  invoke_new(sc,
             class="org.apache.spark.sql.types.FloatType")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
integer_type <- function(sc) {
  invoke_new(sc,
             class="org.apache.spark.sql.types.IntegerType")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
long_type <- function(sc) {
  invoke_new(sc,
             class="org.apache.spark.sql.types.LongType")
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
null_type <- function(sc) {
  invoke_new(sc,
             class="org.apache.spark.sql.types.NullType")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
short_type <- function(sc) {
  invoke_new(sc,
             class="org.apache.spark.sql.types.ShortType")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
string_type <- function(sc) {
  invoke_new(sc,
             class="org.apache.spark.sql.types.StringType")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
struct_field <- function(sc, name, data_type, nullable=FALSE) {
  invoke_new(sc,
             class="org.apache.spark.sql.types.StructField",
             name, data_type, nullable, NULL)
}

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
  
  for (i in 1:length(struct_fields))
    struct <- invoke(struct, "add", struct_fields[[i]])
  
  return(struct)
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
timestamp_type <- function(sc) {
  invoke_new(sc,
             class="org.apache.spark.sql.types.TimestampType")
}

#' @rdname struct_type
#' @importFrom sparklyr invoke_new
#' @export
varchar_type <- function(sc, length) {
  invoke_new(sc,
             class="org.apache.spark.sql.types.VarcharType",
            as.integer(length))
}
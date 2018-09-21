#' Work with the schema
#' 
#' These functions support flexible schema inspection both algorithmically and in human-friendly ways.
#' 
#' @param x An \code{R} object wrapping, or containing, a Spark DataFrame.
#' @param parse_json Logical. If \code{TRUE} then the JSON return value will be parsed into an R list.
#' @param simplify Logical. If \code{TRUE} then the schema will be folded into itself such that
#'   \code{{"name" : "field1", "type" : {"type" : "array", "elementType" : "string", "containsNull" : true},
#'    "nullable" : true, "metadata" : { } }} will be rendered simply \code{{"field1 (array)" : "[string]"}}
#' @param append_complex_type Logical. This only matters if \code{parse_json=TRUE} and \code{simplify=TRUE}. 
#'   In that case indicators will be included in the return value for array and struct types.
#' @export
#' @importFrom jsonlite fromJSON
#' @importFrom sparklyr invoke spark_dataframe
#' @importFrom dplyr %>%
#' @seealso \code{\link[sparklyr]{sdf_schema}}
sdf_schema_json <- function(x, parse_json=TRUE, simplify=FALSE, append_complex_type=TRUE){
  sdf <- spark_dataframe(x)
  schema <- sdf %>%
    invoke("schema") %>%
    invoke("json")
  
  if (!parse_json)
    return(schema)
  
  schema <- schema %>%
    fromJSON(simplifyVector = FALSE)

  if (simplify)
    schema <- simplify_schema(schema, append_complex_type)
  
  return(schema)
}

#' @rdname sdf_schema_json
#' @export
#' @importFrom listviewer jsonedit
#' @examples
#' \dontrun{
#' library(testthat)
#' library(jsonlite)
#' library(sparklyr)
#' library(sparklyr.nested)
#' sample_json <- paste0(
#'   '{"aircraft_id":["string"],"phase_sequence":["string"],"phases (array)":{"start_point (struct)":',
#'   '{"segment_phase":["string"],"agl":["double"],"elevation":["double"],"time":["long"],',
#'   '"latitude":["double"],"longitude":["double"],"altitude":["double"],"course":["double"],',
#'   '"speed":["double"],"source_point_keys (array)":["[string]"],"primary_key":["string"]},',
#'   '"end_point (struct)":{"segment_phase":["string"],"agl":["double"],"elevation":["double"],',
#'   '"time":["long"],"latitude":["double"],"longitude":["double"],"altitude":["double"],',
#'   '"course":["double"],"speed":["double"],"source_point_keys (array)":["[string]"],',
#'   '"primary_key":["string"]},"phase":["string"],"primary_key":["string"]},"primary_key":["string"]}'
#' )
#' 
#' with_mock(
#'   # I am mocking functions so that the example works without a real spark connection
#'   spark_read_parquet = function(x, ...){return("this is a spark dataframe")},
#'   sdf_schema_json = function(x, ...){return(fromJSON(sample_json))},
#'   spark_connect = function(...){return("this is a spark connection")},
#'   
#'   # the meat of the example is here
#'   sc <- spark_connect(),
#'   spark_data <- spark_read_parquet(sc, path="path/to/data/*.parquet", name="some_name"),
#'   sdf_schema_viewer(spark_data)
#' )
#' }
sdf_schema_viewer <- function(x, simplify=TRUE,  append_complex_type=TRUE) {
  schema <- sdf_schema_json(x, simplify=simplify, append_complex_type=append_complex_type)
  jsonedit(schema) 
}

#' @keywords internal
simplify_schema <- function(schema, append_complex_type) {
  
  if (!is.list(schema))
    return(schema)
  
  type <- schema$type
  if (is.character(type)) {
    if (type=="struct") {
      fldnames <- unlist(lapply(schema$fields, function(fld){return(fld$name)}))
      if (append_complex_type) {
        fldtypes <- unlist(lapply(schema$fields, get_field_type))
        id <- !is.na(fldtypes)
        fldnames[id] <- paste0(fldnames[id], " (", fldtypes[id], ")")
      }
      l <- vector(mode="list", length=length(fldnames))
      for (i in 1:length(fldnames)) {
        l[[i]] <- simplify_schema(schema$fields[[i]], append_complex_type=append_complex_type)
      }
      names(l) <- fldnames
      return(l)
    } else if (type=="array") {
      if (is.character(schema$elementType))
        return(paste0("[", schema$elementType, "]"))
      else
        return(simplify_schema(schema$elementType, append_complex_type=append_complex_type))
    } else 
      return(type)
      
  } else if (is.list(type)) {
    return(simplify_schema(schema$type, append_complex_type=append_complex_type))
  }
}

get_field_type <- function(field) {
  
  if (is.character(field$type))
    return(NA_character_)
  return(field$type$type)
}
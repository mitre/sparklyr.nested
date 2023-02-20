#' Unnest data along a column
#' 
#' Unnesting is an (optional) explode operation coupled with a nested select to promote the sub-fields of
#' the exploded top level array/map/struct to the top level. Hence, given \code{a}, an array with fields
#' \code{a1, a2, a3}, then code{sdf_explode(df, a)} will produce output with each record replicated
#' for every element in the \code{a} array and with the fields \code{a1, a2, a3} (but not \code{a})
#' at the top level. Similar to \code{tidyr::unnest}.
#' 
#' Note that this is a less precise tool than using \code{\link{sdf_explode}} and \code{\link{sdf_select}}
#' directly because all fields of the exploded array will be kept and promoted. Direct calls to these
#' methods allows for more targeted use of \code{\link{sdf_select}} to promote only those fields that
#' are wanted to the top level of the data frame.
#' 
#' Additionally, though \code{\link{sdf_select}} allows users to reach arbitrarily far into a nested
#' structure, this function will only reach one layer deep. It may well be that the unnested fields
#' are themselves nested structures that need to be dealt with accordingly.
#' 
#' Note that map types are supported, but there is no \code{is_map} argument. This is because the
#' function is doing schema interrogation of the input data anyway to determine whether an explode
#' operation is required (it is of maps and arrays, but not for bare structs). Given this the result
#' of the schema interrogation drives the value o \code{is_map} provided to \code{sdf_explode}.
#' 
#' @param x An object (usually a \code{spark_tbl}) coercible to a Spark DataFrame.
#' @param column The field to explode
#' @param keep_all Logical. If \code{FALSE} then records where the exploded value is empty/null
#'   will be dropped.
#' 
#' @importFrom rlang !!! enquo quo_name !!
#' @importFrom dplyr everything %>%
#' @export
#' 
#' @examples 
#' \dontrun{
#' # first get some nested data
#' iris_tbl <- copy_to(sc, iris, name="iris")
#' iris_nst <- iris_tbl %>%
#'   sdf_nest(Sepal_Length, Sepal_Width, Petal_Length, Petal_Width, .key="data") %>%
#'   group_by(Species) %>%
#'   summarize(data=collect_list(data))
#' 
#' # then explode it
#' iris_nst %>% sdf_unnest(data)
#' }
sdf_unnest <- function(x, column, keep_all=FALSE) {
  
  col_quosure <- enquo(column)
  col_name <- quo_name(col_quosure)
  
  schema <- sdf_schema_json(x, simplify = FALSE, append_complex_type = FALSE)
  schema <- schema[["fields"]]
  names(schema) <- unlist(lapply(schema,  function(y){y[[1]]}))
  fld_type <- get_field_type(schema[[col_name]])
  if (fld_type == "array")
    x <- sdf_explode(x, !!col_quosure, is_map = FALSE, keep_all = keep_all)
  else if (fld_type == "map")
    x <- sdf_explode(x, !!col_quosure, is_map = TRUE, keep_all = keep_all)

  # get nested field columns (representing struct fields, not array fields, since explosion already happened)
  nested_schema <- x %>%
    sdf_select(!!col_quosure) %>%
    sdf_schema_json(simplify=TRUE, append_complex_type=FALSE)
  nested_aliases <- names(nested_schema[[col_name]])
  nested_select_fields <- paste0(col_name, ".", nested_aliases)
  
  # do select
  sdf_select(x, everything(), !!! nested_select_fields, .drop_parents = TRUE)
}

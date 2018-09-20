#' Unnest data along a column
#' 
#' Unnesting is an explode operation coupled with a nested select to promote the sub-fields of
#' the exploded top level array to the top level. Hence, given \code{a}, an array with fields
#' \code{a1, a2, a3}, then code{sdf_explode(df, a)} will produce output with each record replicated
#' for every element in the \code{a} array and with the fields \code{a1, a2, a3} (but not \code{a})
#' at the top level. Similar to \code{tidyr::explode}.
#' 
#' Note that this is a less precise tool than using \code{\link{sdf_explode}} and \code{\link{sdf_select}}
#' directly because all fields of the exploded array will be kept and promoted. Direct calls to these
#' methods allows for more targetted use of \code{\link{sdf_select}} to promote only those fields that
#' are wanted to the top level of the data frame.
#' 
#' Additionally, though \code{\link{sdf_select}} allows users to reach arbitrarily far into a nested
#' structure, this function will only reach one layer deep. It may well be that the unnested fields
#' are themselves nested structures that need to be dealt with accordingly.
#' 
#' @param x An object (usually a \code{spark_tbl}) coercable to a Spark DataFrame.
#' @param column The column to unnest
#' @param prepend Character. In the event that a nested field to be promoted has the same name as an
#'   existing top level field then this string will be prepended to the field name. The default 
#'   behavior is to prepend using the name of the field that is being unnested with an underscore.
#' @param prepend_all Logical. If \code{TRUE} then the \code{prepend} argument will be used for all
#'   promoted fields even if no name conflict exists.
#' 
#' @importFrom rlang !!! enquo quo_name !!
#' @importFrom dplyr everything %>%
#' @export
#' 
#' @examples 
#' \dontrun{
#' # first get some nested data
#' iris2 <- copy_to(sc, iris, name="iris")
#' iris_nst <- iris2 %>%
#'   sdf_nest(Sepal_Length, Sepal_Width, Petal_Length, Petal_Width, .key="data") %>%
#'   group_by(Species) %>%
#'   summarize(data=collect_list(data))
#' 
#' # then explode it
#' iris_nst %>% sdf_unnest(data)
#' }
sdf_unnest <- function(x, column, ..., is_map=FALSE, keep_all=FALSE) {
  
  col_quosure <- enquo(column)
  col_name <- quo_name(col_quosure)
  x <- sdf_explode(x, !!col_quosure, is_map = is_map, keep_all = keep_all)

  # get nested field columns (representing struct fields, not array fields, since explosion already happened)
  nested_schema <- x %>%
    sdf_select(!!col_quosure) %>%
    sdf_schema_json(simplify=TRUE, append_complex_type=FALSE)
  nested_aliases <- names(nested_schema[[col_name]])
  nested_select_fields <- paste0(col_name, ".", nested_aliases)
  
  # do select
  sdf_select(x, everything(), !!! nested_select_fields, .drop_parents = TRUE)
}
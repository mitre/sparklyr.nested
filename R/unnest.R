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
#' @export
sdf_unnest <- function(x, column, prepend, prepend_all=TRUE) {
  
  col_name <- deparse(substitute(column))
  return(sdf_unnest_(x, column=col_name, prepend=prepend, prepend_all=prepend_all))
}

#' @rdname sdf_unnest
#' @export
sdf_unnest_ <- function(x, column, prepend, prepend_all=FALSE) {

  stop("function temporarily broken")
  # default behavior
  if (missing(prepend))
    prepend <- paste0(column, "_")
  
  # first explode along the column to unnest  
  df <- sdf_explode_(x, column)

  # get nested field columns (representing struct fields, not array fields, since explosion already happened)
  # nested_schema <- df %>%
  #   sdf_select_(column) %>%
  #   sdf_schema_json(simplify=TRUE, append_complex_type=FALSE)
  # nested_schema <- sdf_schema_json(sdf_select_(df, column), simplify=TRUE, append_complex_type=FALSE)
  nested_aliases <- names(nested_schema[[column]])
  nested_select_fields <- paste0(column, ".", nested_aliases)
  
  # get other fields to keep
  fields <- colnames(df)
  
  # resolve name conflicts
  if (prepend_all)
    id <- rep(TRUE, times=length(nested_aliases))
  else
    id <- nested_aliases %in% fields

  if (any(id)) {
    if (!prepend_all) {
      message("Field name conflicts detected for nested fields: ", 
              paste0(nested_aliases[id], collapse=", "),
              ". These fields will be prepended with ", prepend)
    }
    
    nested_aliases[id] <- paste0(prepend, nested_aliases[id])
  }
  
  # add in other top level fields
  ind <- which(fields==column)
  select_fields <- c(fields[1:ind-1], nested_select_fields, fields[(ind+1):length(fields)])
  aliases <- c(fields[1:ind-1], nested_aliases, fields[(ind+1):length(fields)])
  
  # # do select
  # sdf_select_(df, .dots=select_fields, aliases=aliases)
}
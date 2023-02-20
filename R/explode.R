#' Explode data along a column
#' 
#' Exploding an array column of length \code{N} will replicate the top level record \code{N} times.
#' The i^th replicated record will contain a struct (not an array) corresponding to the i^th element
#' of the exploded array. Exploding will not promote any fields or otherwise change the schema of
#' the data. 
#' 
#' Two types of exploding are possible. The default method calls the scala \code{explode} method.
#' This operation is supported in both Spark version > 1.6. It will however drop records where the
#' exploding field is empty/null. Alternatively \code{keep_all=TRUE} will use the \code{explode_outer}
#' scala method introduced in spark 2 to not drop any records.
#' 
#' @param x An object (usually a \code{spark_tbl}) coercible to a Spark DataFrame.
#' @param column The field to explode
#' @param is_map Logical. The (scala) \code{explode} method works for both \code{array} and \code{map}
#'   column types. If the column to explode in an array, then \code{is_map=FALSE} will ensure that
#'   the exploded output retains the name of the array column. If however the column to explode is
#'   a map, then the map will have key/value names that will be used if \code{is_map=TRUE}.
#' @param keep_all Logical. If \code{FALSE} then records where the exploded value is empty/null
#'   will be dropped.
#'
#' @importFrom rlang enquo quo_name
#' @importFrom sparklyr invoke invoke_static spark_dataframe spark_connection sdf_register
#' @importFrom dplyr %>%
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
#' iris_nst %>% sdf_explode(data)
#' }
sdf_explode <- function(x, column, is_map=FALSE, keep_all=FALSE) {
  
  # capture column name for field to explode
  col_quosure <- enquo(column)
  col_name <- quo_name(col_quosure)
  
  # get refs
  sdf <- spark_dataframe(x)
  sc <- spark_connection(x)
  
  # explode_outer in spark 2.2+ only
  scala_method = ifelse(keep_all, "explode_outer", "explode")
  
  # idetnify columns
  cols <- colnames(x)
  columns <- lapply(cols, function(field) {
    sdf_col <- invoke(sdf, "col", field)
    if (field == col_name) {
      sdf_col <- invoke_static(sc, method=scala_method, 
                               class="org.apache.spark.sql.functions", 
                               sdf_col)
      if (!is_map)
        sdf_col <- invoke(sdf_col, "alias", field)
    }
    
    return(sdf_col)
  })
  
  # do select
  outdf <- sdf %>%
    invoke("select", columns)

  # regisger new table
  sdf_register(outdf)
  
}

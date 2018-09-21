#' Nest data in a Spark Dataframe
#' 
#' This function is like \code{tidyr::nest}. Calling this function will not
#' aggregate over other columns. Rather the output has the same number of
#' rows/records as the input. See examples of how to achieve row reduction
#' by aggregating elements using \code{collect_list}, which is a Spark SQL function
#' 
#' @examples
#' \dontrun{
#' # produces a dataframe with an array of characteristics nested under
#' # each unique species identifier
#' iris2 <- copy_to(sc, iris, name="iris")
#' iris2 %>%
#'   sdf_nest(Sepal_Length, Sepal_Width, Petal_Length, Petal_Width, .key="data") %>%
#'   group_by(Species) %>%
#'   summarize(data=collect_list(data))
#' }
#' 
#' @param x A Spark dataframe.
#' @param ... Columns to nest.
#' @param .key Character. A name for the new column containing nested fields
#' @importFrom dplyr select_vars
#' @export
sdf_nest <- function(x, ..., .key="data") {
  
  vars_to_nest <- select_vars(colnames(x), ...)
  
  sdf <- spark_dataframe(x)
  sc <- spark_connection(x)
  
  cols <- colnames(x)
  nested_columns <- list()
  select_columns <- list()
  
  for (col in cols) {
    sdf_col <- invoke(sdf, "col", col)
    if (col %in% vars_to_nest) {
      nested_columns <- c(nested_columns, sdf_col)
    } else {
      select_columns <- c(select_columns, sdf_col)
    }
  }
  
  nested_column <- invoke_static(sc, method="struct", 
                                 class="org.apache.spark.sql.functions",
                                 nested_columns)
  nested_column <- invoke(nested_column, "alias", .key)
  
  # do select
  # outdf <- sdf %>% 
  #   invoke("select", columns) 
  outdf <- invoke(sdf, "select", c(select_columns, nested_column)) 
  
  # regisger new table
  sdf_register(outdf)
}

#' Nest data in a Spark Dataframe
#' 
#' This function is designed to behave similarly to \code{tidyr::nest}.
#' 
#' Note that calling \code{sdf_nest} will not aggregate and cannot be done
#' inside of a \code{group_by(...) %>% summarize(..)} operation. To produce
#' a nested array one might use \code{sdf_nest} in conjunction with the 
#' \code{collect_list} Spark SQL function:
#' 
#' @examples 
#' \dontrun{
#' # produces a dataframe with an array of characteristics nested under
#' # each unique species identifier
#' iris2 <- copy_to(sc, iris, name="iris")
#' iris2 %>%
#'   sdf_nest(Sepal_Length, Sepal_Width, Petal.Length, Petal.Width, .key="data") %>%
#'   group_by(Species) %>%
#'   summarize(data=collect_list(data))
#' }
#' 
#' @param x A Spark dataframe.
#' @param ... Columns to nest.
#' @param .key Character. A name for the new column containing nested fields
#' @export
sdf_nest <- function(x, ..., .key="data") {
  
  dots <- convert_dots_to_strings(...)
  sdf_nest_(x, columns=dots, .key=.key)
}

#' @rdname sdf_nest
#' @param columns Character vector. Columns to nest.
#' @export
sdf_nest_ <- function(x, columns, .key="data") {
  
  sdf <- spark_dataframe(x)
  sc <- spark_connection(x)
  
  cols <- colnames(x)
  nested_columns <- list()
  select_columns <- list()
  
  for (col in cols) {
    sdf_col <- invoke(sdf, "col", col)
    if (col %in% columns) {
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

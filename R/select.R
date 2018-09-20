#' Select nested items
#' 
#' The \code{select} function works well for keeping/dropping top level fields. It does not
#' however support access to nested data. This function will accept complex field names
#' such as \code{x.y.z} where \code{z} is a field nested within \code{y} which is in turn
#' nested within \code{x}. Since R uses "$" to access nested elements and java/scala use ".",
#' \code{sdf_select(data, x.y.z)} and \code{sdf_select(data, x$y$z)} are equivalent.
#' 
#' @section Selection Helpers:
#' 
#' \code{dplyr} allows the use of selection helpers (e.g., see \code{\link[dplyr]{everything}}). 
#' These same helpers are supported for \code{sdf_select}, but not for \code{sdf_select_}.
#' 
#' @param x An object (usually a \code{spark_tbl}) coercable to a Spark DataFrame.
#' @param ... Fields to select
#' @param .aliases Character. Optional. If provided these names will be matched positionally with
#'   select fields provided in \code{...}. This is more useful when calling from a function and
#'   less natural to use when calling the function directly. The alternative with direct calls
#'   is to put the alias on the left side of the expression (e.g. \code{sdf_select(df, fld_alias=parent.child.fld)})
#' @param .drop_parents Logical. If \code{TRUE} then any field from which nested elements are extracted
#'   will be dropped, even if they were included in the selected \code{...}. This better supports using 
#'   \code{dplyr} field matching helpers like \code{everything()} and \code{starts_with}.
#' @param .dots List. Treated just like \code{...} except that the named arguments are in a list
#' @importFrom lazyeval lazy_dots
#' @importFrom dplyr select_vars
#' @export
#' 
#' @examples 
#' \dontrun{
#' # produces a dataframe with an array of characteristics nested under
#' # each unique species identifier
#' iris2 <- copy_to(sc, iris, name="iris")
#' iris_nst <- iris2 %>%
#'   sdf_nest(Sepal_Length, Sepal_Width, .key="Sepal") 
#' 
#' # using java-like dot-notation
#' iris_nst %>%
#'   sdf_select(Species, Petal_Width, Sepal.Sepal_Width)
#' 
#' # using R-like dollar-sign-notation
#' iris_nst %>%
#'   sdf_select(Species, Petal_Width, Sepal$Sepal_Width)
#'   
#' # using dplyr selection helpers
#' iris_nst %>%
#'   sdf_select(Species, matches("Petal"), Sepal$Sepal_Width)
#' }
sdf_select <- function(x, ..., .aliases, .drop_parents=TRUE) {
  
  dots <- quos(...)
  arg_names <- unlist(lapply(dots, quo_name))
  id <- is_nested_field_ref(arg_names)
  top_level_vars <- do.call(select_vars, c(list(colnames(x)), dots[!id]))
  nested_vars <- arg_names[id]
  sdf_select_(x, .dots=c(top_level_vars, nested_vars), .aliases=.aliases)
}

#' @rdname sdf_select
#' @importFrom lazyeval all_dots
#' @importFrom sparklyr invoke
#' @importFrom sparklyr spark_dataframe
#' @importFrom sparklyr sdf_register
#' @export
sdf_select_ <- function(x, ..., .dots, .aliases) {
  
  dots <- all_dots(.dots, ...)
  select_cols <- unlist(lapply(dots, function(x){deparse(x$expr)}))
  
  # support both dot and dollar sign notation
  select_cols <- gsub("$", ".", select_cols, fixed=TRUE)
  
  # add aliases
  if (missing(.aliases) & any(names(dots) != "")) {
    .aliases <- names(dots)
    id <- .aliases==""
    .aliases[id] <- select_cols[id]
    .aliases <- gsub(".", "_", .aliases, fixed=TRUE)
  }
  
  if (!missing(.aliases) && length(.aliases) != length(dots))
    stop("If aliases are provided the length of the aliases vector must match the number of fields being selected")
  
  sdf <- spark_dataframe(x)
  
  # idetnify columns
  if (missing(.aliases)) {
    columns <- lapply(select_cols, function(arg) {
      invoke(sdf, "col", arg)
    })
  } else {
    columns <- mapply(FUN=function(arg, name) {
      # invoke(sdf, "col", arg) %>%
      #   invoke("alias", name)
      invoke(invoke(sdf, "col", arg), "alias", name)
    }, select_cols, .aliases)
  }
  
  # do select
  outdf <- invoke(sdf, "select", columns)
  
  # register new table
  sdf_register(outdf)
}

is_nested_field_ref <- function(names) {
  return(grepl("\\.|\\$", names) & !grepl("(", names, fixed=TRUE))
}
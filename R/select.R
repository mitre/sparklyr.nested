#' Select nested items
#' 
#' The \code{select} function works well for keeping/dropping top level fields. It does not
#' however support access to nested data. This function will accept complex field names
#' such as \code{x.y.z} where \code{z} is a field nested within \code{y} which is in turn
#' nested within \code{x}. Since R uses "$" to access nested elements and java/scala use ".",
#' \code{sdf_select(data, x.y.z)} and \code{sdf_select(data, x$y$z)} are equivalent.
#' 
#' @param x An object (usually a \code{spark_tbl}) coercable to a Spark DataFrame.
#' @param ... Fields to select
#' @param aliases Character. Optional. If provided these names will be matched positionally with
#'   select fields provided in \code{...}. This is more useful when calling from a function and
#'   less natural to use when calling the function directly. The alternative with direct calls
#'   is to put the alias on the left side of the expression (e.g. \code{sdf_select(df, fld_alias=parent.child.fld)})
#' @param .dots List. Treated just like \code{...} except that the named arguments are in a list
#' @importFrom lazyeval lazy_dots
#' @export
sdf_select <- function(x, ..., aliases) {
  
  dots <- convert_dots_to_strings(...)
  sdf_select_(x, .dots=dots, aliases=aliases)
}

#' @rdname sdf_select
#' @importFrom lazyeval all_dots
#' @importFrom sparklyr invoke
#' @importFrom sparklyr spark_dataframe
#' @importFrom sparklyr sdf_register
#' @export
sdf_select_ <- function(x, ..., .dots, aliases) {
  
  dots <- all_dots(.dots, ...)
  select_cols <- select_cols <- unlist(lapply(dots, function(x){deparse(x$expr)}))
  
  # support both dot and dollar sign notation
  select_cols <- gsub("$", ".", select_cols, fixed=TRUE)
  
  # add aliases
  if (missing(aliases) & any(names(dots) != "")) {
    aliases <- names(dots)
    id <- aliases==""
    aliases[id] <- select_cols[id]
    aliases <- gsub(".", "_", aliases, fixed=TRUE)
  }
  
  if (!missing(aliases) && length(aliases) != length(dots))
    stop("If out names are provided the length of the names vector must match the number of fields being selected")
  
  sdf <- spark_dataframe(x)
  
  # idetnify columns
  if (missing(aliases)) {
    columns <- lapply(select_cols, function(arg) {
      invoke(sdf, "col", arg)
    })
  } else {
    columns <- mapply(FUN=function(arg, name) {
      # invoke(sdf, "col", arg) %>%
      #   invoke("alias", name)
      invoke(invoke(sdf, "col", arg), "alias", name)
    }, select_cols, aliases)
  }
  
  # do select
  outdf <- invoke(sdf, "select", columns)
  
  # register new table
  sdf_register(outdf)
}
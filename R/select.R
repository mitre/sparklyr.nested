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
#' These helpers only work for top level fields however. For now all nested fields that should
#' be promoted need to be explicitly identified. 
#' 
#' @param x An object (usually a \code{spark_tbl}) coercible to a Spark DataFrame.
#' @param ... Fields to select
#' @param .drop_parents Logical. If \code{TRUE} then any field from which nested elements are extracted
#'   will be dropped, even if they were included in the selected \code{...}. This better supports using 
#'   \code{dplyr} field matching helpers like \code{everything()} and \code{starts_with}.
#' @param .full_name Logical. If \code{TRUE} then nested field names that are not named (either using
#'   a LHS \code{name=field_name} construct or the \code{.aliases} argument) will be disambiguated using
#'   the parent field name. For example \code{sdf_select(df, x.y)} will return a field named \code{x_y}.
#'   If \code{FALSE} then the parent field name is dropped unless it is needed to avoid duplicate names.
#' @param .aliases Character. Optional. If provided these names will be matched positionally with
#'   selected fields provided in \code{...}. This is more useful when calling from a function and
#'   less natural to use when calling the function directly. It is likely to get you into trouble
#'   if you are using \code{dplyr} select helpers. The alternative with direct calls
#'   is to put the alias on the left side of the expression (e.g. \code{sdf_select(df, fld_alias=parent.child.fld)})
#' @importFrom dplyr select_vars %>%
#' @importFrom purrr map flatten_chr
#' @importFrom rlang !!! quos quo_name set_names
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
sdf_select <- function(x, ..., .aliases, .drop_parents=TRUE, .full_name=FALSE) {
  
  dots <- quos(...)
  
  # need to pull out nested field refs since select_vars will not find them
  arg_strings <- dots %>% 
    map(quo_name) %>%
    flatten_chr() %>%
    set_names(names(dots))
  id <- is_nested_field_ref(arg_strings)
  
  # collect field names to select as strings
  # `!!!` will splice dots[!id] into `...` of select_vars
  top_level_vars <- select_vars(colnames(x), !!! dots[!id])
  nested_vars <- arg_strings[id] %>%
    # support both dot and dollar sign notation
    gsub(pattern="$", replacement=".", fixed=TRUE)
  select_cols <- c(top_level_vars, nested_vars)
  
  # drop parents as directed
  if (.drop_parents) {
    nested_fields_accessed <- select_cols[grepl(".", select_cols, fixed = TRUE)] %>%
      strsplit(split=".", fixed=TRUE) %>%
      map(function(y){return(y[[1]])}) %>%
      flatten_chr()
    
    select_cols <- select_cols[!(select_cols %in% nested_fields_accessed)]
  }
  
  # add aliases
  if (missing(.aliases)) {
    .aliases <- names(select_cols)
    id <- .aliases==""
    .aliases[id] <- select_cols[id]
    
    if (.full_name) {
      .aliases <- gsub(".", "_", .aliases, fixed=TRUE)
    } else {
      nested_names <- nested_vars %>%
        strsplit(split=".", fixed=TRUE) %>%
        map(function(y){return(y[[2]])}) %>%
        flatten_chr()
      if (any(nested_names %in% .aliases)) {
        .aliases <- gsub(".", "_", .aliases, fixed=TRUE)
        warning("Variable name conflict detected, using disambuigated names for all nested fields")
      } else {
        .aliases[grepl(".", .aliases, fixed=TRUE)] <- nested_names
      }
    }
  } else if (length(.aliases) != length(select_cols)) {
    stop("If aliases are provided the length of the aliases vector must match the number of fields being selected")
  }
  
  sdf <- spark_dataframe(x)
  
  # idetnify columns
  columns <- mapply(FUN=function(arg, name) {
    invoke(sdf, "col", arg) %>%
      invoke("alias", name) %>%
      return()
  }, select_cols, .aliases)
  
  # do select
  outdf <- invoke(sdf, "select", columns)
  
  # register new table
  sdf_register(outdf)
}

is_nested_field_ref <- function(names) {
  return(grepl("\\.|\\$", names) & !grepl("(", names, fixed=TRUE))
}
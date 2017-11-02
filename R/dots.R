#' @keywords internal
#' 
#' Turn ellipsis arguments into partially evaluated string arguments. These
#' will be used as arguments to a scala method, so character type is needed.
#' Partial evaluation is needed such that Spark data frame names are retained
#' unevaluated, but other elements (e.g., the RHS of a filter statement) from
#' the local R session are evaluated into numeric or character values to be
#' passed to the Spark executors.
#' 
#' @importFrom lazyeval lazy_dots
convert_dots_to_strings <- function(...) {
  
  # parse dots
  dots <- lazyeval::lazy_dots(...)
  txt <- vector(mode = "character", length=length(dots))
  for (i in seq_along(dots)) {
    txt[i] <- parse_name_to_string(dots[[i]])
  }
  
  # add names back in
  names(txt) <- names(dots)
  
  return(txt)
}

#' @keywords internal
#' 
#' Function to deal with the oddball case of wanting to half-parse expressions.
#' At issue here is the fact that some variables come from a local R session and 
#' should be used (via the scala API) by the remote Spark executors. Thus we want
#' to leave Spark data frame column references alone while we replace strings and
#' numbers with their values from local
#' 
#' @importFrom lazyeval lazy_eval
#' @importFrom lazyeval as.lazy
parse_name_to_string <- function(lazy_expr) {
  
  # easy solution when this thing can be evaluated
  value <- tryCatch(as.character(lazy_eval(lazy_expr)),
                    error=function(cond) {return(NA_character_)})
  if (!is.na(value))
    return(value)
  
  # not it gets a bit trickier
  name <- lazy_expr$expr
  env <- lazy_expr$env
  n_components <- length(name)
  
  # length 1 names probably refer to variable names, likely in spark land
  if (n_components==1)
    return(substitute_constant(deparse(name), env))
  
  # break out name into string pieces
  components <- as.character(name)
  vars <- ls(envir=env)
  
  # special handling for $ operator to support nested calls to spark data frames
  if (components[1]=="$" && !(components[2] %in% vars))
    return(deparse(name))

  # recurse
  components <- lapply(name, function(component){
    parse_name_to_string(as.lazy(component, env))
    })
  
  # put it all back together
  return(assemble_components_as_character(components))
}

#' @keywords internal
#' 
#' When a simple value exists in the calling environment, use that value in place 
#' of the variable. Most likely Spark will not know about the value otherwise
substitute_constant <- function(x, env) {
  if (!(x %in% ls(envir=env)))
    return(x)
  
  value <- get(x, envir=env)
  if (length(value==1) && mode(value) %in% c("numeric", "character"))
    return(value)
  
  warning(paste0("Found ", x, " in calling environment, but could not use it in expression because either ",
                 "it is not of length 1 or else it is not a character or numeric value"))
  return(x)
}

#' @keywords internal
#' 
#' Handle cases of typical functions along the lines of f(x, y) as well as of the form x+y
assemble_components_as_character <- function(components) {
  n <- length(components)
  
  # length 3 names are more interesting, in this case it could be a standard function call,
  # it could be a subsetting (`[) or indexing (`$) operation, or it could be an operator-style
  # function call (e.g., x+3 or x %in% c("a", "b", "c"))
  if (!grepl("^[:alpha:]", components) && n==3)
    string <- paste0(components[2], components[1], components[3],
                     ifelse(components[1]=="[", yes="]", no=""))
  else
    string <- paste0(components[1], "(", paste0(components[2:n], collapse=", "), ")")
  
  # for the Spark column, the indexing separator is a period, not a dollar sign
  return(gsub("$", ".", string, fixed=TRUE))
}
#' Subset an Avro schema to a set of fields
#'
#' Returns a simplified Avro schema containing only the specified fields.
#' Nested fields (e.g. \code{points.latitude} in a \code{points} array of records)
#' are supported. The field specification is the same for a field in a nested array
#' of records or for a field in a nested record. The function will detect the type
#' and produce a valid sub-schema.
#'
#' The purpose of a sub-schema is use with \code{\link[sparklyr]{spark_read_avro}} or
#' \code{\link[sparklyr]{spark_read_parquet}} to read data more efficiently into Spark
#' DataFrames.
#'
#' @section Return types:
#' Both functions accept the same schema and field arguments, but differ in their
#' return value:
#' \describe{
#'   \item{\code{avro_subschema}}{Returns a JSON string representing the subsetted
#'     Avro schema. Useful when you need the schema as text (e.g. for storage,
#'     logging, or passing to another system).}
#'   \item{\code{avro_subschema_jobj}}{Returns a Spark \code{StructType} Java
#'     object (\code{spark_jobj}), ready to pass directly to
#'     \code{\link[sparklyr]{spark_read_parquet}} or
#'     \code{\link[sparklyr]{spark_read_avro}} as the \code{schema} argument.
#'     Requires an active \code{spark_connection}.}
#' }
#'
#' @param schema Character or list. The Avro schema to subset (JSON string or
#'   parsed list). This can be extracted using \code{\link{sdf_schema_json}}.
#' @param fields List or character. The fields to keep. If a list, use nested
#'   structure mirroring the schema; if a character vector, use dot or dollar
#'   notation for nested paths.
#'
#'   List form: \code{list("primary_key", "points" = list("latitude", "longitude", "altitude"))}
#'
#'   Character form: \code{c("primary_key", "points.latitude", "points.longitude", "points.altitude")}
#'   or \code{c("primary_key", "points$latitude", "points$longitude", "points$altitude")}
#'   (dot and dollar are equivalent).
#' @param keep_docs Logical. If \code{FALSE} (default), remove \code{doc} entries from the schema.
#' @param keep_namespace Logical. If \code{FALSE} (default), remove \code{namespace} entries.
#' @param keep_java_class Logical. If \code{FALSE} (default), remove \code{java-class} entries.
#'   Defaults to \code{keep_namespace}.
#' @param keep_default Logical. If \code{FALSE} (default), remove \code{default} entries from fields.
#' @param ... Arguments passed to \code{\link[jsonlite]{toJSON}}
#'
#' @return \code{avro_subschema}: A JSON string (subsetted schema).
#'
#' @examples
#' \dontrun{
#' schema <- '{"type":"record","name":"X","fields":[{"name":"a","type":"string"},{"name":"b","type":"int"}]}'
#'
#' # JSON string output
#' avro_subschema(schema, "a")
#' avro_subschema(schema, list("a", "b"))
#'
#' # Spark StructType output (requires a spark connection)
#' sc <- sparklyr::spark_connect(master = "local")
#' jobj <- avro_subschema_jobj(sc, schema, "a")
#' df <- sparklyr::spark_read_parquet(sc, path = "data.parquet", schema = jobj)
#' }
#'
#' @rdname avro_subschema
#' @export
avro_subschema <- function(
  schema,
  fields,
  keep_docs = FALSE,
  keep_namespace = FALSE,
  keep_java_class = keep_namespace,
  keep_default = FALSE,
  ...
) {
  if (is.character(schema)) {
    schema <- jsonlite::fromJSON(schema, simplifyVector = FALSE)
  }
  if (!is.list(schema)) {
    rlang::abort("`schema` must be a character (JSON) or list.")
  }
  schema <- resolve_schema_references(schema)
  fields <- normalize_fields_spec(fields)
  invalid <- validate_subschema_fields(schema, fields)
  if (length(invalid) > 0) {
    rlang::abort(
      c(
        "One or more requested fields are not valid in the schema.",
        "i" = paste0(
          "Invalid field(s): ",
          paste0("\"", invalid, "\"", collapse = ", ")
        ),
        "x" = "Each field must exist in the schema. Use dot notation for nested paths (e.g. \"points.latitude\")."
      ),
      class = "subschema_invalid_fields"
    )
  }
  opts <- list(
    keep_docs = keep_docs,
    keep_namespace = keep_namespace,
    keep_java_class = keep_java_class,
    keep_default = keep_default
  )
  schema$fields <- subset_schema_fields(schema$fields, fields, opts)
  schema <- strip_schema_meta(schema, opts)
  schema |> jsonlite::toJSON(auto_unbox = TRUE, ...)
}

# Infer full Avro name for a record (namespace.name or name)
# @keywords internal
infer_record_full_name <- function(rec) {
  if (!is.list(rec) || !identical(rec[["type"]], "record")) {
    return(NULL)
  }
  name <- rec[["name"]]
  ns <- rec[["namespace"]]
  if (is.null(name) || !nzchar(name)) {
    return(NULL)
  }
  if (is.null(ns) || !nzchar(ns)) {
    return(name)
  }
  paste0(ns, ".", name)
}

# Collect all named record types from schema: full_name -> record (for resolution)
# @keywords internal
collect_named_types <- function(schema, into = list()) {
  if (is.null(schema) || !is.list(schema)) {
    return(into)
  }
  if (identical(schema[["type"]], "record")) {
    full_name <- infer_record_full_name(schema)
    if (!is.null(full_name) && !full_name %in% names(into)) {
      into[[full_name]] <- schema
    }
  }
  if (identical(schema[["type"]], "record") && length(schema[["fields"]]) > 0) {
    for (f in schema[["fields"]]) {
      into <- collect_named_types(f[["type"]], into)
    }
  }
  if (identical(schema[["type"]], "array")) {
    items <- schema[["items"]]
    if (is.list(items)) {
      into <- collect_named_types(items, into)
    }
  }
  if (identical(schema[["type"]], "map")) {
    into <- collect_named_types(schema[["values"]], into)
  }
  if (is.null(schema[["type"]]) && is.list(schema) && length(schema) > 0) {
    for (branch in schema) {
      if (is.list(branch)) {
        into <- collect_named_types(branch, into)
      }
    }
  }
  into
}

# Resolve type reference (string) to definition, or recurse into type object
# @keywords internal
resolve_type <- function(type, named_types) {
  if (is.null(type)) {
    return(NULL)
  }
  if (is.character(type) && length(type) == 1 && nzchar(type)) {
    def <- named_types[[type]]
    if (!is.null(def)) {
      return(rlang::duplicate(def, shallow = FALSE))
    }
  }
  if (is.vector(type) && !is.list(type)) {
    return(type)
  }
  if (!is.list(type)) {
    return(type)
  }
  kind <- type[["type"]]
  if (identical(kind, "record")) {
    for (i in seq_along(type[["fields"]])) {
      f <- type[["fields"]][[i]]
      type[["fields"]][[i]][["type"]] <- resolve_type(f[["type"]], named_types)
    }
    return(type)
  }
  if (identical(kind, "array")) {
    items <- type[["items"]]
    if (is.character(items) && length(items) == 1) {
      def <- named_types[[items]]
      if (!is.null(def)) {
        type[["items"]] <- rlang::duplicate(def, shallow = FALSE)
      }
    } else if (is.list(items)) {
      type[["items"]] <- resolve_type(items, named_types)
    }
    return(type)
  }
  if (identical(kind, "map")) {
    type[["values"]] <- resolve_type(type[["values"]], named_types)
    return(type)
  }
  if (is.null(kind) && is.list(type) && length(type) > 0) {
    for (i in seq_along(type)) {
      if (is.list(type[[i]])) {
        type[[i]] <- resolve_type(type[[i]], named_types)
      } else if (is.character(type[[i]]) && length(type[[i]]) == 1) {
        def <- named_types[[type[[i]]]]
        if (!is.null(def)) {
          type[[i]] <- rlang::duplicate(def, shallow = FALSE)
        }
      }
    }
    return(type)
  }
  type
}

# Normalize schema by inlining all Avro type references
# @keywords internal
resolve_schema_references <- function(schema) {
  named_types <- collect_named_types(schema)
  if (length(named_types) == 0) {
    return(schema)
  }
  if (!identical(schema[["type"]], "record") || is.null(schema[["fields"]])) {
    return(schema)
  }
  for (i in seq_along(schema[["fields"]])) {
    f <- schema[["fields"]][[i]]
    schema[["fields"]][[i]][["type"]] <- resolve_type(f[["type"]], named_types)
  }
  schema
}

# Convert character fields to list form: c("a", "b.c") -> list("a", "b" = list("c"))
# @keywords internal
normalize_fields_spec <- function(fields) {
  if (is.list(fields)) {
    return(fields)
  }
  if (!is.character(fields)) {
    rlang::abort("`fields` must be a character vector or list.")
  }
  # Treat $ as . for path notation
  paths <- strsplit(gsub("$", ".", fields, fixed = TRUE), ".", fixed = TRUE)
  result <- list()
  for (path in paths) {
    path <- unlist(path)
    path <- path[nzchar(path)]
    if (length(path) == 0) {
      next
    }
    if (length(path) == 1) {
      result[[path[1]]] <- list()
    } else {
      parent <- path[1]
      child <- path[length(path)]
      if (is.null(result[[parent]]) || identical(result[[parent]], list())) {
        result[[parent]] <- list(child)
      } else {
        result[[parent]] <- c(result[[parent]], child)
      }
    }
  }
  # Build list("primary_key", "points" = list("latitude", ...)) format
  out <- list()
  for (nm in names(result)) {
    val <- result[[nm]]
    if (identical(val, list())) {
      out <- c(out, list(nm))
    } else {
      out[[nm]] <- val
    }
  }
  out
}

# Collect all valid field paths from a schema (e.g. c("primary_key", "points", "points.latitude"))
# @keywords internal
collect_schema_paths <- function(schema, prefix = "") {
  if (is.null(schema) || !is.list(schema)) {
    return(character())
  }
  fields <- schema$fields
  if (is.null(fields) || length(fields) == 0) {
    return(character())
  }
  paths <- character()
  for (f in fields) {
    name <- f$name
    if (is.null(name) || !nzchar(name)) {
      next
    }
    path <- if (nzchar(prefix)) paste0(prefix, name) else name
    paths <- c(paths, path)
    type <- f$type
    child_prefix <- paste0(path, ".")
    if (is.list(type)) {
      kind <- type[["type"]]
      if (identical(kind, "record")) {
        paths <- c(paths, collect_schema_paths(type, child_prefix))
      } else if (identical(kind, "array")) {
        items <- type[["items"]]
        if (is.list(items) && identical(items[["type"]], "record")) {
          paths <- c(paths, collect_schema_paths(items, child_prefix))
        }
      } else if (is.null(kind) && is.list(type) && length(type) > 0) {
        # Union: find non-null branch
        for (branch in type) {
          if (is.list(branch) && identical(branch[["type"]], "record")) {
            paths <- c(paths, collect_schema_paths(branch, child_prefix))
            break
          }
          if (is.list(branch) && identical(branch[["type"]], "array")) {
            items <- branch[["items"]]
            if (is.list(items) && identical(items[["type"]], "record")) {
              paths <- c(paths, collect_schema_paths(items, child_prefix))
            }
            break
          }
        }
      }
    }
  }
  paths
}

# Collect requested field paths from normalized spec
# @keywords internal
collect_requested_paths <- function(spec, prefix = "") {
  if (is.null(spec) || length(spec) == 0) {
    return(character())
  }
  paths <- character()
  for (i in seq_along(spec)) {
    nm <- names(spec)[i]
    val <- spec[[i]]
    if (is.null(nm) || is.na(nm) || !nzchar(nm)) {
      name <- if (is.list(val) && length(val) > 0) val[[1]] else val
      path <- if (nzchar(prefix)) paste0(prefix, name) else name
      paths <- c(paths, path)
    } else {
      path <- if (nzchar(prefix)) paste0(prefix, nm) else nm
      paths <- c(paths, path)
      if (is.list(val) && length(val) > 0) {
        paths <- c(paths, collect_requested_paths(val, paste0(path, ".")))
      }
    }
  }
  paths
}

# Validate that all requested fields exist in schema. Returns character vector of invalid paths.
# @keywords internal
validate_subschema_fields <- function(schema, fields_spec) {
  valid <- unique(collect_schema_paths(schema))
  requested <- unique(collect_requested_paths(fields_spec))
  requested[!requested %in% valid]
}

# Find field by name in schema fields list
# @keywords internal
find_field <- function(schema_fields, name) {
  for (f in schema_fields) {
    if (identical(f$name, name)) return(f)
  }
  NULL
}

# Subset schema fields based on spec
# spec: list("primary_key", "points" = list("latitude", "longitude"))
# @keywords internal
subset_schema_fields <- function(schema_fields, spec, opts) {
  if (is.null(schema_fields) || length(schema_fields) == 0) {
    return(list())
  }
  result <- list()
  for (i in seq_along(spec)) {
    nm <- names(spec)[i]
    val <- spec[[i]]
    if (is.null(nm) || is.na(nm) || !nzchar(nm)) {
      # Unnamed: val is the field name
      name <- if (is.list(val)) val[[1]] else val
      child_spec <- list()
    } else {
      name <- nm
      child_spec <- val
    }
    field <- find_field(schema_fields, name)
    if (is.null(field)) {
      next
    }
    if (length(child_spec) == 0) {
      result <- c(result, list(strip_field_meta(field, opts)))
    } else {
      subsetted <- subset_field_type(field$type, child_spec, opts)
      if (!is.null(subsetted)) {
        result <- c(result, list(list(name = field$name, type = subsetted)))
      }
    }
  }
  result
}

# Subset a field type (record, array, or union) by child spec
# @keywords internal
subset_field_type <- function(type, child_spec, opts) {
  if (is.null(type)) {
    return(NULL)
  }
  if (is.character(type)) {
    return(type)
  }
  # Union: [null, X] or similar - recurse into non-null branch
  if (is.vector(type) && !is.list(type)) {
    return(type)
  }
  if (!is.list(type)) {
    return(type)
  }
  kind <- type[["type"]]
  if (kind == "record") {
    type$fields <- subset_schema_fields(type$fields, child_spec, opts)
    return(strip_type_meta(type, opts))
  }
  if (kind == "array") {
    items <- type[["items"]]
    if (is.list(items) && identical(items[["type"]], "record")) {
      items$fields <- subset_schema_fields(items$fields, child_spec, opts)
      type$items <- strip_type_meta(items, opts)
    }
    return(strip_type_meta(type, opts))
  }
  if (kind == "map") {
    return(strip_type_meta(type, opts))
  }
  # Union type
  if (is.null(kind) && is.list(type) && length(type) > 0) {
    return(type)
  }
  strip_type_meta(type, opts)
}

# Remove doc, namespace, java-class from schema based on opts
# @keywords internal
strip_schema_meta <- function(x, opts) {
  if (!opts$keep_docs) {
    x$doc <- NULL
  }
  if (!opts$keep_namespace) {
    x$namespace <- NULL
  }
  if (!opts$keep_java_class) {
    x$`java-class` <- NULL
  }
  x
}

# Remove doc, namespace, java-class from type based on opts
# @keywords internal
strip_type_meta <- function(x, opts) {
  if (!opts$keep_docs) {
    x$doc <- NULL
  }
  if (!opts$keep_namespace) {
    x$namespace <- NULL
  }
  if (!opts$keep_java_class) {
    x$`java-class` <- NULL
  }
  x
}

# Remove doc, default, java-class from field based on opts
# @keywords internal
strip_field_meta <- function(x, opts) {
  if (!opts$keep_docs) {
    x$doc <- NULL
  }
  if (!opts$keep_default) {
    x$default <- NULL
  }
  if (!opts$keep_java_class) {
    x$`java-class` <- NULL
  }
  x
}

#' Convert an Avro schema to a Spark StructType Java object
#'
#' Parses an Avro JSON schema (character or list) and builds the corresponding
#' Spark \code{StructType} via \code{sparklyr.nested} type constructors. This is
#' useful for supplying a \code{schema} argument to
#' \code{\link[sparklyr]{spark_read_parquet}} or
#' \code{\link[sparklyr]{spark_read_avro}}.
#'
#' @param sc A \code{spark_connection}.
#' @param schema Character or list. An Avro schema (JSON string or parsed list).
#'
#' @return A Spark \code{StructType} Java object (\code{spark_jobj}).
#'
#' @examples
#' \dontrun{
#' sc <- sparklyr::spark_connect(master = "local")
#' schema <- '{"type":"record","name":"X","fields":[{"name":"a","type":"string"}]}'
#' jobj <- schema_to_jobj(sc, schema)
#' df <- sparklyr::spark_read_parquet(sc, path = "data.parquet", schema = jobj)
#' }
#'
#' @export
schema_to_jobj <- function(sc, schema) {
  if (is.character(schema)) {
    schema <- jsonlite::fromJSON(schema, simplifyVector = FALSE)
  }
  if (!is.list(schema)) {
    rlang::abort("`schema` must be a character (JSON) or list.")
  }
  avro_to_spark_type(sc, schema)
}

#' @rdname avro_subschema
#' @param sc A \code{spark_connection}.
#' @return \code{avro_subschema_jobj}: A Spark \code{StructType} Java object (\code{spark_jobj}).
#' @export
avro_subschema_jobj <- function(
  sc,
  schema,
  fields,
  keep_docs = FALSE,
  keep_namespace = FALSE,
  keep_java_class = keep_namespace,
  keep_default = FALSE,
  ...
) {
  sub_json <- avro_subschema(
    schema, fields,
    keep_docs = keep_docs,
    keep_namespace = keep_namespace,
    keep_java_class = keep_java_class,
    keep_default = keep_default,
    ...
  )
  schema_to_jobj(sc, sub_json)
}

# --- Avro-to-Spark type conversion helpers ---

# @keywords internal
avro_to_spark_type <- function(sc, avro_type, nullable = FALSE) {
  if (is.null(avro_type)) {
    return(string_type(sc))
  }
  if (is.character(avro_type)) {
    return(avro_primitive_to_spark(sc, avro_type))
  }
  # Unnamed list = union type (e.g. ["null", "string"])
  if (is.list(avro_type) && length(avro_type) > 0 && is.null(names(avro_type))) {
    if (length(avro_type) == 2 && "null" %in% avro_type) {
      inner <- avro_type[[which(avro_type != "null")]]
      return(avro_to_spark_type(sc, inner, nullable = TRUE))
    }
    return(avro_to_spark_type(sc, avro_type[[1]], nullable))
  }
  # Named list with a "type" key
  if (is.list(avro_type) && !is.null(names(avro_type))) {
    kind <- avro_type[["type"]]
    if (!is.null(kind)) {
      return(avro_typed_to_spark(sc, avro_type, kind))
    }
  }
  string_type(sc)
}

# @keywords internal
avro_typed_to_spark <- function(sc, avro_type, kind) {
  switch(
    kind,
    record = {
      struct_fields <- lapply(avro_type$fields, function(f) {
        nullable_field <- is_nullable_avro(f$type)
        field_type <- avro_to_spark_type(
          sc,
          avro_unwrap_null(f$type),
          nullable = FALSE
        )
        struct_field(sc, name = f$name, data_type = field_type, nullable = nullable_field)
      })
      struct_type(sc, struct_fields = struct_fields)
    },
    array = {
      items_type <- avro_to_spark_type(sc, avro_type$items, nullable = FALSE)
      array_type(sc, items_type)
    },
    map = {
      key_type <- string_type(sc)
      values_avro <- avro_type[["values"]]
      value_type <- avro_to_spark_type(
        sc,
        if (is.null(values_avro)) "string" else values_avro,
        nullable = FALSE
      )
      map_type(sc, key_type, value_type)
    },
    avro_primitive_to_spark(sc, kind)
  )
}

# @keywords internal
avro_unwrap_null <- function(avro_type) {
  if (is.list(avro_type) && length(avro_type) == 2 && "null" %in% avro_type) {
    return(avro_type[[which(avro_type != "null")]])
  }
  avro_type
}

# @keywords internal
is_nullable_avro <- function(avro_type) {
  is.list(avro_type) && length(avro_type) == 2 && "null" %in% avro_type
}

# @keywords internal
avro_primitive_to_spark <- function(sc, avro_type) {
  switch(
    avro_type,
    string = string_type(sc),
    int = integer_type(sc),
    long = long_type(sc),
    float = float_type(sc),
    double = double_type(sc),
    boolean = boolean_type(sc),
    bytes = binary_type(sc),
    string_type(sc)
  )
}

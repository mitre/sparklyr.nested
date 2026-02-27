test_that("avro_subschema works with character fields (dot notation)", {
  schema <- list(
    type = "record",
    name = "LiteTrack",
    namespace = "org.mitre.caasd.ttfs",
    fields = list(
      list(name = "primary_key", type = list(type = "string")),
      list(name = "track_key", type = list(type = "string")),
      list(
        name = "points",
        type = list(
          type = "array",
          items = list(
            type = "record",
            name = "LitePoint",
            fields = list(
              list(name = "primary_key", type = list(type = "string")),
              list(name = "latitude", type = "double"),
              list(name = "longitude", type = "double"),
              list(name = "altitude", type = list("null", "float"))
            )
          )
        )
      )
    )
  )
  out <- avro_subschema(schema, c("primary_key", "points.latitude", "points.longitude"))
  if (is.character(out)) out <- jsonlite::fromJSON(out, simplifyVector = FALSE)
  expect_equal(length(out$fields), 2L)
  expect_equal(out$fields[[1]]$name, "primary_key")
  expect_equal(out$fields[[2]]$name, "points")
  expect_equal(
    vapply(out$fields[[2]]$type$items$fields, \(f) f$name, character(1)),
    c("latitude", "longitude")
  )
})

test_that("avro_subschema works with character fields (dollar notation)", {
  schema <- list(
    type = "record",
    name = "X",
    fields = list(
      list(name = "a", type = "string"),
      list(name = "b", type = list(
        type = "record",
        fields = list(
          list(name = "c", type = "int"),
          list(name = "d", type = "int")
        )
      ))
    )
  )
  out <- avro_subschema(schema, c("a", "b$c"))
  if (is.character(out)) out <- jsonlite::fromJSON(out, simplifyVector = FALSE)
  expect_equal(length(out$fields), 2L)
  expect_equal(out$fields[[1]]$name, "a")
  expect_equal(out$fields[[2]]$name, "b")
  expect_equal(out$fields[[2]]$type$fields[[1]]$name, "c")
})

test_that("avro_subschema works with list fields", {
  schema <- list(
    type = "record",
    name = "X",
    fields = list(
      list(name = "primary_key", type = "string"),
      list(name = "points", type = list(
        type = "array",
        items = list(
          type = "record",
          fields = list(
            list(name = "latitude", type = "double"),
            list(name = "longitude", type = "double"),
            list(name = "altitude", type = "float")
          )
        )
      ))
    )
  )
  out <- avro_subschema(schema, list("primary_key", "points" = list("latitude", "longitude", "altitude")))
  if (is.character(out)) out <- jsonlite::fromJSON(out, simplifyVector = FALSE)
  expect_equal(length(out$fields), 2L)
  expect_equal(out$fields[[1]]$name, "primary_key")
  expect_equal(
    vapply(out$fields[[2]]$type$items$fields, \(f) f$name, character(1)),
    c("latitude", "longitude", "altitude")
  )
})

test_that("avro_subschema accepts JSON string schema", {
  schema_str <- '{"type":"record","name":"X","fields":[{"name":"a","type":"string"},{"name":"b","type":"int"}]}'
  out <- avro_subschema(schema_str, c("a"))
  if (is.character(out)) out <- jsonlite::fromJSON(out, simplifyVector = FALSE)
  expect_equal(out$type, "record")
  expect_equal(length(out$fields), 1L)
  expect_equal(out$fields[[1]]$name, "a")
})

test_that("avro_subschema preserves schema metadata when keep_* is TRUE", {
  schema <- list(
    type = "record",
    name = "LiteTrack",
    namespace = "org.mitre.caasd.ttfs",
    fields = list(
      list(name = "primary_key", type = "string"),
      list(name = "track_key", type = "string")
    )
  )
  out <- avro_subschema(schema, "primary_key", keep_namespace = TRUE)
  if (is.character(out)) out <- jsonlite::fromJSON(out, simplifyVector = FALSE)
  expect_equal(out$name, "LiteTrack")
  expect_equal(out$namespace, "org.mitre.caasd.ttfs")
  expect_equal(out$type, "record")
})

test_that("avro_subschema drops doc, namespace, java-class, default when keep_* is FALSE", {
  schema <- list(
    type = "record",
    name = "X",
    namespace = "ns",
    doc = "A record",
    fields = list(
      list(name = "a", type = "string", default = "x", doc = "field a")
    )
  )
  out <- avro_subschema(schema, "a")
  if (is.character(out)) out <- jsonlite::fromJSON(out, simplifyVector = FALSE)
  expect_null(out$namespace)
  expect_null(out$doc)
  expect_null(out$fields[[1]]$default)
  expect_null(out$fields[[1]]$doc)
})

test_that("avro_subschema throws useful error for invalid fields", {
  schema <- list(
    type = "record",
    name = "X",
    fields = list(
      list(name = "a", type = "string"),
      list(name = "b", type = list(
        type = "record",
        fields = list(
          list(name = "c", type = "int"),
          list(name = "d", type = "int")
        )
      ))
    )
  )
  expect_error(
    avro_subschema(schema, c("a", "b.nonexistent")),
    "not valid in the schema",
    class = "subschema_invalid_fields"
  )
  expect_error(
    avro_subschema(schema, c("missing_field")),
    "not valid in the schema",
    class = "subschema_invalid_fields"
  )
})

test_that("avro_subschema resolves Avro type references (named type reuse)", {
  airport_runway_weather <- list(
    type = "record",
    name = "AirportRunwayWeather",
    namespace = "org.mitre.caasd.ttfs.airportweather.models",
    fields = list(
      list(name = "primary_key", type = "string"),
      list(name = "time", type = "long"),
      list(name = "next_weather_key", type = "string")
    )
  )
  schema <- list(
    type = "record",
    name = "FusedAirportWeather",
    namespace = "org.mitre.caasd.ttfs",
    fields = list(
      list(name = "primary_key", type = "string"),
      list(
        name = "weather_minus_30_min",
        type = list(
          "null",
          airport_runway_weather
        )
      ),
      list(
        name = "weather_zero",
        type = "org.mitre.caasd.ttfs.airportweather.models.AirportRunwayWeather"
      )
    )
  )
  out <- avro_subschema(schema, c("primary_key", "weather_zero.time", "weather_zero.next_weather_key"))
  if (is.character(out)) out <- jsonlite::fromJSON(out, simplifyVector = FALSE)
  expect_equal(length(out$fields), 2L)
  expect_equal(out$fields[[1]]$name, "primary_key")
  expect_equal(out$fields[[2]]$name, "weather_zero")
  expect_true(is.list(out$fields[[2]]$type))
  expect_equal(out$fields[[2]]$type$type, "record")
  expect_equal(
    vapply(out$fields[[2]]$type$fields, \(f) f$name, character(1)),
    c("time", "next_weather_key")
  )
})

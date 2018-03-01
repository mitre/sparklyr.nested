context("testing schema parsing")

test_that("fields are read properly", {
  sample_json <- paste0(
    '{"type":"struct","fields": [{"name" : "field1", "type" : {"type" : "array", "elementType" : "string", "containsNull" : true}, ',
    '"nullable" : true, "metadata" : { } }, {"name": "field2", "type" : "double"}]}')
  schema <- fromJSON(sample_json, simplifyVector=FALSE)
  
  inspection_view <- simplify_schema(schema, append_complex_type=TRUE)
  
  expect_equal(names(inspection_view), c("field1 (array)", "field2"))
  expect_equal(inspection_view[[1]], "[string]")
  expect_equal(inspection_view[[2]], "double")
})
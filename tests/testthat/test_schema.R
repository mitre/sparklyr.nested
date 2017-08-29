test_that("schema can be parsed", {
  require(sparklyr)
  sample_json <- '[{"fields":"a","types":"string"},{"fields":"b","types":"double"},{"fields":"c","types":"long"}]'
  with_mock(
    spark_dataframe = function(x){return(x)},
    invoke = function(x, ...){return(sample_json)},
    res <- sdf_schema_json(1),
    expect_equal(names(res[[1]]), c("fields", "types"))
  )
})
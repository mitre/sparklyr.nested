
[![Build Status](https://travis-ci.org/mitre/sparklyr.nested.svg?branch=master)](https://travis-ci.org/mitre/sparklyr.nested)



A package to extend the capabilities available in the open source `sparklyr` package with support for working with nested data.

## Installation & Documentation

To install:

```r
devtools::install_github("mitre/sparklyr.nested")
```

Note that per the `sparklyr` installation instructions, you will need to install Spark if you have not already done so or are not using a cluster where it is already installed.

Full documentation is available here: https://mitre.github.io/sparklyr.nested/

## Nested Operations

The `sparklyr` package makes working with Spark in R easy.
The goal of this package is to extend `sparklyr` so that working with nested data is easy.
The flagship functions are `sdf_select`, `sdf_explode`, `sdf_unnest`, and `sfd_schema_viewer`.

### Schema Viewer

Suppose I have data about aircraft phase of flight (e.g., climb, cruise, descent).
The data is somewhat complex, storing radar data points marked as the start and end points of a given phase.
Furthermore, the data is structured such that for a given flight, there are several phases (disjoint in time) in a nested array.

This is a data set that is not very natural for more R use cases (though the `tidyr` package helps close this gap) but is fairly typical for Hadoop storage (e.g., using Avro or Parquet).
The schema viewer (coupled with a json schema getter `sdf_schema_json`) makes understanding the structure of the data simple.

Suppose that `spark_data` is a Spark data frame.
The following is mostly a bunch of code to simulate a `sparklyr` environment where no actual spark connection exists, but the key bit at the end shows how the `listviewer` package with some schema parsing yields an effective view of how the data is structured.



```r
library(testthat)
library(jsonlite)
library(sparklyr)
library(sparklyr.nested)

with_mock(
  # I am mocking functions so that the example works without a real spark connection
  spark_read_parquet = function(x, ...){return("this is a spark dataframe")},
  sdf_schema_json = function(x, ...){return(fromJSON(sample_json))},
  spark_connect = function(...){return("this is a spark connection")},

  
  # -- interesting part starts here -------------------
  
  sc <- spark_connect(),  
  spark_data <- spark_read_parquet(sc, path="path/to/data/*.parquet", name="some_name"),
  sdf_schema_viewer(spark_data)
)
```

<!--html_preserve--><div id="htmlwidget-6673" style="width:672px;height:480px;" class="jsonedit html-widget"></div>
<script type="application/json" data-for="htmlwidget-6673">{"x":{"data":{"aircraft_id":"string","phase_sequence":"string","phases (array)":{"start_point (struct)":{"segment_phase":"string","agl":"double","elevation":"double","time":"long","latitude":"double","longitude":"double","altitude":"double","course":"double","speed":"double","source_point_keys (array)":"[string]","primary_key":"string"},"end_point (struct)":{"segment_phase":"string","agl":"double","elevation":"double","time":"long","latitude":"double","longitude":"double","altitude":"double","course":"double","speed":"double","source_point_keys (array)":"[string]","primary_key":"string"},"phase":"string","primary_key":"string"},"primary_key":"string"},"options":{"mode":"tree","modes":["code","form","text","tree","view"]}},"evals":[],"jsHooks":[]}</script><!--/html_preserve-->

It is also handy to use the schema viewer to quickly inspect what a pipeline of operations is going to return.
This enables you to anticipate the structure of the output you are going to get (and make sure the operations are valid with respect to schema modification) without doing any actual computation on your data.
For example:

```r
spark_data %>%
  sdf_unnest(phases) %>%
  select(aircraft_id, start_point) %>%
  sdf_schema_viewer()
```

### Nested Select

The `sdf_select` function makes it possible to select nested elements.
For example, given the schema displayed above, we may be interested in only the phase start point; and there only the time and altitude fields.
Grabbing these elements is not possible with a simple `select`, but can be done via:

```r
spark_data %>%
  sdf_select(aircraft_id, time=phases.start_point.time, altitude=phases.start_point.altitude)
```

In java dots are not valid characters in a field name so the dot-operator is handled.
However, since the dollar sign is typically used for this purpose in R, that is supported as well.
The following will trigger the same operation as the above:

```r
spark_data %>%
  sdf_select(aircraft_id, time=phases$start_point$time, altitude=phases$start_point$altitude)
```

### Explode

The example above would work, but would return something of a mess.
The `time` and `altitude` fields would contain vectors of time and altitude values (respectively) for each row-wise element.
Thus for a single `aircraft_id` you would have N values in the corresponding `time` and `altitude` fields.
The explode functionality will flatten the data in the sense that it will replicate the top level record once for each element in a nested array along which you are exploding.
It will *not* change the schema beyond transforming arrays to structures.

The above example could be repeated like so to get something more typical for R where there is a single value per field per "row" (record).
In this case the `aircraft_id` values would be replicated N times instead of having vectors in the `time`/`altitude` fields.

```r
spark_data %>%
  sdf_explode(phases) %>%
  sdf_select(aircraft_id, time=phases$start_point$time, altitude=phases$start_point$altitude)
```

### Unnest

Unnesting here works essentially the same way as in the `tidyr` package.
It is necessary here because `tidyr` functions cannot be called directly on spark data frames.
An unnest operation is a combination of an explode with a corresponding nested select to promote the (exploded) nested fields up one level of the data schema.

Thus the following operations are equivalent:

```r
spark_data %>%
  sdf_unnest(phases)
```

and

```r
spark_data %>%
  sdf_explode(phases) %>%
  sdf_select(aircraft_id, terrain_fusion_key, vertical_taxonomy_key, sequence, 
             start_point=phases.start_point, start_point=phases.start_point, phase=phases.phase,
             takeoff_mode=phases.takeoff_model, landing_model=phases.landing_model, 
             phases_primary_key=phases.primary_key, primary_key)
```

There are a few things to notice about how `sdf_unnest` does things:

- It will only dig one level deep into the schema. If there are fields nested 2 levels deep, they will still be nested (albeit only down 1 level) after `sdf_unnest` is executed. Therefore you are *not* guaranteed totally flat data after calling `sdf_unnest`.

- It will promote *every* one-level-deep field, even if you only care about a few of them.

- In the event of a name conflict (e.g., there is a top level `primary_key` and a `primary_key` nested inside the `phases` field) then the nested field will be renamed with a prepended value. By default this takes the name of the field in which it was nested (as above) but is settable.

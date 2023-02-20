# sparklyr.nested 0.0.4

- Replaced now-defunct `dplyr::select_vars` calls with `tidyselect::vars_select`
- Support (optinally) using `listviewer::reactjson` to view schemas

# sparklyr.nested 0.0.3

- Initial release
- To support read schemas, `array_type`, `binary_type`, `boolean_type`, `byte_type`, `character_type`, `date_type`, `double_type`, `float_type`, `integer_type`, `long_type`, `map_type`, `numeric_type`, `string_type`, `struct_field`, `struct_type`, and `timestamp_type` are provided. These will return java references neede for schema definition.
- `sdf_select` enables access to nested fields (instide maps, structs, and arrays of structs).
- `sdf_explode` will convert arrays of structs to simple structs by duplicating rows over each unique array entry. Also works on map types if `is_map=TRUE`.
- `sdf_nest` and `sdf_unnest` are similar to the similarly named `tidyr` functions. `sdf_unnest` essentially chains `sdf_explode` and `sdf_select` with some schema inspection.
- `sdf_schema_json` and `sdf_schema_viewer` support schema interrogation

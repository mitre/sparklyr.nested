## Test environments
* local OS X install, R 3.5.1
* Ubuntu 14.04.5 LTS (on travis-ci), R 3.5.1
* win-builder (devel and release)

## R CMD check results
0 errors | 0 warnings | 0 notes

## Downstream dependencies
None, first time submission

## Additional notes
* All examples are wrapped in \dontrun{} due to their dependence on an Apache Spark installation. These functions call java methods implemented in Spark via the sparklyr `invoke` methods. To run tests on CRAN machines would require installing a version of spark and spooling up a spark session - which is far out of bounds for the 5s runtime I was advised to respect.
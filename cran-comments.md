## Test environments

* local OS X install, R 4.1.1
* Ubuntu 22.04.1 LTS (using the `rocker/binder:4.2.2` docker image), R 4.2.2

## R CMD check results

0 errors | 0 warnings | 0 notes

## Downstream dependencies (revdepcheck results)

We checked 1 reverse dependencies (0 from CRAN + 1 from Bioconductor), comparing R CMD check results across CRAN and dev versions of this package.

 * We saw 0 new problems
 * We failed to check 0 packages

## Additional notes

* All examples are wrapped in \dontrun{} due to their dependence on an Apache Spark installation. These functions call java methods implemented in Spark via the sparklyr `invoke` methods. To run tests on CRAN machines would require installing a version of spark and spooling up a spark session - which is far out of bounds for the 5s runtime I was advised to respect.
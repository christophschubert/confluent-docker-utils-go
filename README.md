# confluent-docker-utils-go

A re-implementation of the confluent-docker-utils in go.

## Differences with the original python based implementation

* instead of separate `cub` and `dub` commands, we provide only one `ub` binary
* instead of the `template` subcommand, we offer three methods
  
  1. `render-properties`
  1. `render-properties-prefix`
  1. `render-template` 
   
These commands write to stdout instead into a file, making it easier to combine their output into a single config file.

### Mapping environment variables to property keys

### Difference in templating

### Custom functions available in go templates
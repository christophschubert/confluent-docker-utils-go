# confluent-docker-utils-go

A re-implementation of the confluent-docker-utils in go.

## Differences with the original python based implementation

* instead of separate `cub` and `dub` commands, we provide only one `ub` binary
* instead of the `template` subcommand, we offer three methods
  
  1. ``
  1. 11
  1. `render-template`
    
These commands write to stdout instead into a file, making it easier to combine their output into a single config file.

### Difference in templating
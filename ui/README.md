# Hamilton UI

This contains the code for the new Hamilton UI. Read the documentation for getting started
[here](https://hamilton.dagworks.io/concepts/ui). 

## Overview

The Hamilton UI is an application that does the following:

1. Tracks Hamilton executions through an SDK using the [lifecycle adapter framework](https://hamilton.dagworks.io/en/latest/reference/lifecycle-hooks/) 
2. Provides telemetry/monitoring of Hamilton executions + specific function results/code through a web interface
3. Provides a persistent database to store/manage these
4. Provides a server that allows reading/writing/authentication

It is meant to be used both for debugging purposes and for monitoring Hamilton executions in production.
It works with the vast majority of Hamilton features.

## Docs/usage

See documentation [here](https://hamilton.dagworks.io/concepts/ui). 

## Architecture

The architecture is simple.

![architecture-diagram](./hamilton-ui-architecture.png)

The tracking server stores data on postgres, as well as any blobs on s3. This is stored in a docker volume
on local mode. The frontend is a simple React application. There are a few authentication/ACL capabilities, 
but the default is to use local/unauthenticated (open). Please talk to us if you have a need for more custom authentication.


## License

There are a few directories that are not licensed under the BSD-3 Clear Clause license. These are:
* frontend/src/ee
* backend/server/trackingserver_auth

See the main repository [LICENSE](../LICENSE) for details, else the LICENSE file in the respective directories
mentioned above.

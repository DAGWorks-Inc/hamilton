# Hamilton UI

This contains the code for the new Hamilton UI. For an overview of getting started & featuresr
[see this documentation](https://hamilton.dagworks.io/en/latest/concepts/ui). For a lengthier post and intro see our [blog post](https://blog.dagworks.io/p/hamilton-ui-streamlining-metadata).

## One operational UI for all your dataflows

The Hamilton UI is a system that does the following:

1. Tracks Hamilton Executions & surrounding metadata through an SDK using the [lifecycle adapter framework](https://hamilton.dagworks.io/en/latest/reference/lifecycle-hooks/)
   * Provides a persistent database to store/manage these
   * Provides a server that allows reading/writing/authentication
2. Observability: provides telemetry/observability of Hamilton executions + specific function results/code through a web interface
3. Lineage & Provenance: allows you to quickly inspect how code and data is connected.
4. Catalog: everything is observed and cataloged, so you can quickly search and find what exists and when it was run.

The UI is meant to be used both for debugging purposes and for monitoring Hamilton executions in production.
It works with the vast majority of Hamilton features.


## Getting started
Make sure you have docker running:
```bash
# clone the repository if you haven't
git clone https://github.com/dagworks-inc/hamilton
# change into the UI directory
cd hamilton/ui
# run docker
./deployment/run.sh
```
Once docker is running navigate to http://localhost:8242 and create an email and a project; then follow
instructions on integrating with Hamilton.

A fuller guide can be found [here](https://hamilton.dagworks.io/en/latest/concepts/ui).

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

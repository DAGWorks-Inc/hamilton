# Hamilton UI

This contains the code for the new Hamilton UI. For an overview of getting started & features
[see this documentation](https://hamilton.dagworks.io/en/latest/concepts/ui). For a lengthier post and intro see our [blog post](https://blog.dagworks.io/p/hamilton-ui-streamlining-metadata).

## One operational UI for all your dataflows

The Hamilton UI is a system that provides the following capabilities:

1. Execution tracking with associated metadata
   * Provides a persistent database to store/manage these
   * Provides a server that allows reading/writing/authentication
2. Data/artifact observability: provides telemetry/observability of Hamilton executions + specific function results/code through a web interface
3. Lineage & provenance: allows you to quickly inspect how code and data is connected.
4. Catalog: everything is observed and cataloged, so you can quickly search and find what exists and when it was run.

The UI is meant to monitor/debug Hamilton dataflows both in **development** and **production**. The aim is to enable
dataflow authors to move faster during all phases of the software development lifecycle.

### Execution Tracking

<p align="center">
  <img src="./screenshots/execution_waterfall_view.png" alt="Description1" width="30%" style="margin-right: 20px;"/>
  <img src="./screenshots/execution_graph_error.png" alt="Description2" width="30%" style="margin-right: 20px;"/>
  <img src="./screenshots/execution_comparison_waterfall.png" alt="Description3" width="30%"/>
</p>
<p align="center">
  <em>See what's slow (left), pinpoint errors (middle) compare execution performance (right)</em>
</p>

### Data/Artifact Observability

<p align="center">
  <img src="./screenshots/execution_data_view.png" alt="Description3" width="30%"/>
  <img src="./screenshots/execution_code_view.png" alt="Description2" width="30%" style="margin-right: 20px;"/>
  <img src="./screenshots/execution_data_comparison.png" alt="Description1" width="30%" style="margin-right: 20px;"/>
</p>
<p align="center">
  <em>Visualize data for a run (left), track code the run used (middle) compare data across executions (right)</em>
</p>

### Lineage & Provenance

<p align="center">
  <img src="./screenshots/lineage_view.png" alt="Description3" width="30%"/>
  <img src="./screenshots/lineage_code_view_grouped_by_module.png" alt="Description2" width="30%" style="margin-right: 20px;"/>
</p>
<p align="center">
  <em>See how things connect: what's upstream/downstream (left), walk through code visually (right) </em>
</p>

### Catalog

<p align="center">
  <img src="./screenshots/catalog_artifact.png" alt="Description3" width="30%"/>
  <img src="./screenshots/catalog_transform.png" alt="Description2" width="30%" style="margin-right: 20px;"/>
</p>
<p align="center">
  <em>Understand artifacts produced (left), find features and when they were used (right) </em>
</p>

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

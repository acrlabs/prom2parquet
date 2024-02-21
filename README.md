# prom2parquet

Remote write target for Prometheus that saves metrics to parquet files

**This should be considered an alpha project**

## Overview

The prom2parquet remote write endpoint for Prometheus listens for incoming datapoints from Prometheus and saves them to
[parquet files](https://parquet.apache.org) in a user-configurable location.  This can either (currently) be pod-local
storage or to an AWS S3 bucket.  Metrics are saved in the following directory structure:

```
/data/<prefix>/<metric name>/2024022021.parquet
```

Each file for a particular metric will have the same schema, but different metrics may have different schemas.  At a
minimum, each file has a `timestamp` and a `value` column, and a variety of other extracted columns corresponding to the
labels on the Prometheus timeseries.  They also have a "catch-all" `labels` column to contain other unextracted columns.

## Usage

```
Usage:
  prom2parquet [flags]

Flags:
      --clean-local-storage   delete pod-local parquet files upon flush
  -h, --help                  help for prom2parquet
      --prefix string         directory prefix for saving parquet files
      --remote remote         supported remote endpoints for saving parquet files
                              (valid options: none, s3/aws) (default none)
  -p, --server-port int       port for the remote write endpoint to listen on (default 1234)
  -v, --verbosity verbosity   log level (valid options: debug, error, fatal, info, panic, trace, warning/warn)
                              (default info)
```

Here is a brief overview of the options:

### clean-local-storage

To reduce pod-local storage, you can configure prom2parquet to remove all parquet files after they've been written
(currently once per hour).  This is generally not very useful unless you've also configured a remote storage option.

### prefix

This option provides a prefix that can be used to differentiate between metrics collections.

### remote

Whether to save the parquet files to some remote storage; currently the only supported remote storage option is AWS S3.

### server-port

What port prom2parquet should listen on for timeseries data from Prometheus.

## Configuring Prometheus

Prometheus needs to know where to send timeseries data.  You can include this block in your Prometheus's `config.yml`:

```yaml
remote_write:
- url: http://prom2parquet-svc.monitoring:1234/receive
  remote_timeout: 30s
```

Alternately, if you're using the [Prometheus operator](https://prometheus-operator.dev), you can add this configuration
to your Prometheus custom resource:

```yaml
spec:
  remoteWrite:
    - url: http://prom2parquet-svc.monitoring:1234/receive
```

## Contributing

We welcome any and all contributions to prom2parquet project!  Please open a pull request.

### Development

To set up your development environment, run `git submodule init && git submodule update` and `make setup`.  To build
`prom2parquet`, run `make build`.

This project uses [🔥Config](https://github.com/acrlabs/fireconfig) to generate Kubernetes manifests from definitions
located in `./k8s/`.  If you want to use this mechanism for deploying prom2parquet, you can just type `make` to build
the executable, create and push the Docker images, and deploy to the configured Kubernetes cluster.

All build artifacts are placed in the `.build/` subdirectory.  You can remove this directory or run `make clean` to
clean up.

### Code of Conduct

Applied Computing Research Labs has a strict code of conduct we expect all contributors to adhere to.  Please read the
[full text](https://github.com/acrlabs/simkube/blob/master/CODE_OF_CONDUCT.md) so that you understand the expectations
upon you as a contributor.

### Copyright and Licensing

SimKube is licensed under the [MIT License](https://github.com/acrlabs/simkube/blob/master/LICENSE).  Contributors to
this project agree that they own the copyrights to all contributed material, and agree to license your contributions
under the same terms.  This is "inbound=outbound", and is the [GitHub
default](https://docs.github.com/en/site-policy/github-terms/github-terms-of-service#6-contributions-under-repository-license).

> [!WARNING]
> Due to the uncertain nature of copyright and IP law, this repository does not accept contributions that have been all
> or partially generated with GitHub Copilot or other LLM-based code generation tools.  Please disable any such tools
> before authoring changes to this project.

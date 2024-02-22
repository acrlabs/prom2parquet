![build status](https://github.com/acrlabs/prom2parquet/actions/workflows/verify.yml/badge.svg)

# prom2parquet

Remote write target for Prometheus that saves metrics to parquet files

**⚠️ This should be considered an alpha project. ⚠️**
In particular, the schema for the saved Parquet files is likely to change in the future.

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
      --backend backend       supported remote backends for saving parquet files
                              (valid options: none, s3/aws) (default local)
      --backend-root string   root path/location for the specified backend (e.g. bucket name for AWS S3)
                              (default "/data")
  -h, --help                  help for prom2parquet
      --prefix string         directory prefix for saving parquet files
  -p, --server-port int       port for the remote write endpoint to listen on (default 1234)
  -v, --verbosity verbosity   log level (valid options: debug, error, fatal, info, panic, trace, warning/warn)
                              (default info)
```

Here is a brief overview of the options:

### backend

Where to store the Parquet files;; currently supports pod-local storage and AWS S3.

### backend-root

"Root" location for the backend storage.  For pod-local storage this is the base directory, for AWS S3 this is the
bucket name.

### prefix

This option provides a prefix that can be used to differentiate between metrics collections.

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

### Testing

Run `make test` to run all the unit/integration tests.  If you want to test using pod-local storage, and you want to
flush the Parquet files to disk without terminating the pod (e.g., so you can copy them elsewhere), you can send the
process a SIGUSR1:

```
> kubectl exec prom2parquet-pod -- kill -s SIGUSR1 <pid>
> kubectl cp prom2parquet-pod:/path/to/files ./
```

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

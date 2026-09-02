# Installing the CLI


=== "go install"

    The simplest path if you have Go, and the only one that needs no release
    asset at all — the module is published on the Go proxy:

    ```bash
    go install github.com/priyolahiri/neo4j-kubernetes-operator/cmd/kubectl-neo4j@v1.15.0
    ```

    It installs into `$(go env GOPATH)/bin` under the name `kubectl-neo4j`,
    which is exactly what `kubectl` needs for plugin discovery. Pin the version
    to the operator release you deploy — `@latest` will drift.

=== "Install script"

    Detects your OS and architecture, downloads the matching archive, and
    **verifies its checksum before installing** — it aborts rather than
    installing something it could not verify:

    ```bash
    curl -sSL https://raw.githubusercontent.com/priyolahiri/neo4j-kubernetes-operator/main/hack/install-cli.sh | sh
    ```

    Pipe-to-shell means trusting the network round trip. Downloading first and
    reading it is the better habit:

    ```bash
    curl -sSLO https://raw.githubusercontent.com/priyolahiri/neo4j-kubernetes-operator/main/hack/install-cli.sh
    less install-cli.sh
    sh install-cli.sh
    ```

    Two knobs: `VERSION` (default: latest release) and `INSTALL_DIR`
    (default: `/usr/local/bin`). Windows is not covered — use the `.zip`
    asset.

=== "Download a release"

    Binaries are attached to every [release](https://github.com/priyolahiri/neo4j-kubernetes-operator/releases). Pick your platform:

    ```bash
    VERSION=1.15.0     # the release you want
    OS=darwin          # darwin | linux | windows
    ARCH=arm64         # arm64 | amd64

    curl -sSLO "https://github.com/priyolahiri/neo4j-kubernetes-operator/releases/download/v${VERSION}/kubectl-neo4j_${VERSION}_${OS}_${ARCH}.tar.gz"
    curl -sSLO "https://github.com/priyolahiri/neo4j-kubernetes-operator/releases/download/v${VERSION}/kubectl-neo4j_${VERSION}_checksums.txt"
    shasum -a 256 -c --ignore-missing "kubectl-neo4j_${VERSION}_checksums.txt"

    tar -xzf "kubectl-neo4j_${VERSION}_${OS}_${ARCH}.tar.gz"
    chmod +x kubectl-neo4j
    sudo mv kubectl-neo4j /usr/local/bin/
    ```

    Windows builds are published as `.zip` rather than `.tar.gz`.

=== "Build from source"

    ```bash
    git clone https://github.com/priyolahiri/neo4j-kubernetes-operator
    cd neo4j-kubernetes-operator
    make build-cli          # writes bin/kubectl-neo4j
    export PATH="$PWD/bin:$PATH"
    ```

Anything named `kubectl-*` on your `PATH` becomes a `kubectl` subcommand, so once installed:

```bash
kubectl neo4j version
kubectl plugin list        # should list kubectl-neo4j
```

It also runs standalone as `kubectl-neo4j` if you prefer.

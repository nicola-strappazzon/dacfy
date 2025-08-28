# dacfy | Data as Code for ClickHouse

[![Test](https://github.com/nicola-strappazzon/dacfy/actions/workflows/test.yaml/badge.svg?branch=main)](https://github.com/nicola-strappazzon/dacfy/actions/workflows/test.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/nicola-strappazzon/dacfy)](https://goreportcard.com/report/github.com/nicola-strappazzon/dacfy)

A simple way to use pipelines for data transformation: define your databases, tables, materialized views, and **populate** or **backfill** them, all in a single step using a YAML file. Then, deploy everything from the terminal and rollback just as easily, without effort or added complexity. Of course, with this tool you can use it to versioning code, integrate it into a CI/CD, and maybe more.

To understand exactly what this tool does, I’ve included a series of curated [examples](https://github.com/nicola-strappazzon/cht/tree/main/examples) that have been reorganized to help you grasp how it works much more clearly.

## Install on macOS

Using [Homebrew](https://brew.sh/):

```bash
brew install nicola-strappazzon/tap/dacfy
```

## Install using go

If you have Go installed, you can install the dacfy binary like this:

```bash
go install github.com/nicola-strappazzon/dacfy@latest
```

The binary will be placed in your `GOBIN` directory, which defaults to `~/go/bin`. Depending on how Go is installed, this directory may or may not be in your `PATH`.

## Install on Linux - DEB

This method can be used on Debian, Ubuntu, and related distributions. The example below is for Intel/AMD systems; for an ARM architecture, replace "amd64" with "arm64" in both lines.

```bash
curl -LO https://github.com/nicola-strappazzon/dacfy/releases/latest/download/dacfy_amd64.deb
sudo apt install ./dacfy_amd64.deb
```

## Install on Linux - RPM

This method can be used on RHEL, CentOS, Fedora, Amazon Linux, SUSE, Mandriva, Oracle Linux, etc. The example below is for Intel/AMD systems; for an ARM architecture, replace “amd64” with “arm64” in both lines.

```bash
curl -LO https://github.com/nicola-strappazzon/dacfy/releases/latest/download/dacfy_amd64.rpm
sudo rpm -Uvh dacfy_amd64.rpm
```

## Install on Windows

A Windows build is currently unavailable, but the Linux build works fine under WSL.

> [!WARNING]
> This project is under active development and may be unstable. Use at your own risk.

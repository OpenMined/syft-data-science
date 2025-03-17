# RDS

## Requirements

- [just](https://github.com/casey/just?tab=readme-ov-file#installation)
- [uv](https://docs.astral.sh/uv/getting-started/installation/)

## Quick Install

<div align="left">
  <a href="https://syftbox.openmined.org/datasites/liamtrask@gmail.com/app_installer.html?redirect=https://github.com/OpenMined/rds&source=https://github.com/OpenMined/rds" target="_blank">
    <img src="https://img.shields.io/badge/Install%20with-SyftBox-blue?style=for-the-badge&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAACXBIWXMAAAsTAAALEwEAmpwYAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAADTSURBVHgBrVLBDYMwDLRDJ2AG2IAyQjdgg3YDGIFu0BHoBnQDmKAjdATcWbEihCSA+tKT4sTn2I4TGWOUc+4GYIcrbn7JWnvHCJSUshJC3LTPWkKrbYR6EKkC0zQZKSVexRxEogTv/ehDSBcW0ogSQA9t/hsG/raIHq+QyS4JyNNPWFGCS4zTOI5L5mgljPNWQs4TvMbYaCHPQopfbSUcPaEUEmvCvBfH0FTlzBqpUxoopTK4LRby6kuMW6WU6vcR+qJnrbUPz/Md3PuMvlCN0WcTZiS8AZcXxhY8Y0v/AAAAAElFTkSuQmCC" alt="Install with SyftBox" width="220">
  </a>
</div>

## Installation

To install the syft-rds package in a new virtual env:

```
just install
```

## Running the demo

### In-memory full flow example

The notebook `notebooks/quickstart/full_flow.ipynb` contains a full example of how to use the syft-rds package from both the data scientist and the data owner perspective. This demo mocks most of the SyftBox functionality, and does not require running any services in the background.

### Full example with SyftBox

To run the RDS app on top of syftbox, we need to:

1. launch a syftbox server
2. launch syftbox clients for the data scientist and the data owner
3. run an RDS app on the data owner's datasite
4. (optional) run an RDS app on the data scientist's datasite
5. run the `notebooks/quickstart/do_flow.ipynb` and `notebooks/quickstart/ds_flow.ipynb` notebooks

Steps 1 through 4 can be done with a single just command:

```
just run-rds-stack
```

To run jupyter lab in the syft-rds environment:

```
just run-jupyter
```

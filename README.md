# Pre-Trained Model (PTM) Reuse Through Academic Transactions

> Prototyping if it is possible to identify where PTMs are used based on
> academic citations

## About

This project was a four-week effort by undergraduate members of SSL who were
supported by the
[Undergraduate Research and Engagement Symposium](https://ecommons.luc.edu/ures/)
at Loyola University Chicago.

The goal of this project was to prototype a utility that would be able to:

- Load, search, and parse SQL queries made to both the OpenAlex and PeaTMOSS
  datasets,
- Measure usage of PTMs captured in PeaTMOSS within the OpenAlex dataset,
- Introduce students to graph databases through Neo4J,
- and create a small data app using Streamlit.

To that end, the students involved were able to successfully acomplish these
objectives.

## How to Install

This code has been tested on x86-64 Linux and Mac OS computers.

### Dependencies

- Python 3.10

### Installation steps

1. `git clone https://github.com/NicholasSynovic/research_ptm-reuse-through-academic-transactions prtat`
1. `cd prtat`
1. `make create-dev`
1. `source env/bin/activate`
1. `make build`

## How to Run

### Downloading The Data

See [\`data/README.md](data/README.md) for more information.

### Script Execution Order

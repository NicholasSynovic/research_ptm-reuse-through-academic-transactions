# Data Information

## Table of Contents

- [Data Information](#data-information)
  - [Table of Contents](#table-of-contents)
  - [About](#about)
    - [Figures](#figures)
    - [JSON](#json)
    - [Abstracts](#abstracts)
    - [Pickle](#pickle)
    - [PeaTMOSS](#peatmoss)
    - [OpenAlex](#openalex)

## About

We have provided the resulting figures, JSON data, abstracts, and pickle files
necessary to replicate our work within this directory.

We have not included the OpenAlex sample that we used for this project, nor the
PeaTMOSS database that we leveraged as well.

### Figures

Figures are stored within the [`figs/`](figs/) directory. All figures were
created using Seaborn and Matplotlib and exported as `.png` files.

These figures can be replicated using the
[`src/stats/plot.py`](../src/stats/plot.py) script.

### JSON

The [`json/`](json/) directory contains two types of JSON files:

1. Files that report a random sample of 10 academic works that cite a model
1. The subject classifications of these papers

Academic citation follow the convention `MODEL_NAME.json`. These files can be
replicated using the [`src/stats/stats.py`](../src/stats/stats.py) script.

The AI classifications of papers that cite the aforementioned models can be
found in the `ai_nature_classes.json` file.

### Abstracts

The abstracts used to generate the AI classifications are stored in the
`abstracts` directory. Each file is a text file where each line is a paper
abstract.

### Pickle

To speed up the amount of time required to identify the OpenAlex works that cite
arXiv published PeaTMOSS model papers, we have exported a `pandas.DataFrame`
`pickle` object that can be used without having to execute the time consuming
SQL query.

### PeaTMOSS

Please store the `PeaTMOSS.db` file in the [`db`/](db/) directory.

Instructions for how to get this file are availible on the
[PeaTMOSS GitHub repository](https://github.com/PurdueDualityLab/PeaTMOSS-Artifact?tab=readme-ov-file#globus).

### OpenAlex

This project utilized `part_000.gz` from March 27, 2024.

You can download this data by following
[OpenAlex's instruction](https://docs.openalex.org/download-all-data/download-to-your-machine).

To convert to the JSON lines document to a SQLite3 database, use
[this tool](https://github.com/NicholasSynovic/tool_academic-graph).

Store the converted database in the [`db`/](db/) directory.

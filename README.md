# MTEV

## Project Presentation

The project is about:
Care Pathway for Patients Hospitalized with Venous Thromboembolism. The aim of this study is to illustrate the distribution of VTE patients throughout the hospital and map their care pathway from Emergency Department (ED) to hospital discharge.  

## How to run the code

- Clone the repository:

```shell
git clone https://github.com/aphp-datascience/study-vte.git
```

- Create a virtual environment with the suitable Python version:

```shell
cd study-vte
python -m venv .venv
source .venv/bin/activate
```

- Install [Poetry](https://python-poetry.org/) (a tool for dependency management and packaging in Python) with the following command line:
    - Linux, macOS, Windows (WSL):

    ```shell
    curl -sSL https://install.python-poetry.org | python3 -
    ```

    - Windows (Powershell):

    ```shell
    (Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -
    ```

    For more details, check the [installation guide](https://python-poetry.org/docs/#installation)

- Install dependencies:

```shell
poetry install
```

## How to run the code on AP-HP's data platform

### 1. Install EDS-Toolbox:

EDS-Toolbox is python library that provides an efficient way of submitting PySpark scripts on AP-HP's data platform. As it is AP-HP specific, it is not available on PyPI:

```shell
pip install edstoolbox==0.6.1
```
### 2. Compute and save models and data:

:warning: Depending on your resources, this step can take some times.

```shell
cd scripts/parcours_patients
eds-toolbox spark submit --config ../../conf/parcours_patients/<config_name>.cfg --log-path logs process_data.py
```

### 3. Generate figures:

- **Option 1**: Generate all figures in a raw from the terminal:

```shell
cd scripts/parcours_patients
python generate_figures.py --config-name <config_name>
```

- **Option 2**: Generate figure one at a time from a notebook:

    - Create a Spark-enabled kernel with your environnement:

    ```shell
    eds-toolbox kernel --spark --hdfs
    ```

    - Convert markdowns into jupyter notebooks:

      ```shell
      cd notebooks/parcours_patients
      jupytext --set-formats md,ipynb 'report.md'

    - Open *report.ipynb* and start the kernel you've just created.
     - Run the cells to obtain the visualisations.

## How the template is structured

- `data`: For intermediate data.
- `figures`: For your figures.
- `notebooks`: Notebooks that process data and generate figures.
- `cse_180031`: For the source code.
- `scripts`: Typer applications to process data and generate figures.
- `conf`: Configurations used in the scripts applications.

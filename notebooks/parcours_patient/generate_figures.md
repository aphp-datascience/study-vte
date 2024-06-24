---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.15.0
  kernelspec:
    display_name: mtev
    language: python
    name: mtev
---

```python
%reload_ext autoreload
%autoreload 2
%config Completer.use_jedi = False
%reload_ext lab_black
```

```python
import pandas as pd
import plotly.io as pio

from cse_180031 import BASE_DIR
from cse_180031.parcours_patient import (
    compute_stats_from_sankey_emergency,
    compute_stats_from_sankey_emergency_supplementary,
    compute_stats_from_sankey_all,
    compute_stats_per_cat_from_sankey_emergency,
    viz_histogram,
    viz_histogram_first,
    viz_sankey_plot,
)
```

# Visualisation de la répartion des patients atteints de maladie thrombo-embolique dans les différents services à l'APHP

```python
config_name = "study_v1_DP_DR_DAS"
```

## I. Stats table

```python
sankey_data = pd.read_pickle(
    BASE_DIR / "data" / "parcours_patient" / config_name / "sankey_data.pkl"
)
```

```python
table = compute_stats_per_cat_from_sankey_emergency(sankey_data)

s = table.style

# Draw lines between level rows
for idx, group_df in table.groupby(level=0):
    s.set_table_styles(
        {group_df.index[0]: [{"selector": "", "props": "border-top: 1px solid grey;"}]},
        overwrite=False,
        axis=1,
    )


# Draw line after EMERGENCY
def css_border(x):
    return [
        "border-left: 2px solid black" if (i == 1) else "border: 0px"
        for i, col in enumerate(x)
    ]


table = s.apply(css_border, axis=1)
table.to_html(
    BASE_DIR
    / "figures"
    / "parcours_patients"
    / config_name
    / "stats_table_emergency_per_cat_2.html"
)
display(table)
```

```python
compute_stats_from_sankey_emergency(sankey_data)
```

```python
compute_stats_from_sankey_emergency_supplementary(sankey_data)
```

## II. Sankey

A partir des données UFR, nous pouvons générer les diagrammes de Sankey suivants:

```python
sankey_data = pd.read_pickle(
    BASE_DIR / "data" / "parcours_patient" / config_name / "sankey_data.pkl"
)
```

```python
fig = viz_sankey_plot(
    sankey_data=sankey_data,
    diagnosis="VTE",  # VTE
    type_visit="I",  # hospit
    mode_entree="2-URG",  # Par les Urgence
    max_step=3,
    diagnosed_in_emergency=True,  # MTEV acquise aux urgence
    summarize_last_step=True,
    length_of_stay=2.0,
)
fig.update_layout(
    title_text="Population: VTE patients admitted via emergency and diagnosed in emergency department",
    font_size=20,
    width=1000,
)


fig.show(renderer="iframe")
pio.write_image(fig, "sankey_vte.pdf", width=1000, scale=2)
```

```python
fig = viz_sankey_plot(
    sankey_data=sankey_data,
    diagnosis="PE",  # VTE
    type_visit="I",  # hospit
    mode_entree="2-URG",  # Par les Urgence
    max_step=3,
    diagnosed_in_emergency=True,  # MTEV acquise aux urgence
    summarize_last_step=True,
)
fig.update_layout(
    # title_text="Population: PE patients admitted via emergency and diagnosed in emergency department",
    font_size=20,
    width=1000,
)
fig.show(renderer="iframe")
pio.write_image(fig, "sankey_pe.pdf", width=1000, scale=2)
```

## III. Histogram

A partir des données PMSI, nous pouvons générer les histogrammes suivants:

```python
histogram_data = pd.read_pickle(
    BASE_DIR / "data" / "parcours_patient" / config_name / "histogram_data.pkl"
)
```

```python
histogram_data_first = pd.read_pickle(
    BASE_DIR / "data" / "parcours_patient" / config_name / "histogram_data_first.pkl"
)
```

```python
chart, table = viz_histogram_first(
    data=histogram_data_first,
    category=None,
    mode_entree=None,
    type_visite="I",
    remove_pediatrics=False,
    remove_urgence=False,
)
display(chart)
display(table.groupby(["category"]).agg({"Number of stays": "sum"}))

chart_chir, table_chir = viz_histogram_first(
    data=histogram_data_first,
    category="SURGERY",
    mode_entree=None,
    type_visite="I",
    remove_pediatrics=True,
    remove_urgence=False,
)
display(chart_chir)
display(table_chir)

chart_interne, table_interne = viz_histogram_first(
    data=histogram_data_first,
    category="INTERNAL MEDICINE",
    mode_entree=None,
    type_visite="I",
    remove_pediatrics=True,
    remove_urgence=False,
)
display(chart_interne)
display(table_interne)
```

```python
chart, table = viz_histogram_first(
    data=histogram_data_first,
    category=None,
    mode_entree=None,
    type_visite="I",
    remove_pediatrics=True,
    remove_urgence=False,
)
display(chart)
display(table.groupby(["category"]).agg({"Number of stays": "sum"}))

chart_chir, table_chir = viz_histogram_first(
    data=histogram_data_first,
    category="SURGERY",
    mode_entree=None,
    type_visite="I",
    remove_pediatrics=True,
    remove_urgence=False,
)
display(chart_chir)
display(table_chir)

chart_interne, table_interne = viz_histogram_first(
    data=histogram_data_first,
    category="INTERNAL MEDICINE",
    mode_entree=None,
    type_visite="I",
    remove_pediatrics=True,
    remove_urgence=False,
)
display(chart_interne)
display(table_interne)
```

```python
chart, table = viz_histogram(
    data=histogram_data,
    diagnosis="VTE",
    category=None,
    mode_entree=None,
    type_visite="I",
    remove_unknown=True,
    remove_urgence=False,
    urgence_first=False,
    first_code=True,
)
display(chart)
display(table.groupby(["category", "type_MTEV"]).agg({"Number of stays": "sum"}))

chart_chir, table_chir = viz_histogram(
    data=histogram_data,
    diagnosis="VTE",
    category="SURGERY",
    mode_entree="2-URG",
    type_visite="I",
    remove_unknown=True,
    remove_urgence=False,
    urgence_first=False,
)
display(chart_chir)
display(table_chir)

chart_interne, table_interne = viz_histogram(
    data=histogram_data,
    diagnosis="VTE",
    category="INTERNAL MEDICINE",
    mode_entree="2-URG",
    type_visite="I",
    remove_unknown=True,
    remove_urgence=False,
    urgence_first=False,
)
display(chart_interne)
display(table_interne)
```

```python

```

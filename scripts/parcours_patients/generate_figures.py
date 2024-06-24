import os
import sys

import pandas as pd
import plotly
import typer
from confection import Config
from loguru import logger
from rich import print

from cse_180031 import BASE_DIR
from cse_180031.parcours_patient import (
    compute_stats_from_sankey_all,
    compute_stats_from_sankey_emergency,
    compute_stats_from_sankey_emergency_supplementary,
    compute_stats_per_cat_from_sankey_emergency,
    viz_histogram,
    viz_histogram_first,
    viz_sankey_plot,
)


def main(config_name: str = "study_v2_AREM_DP"):
    # Load config
    config_path = BASE_DIR / "conf" / "parcours_patients" / "{}.cfg".format(config_name)
    config = Config().from_disk(config_path, interpolate=True)

    if config["debug"]["debug"]:
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")

    # Load data
    histogram_data = pd.read_pickle(
        BASE_DIR / "data" / "parcours_patient" / config_name / "histogram_data.pkl"
    )
    histogram_data_first = pd.read_pickle(
        BASE_DIR
        / "data"
        / "parcours_patient"
        / config_name
        / "histogram_data_first.pkl"
    )
    sankey_data = pd.read_pickle(
        BASE_DIR / "data" / "parcours_patient" / config_name / "sankey_data.pkl"
    )

    # Exploratory analysis
    stats_table_emergency_per_cat = compute_stats_per_cat_from_sankey_emergency(
        sankey_data
    )
    stats_table_emergency_per_cat.to_csv(
        BASE_DIR
        / "figures"
        / "parcours_patients"
        / config_name
        / "stats_table_emergency_per_cat.csv"
    )
    stats_table_emergency = compute_stats_from_sankey_emergency(sankey_data)
    stats_table_emergency.to_csv(
        BASE_DIR
        / "figures"
        / "parcours_patients"
        / config_name
        / "stats_table_emergency.csv"
    )
    stats_table_emergency_supplementary = (
        compute_stats_from_sankey_emergency_supplementary(sankey_data)
    )
    stats_table_emergency_supplementary.to_csv(
        BASE_DIR
        / "figures"
        / "parcours_patients"
        / config_name
        / "stats_table_emergency_supplementary.csv"
    )
    stats_table_all = compute_stats_from_sankey_all(sankey_data)
    stats_table_all.to_csv(
        BASE_DIR / "figures" / "parcours_patients" / config_name / "stats_table_all.csv"
    )

    # Process viz
    diagnosis_config = config["MTEV_diagnosis"]

    for diagnosis in diagnosis_config.keys():
        print("Processing {}".format(diagnosis.replace("_", " ")))
        if not os.path.isdir(BASE_DIR / "figures" / "parcours_patients" / config_name):
            os.mkdir(BASE_DIR / "figures" / "parcours_patients" / config_name)
        if not os.path.isdir(
            BASE_DIR / "figures" / "parcours_patients" / config_name / diagnosis
        ):
            os.mkdir(
                BASE_DIR / "figures" / "parcours_patients" / config_name / diagnosis
            )
        if not os.path.isdir(
            BASE_DIR
            / "figures"
            / "parcours_patients"
            / config_name
            / diagnosis
            / "emergency_admission"
        ):
            os.mkdir(
                BASE_DIR
                / "figures"
                / "parcours_patients"
                / config_name
                / diagnosis
                / "emergency_admission"
            )
        if not os.path.isdir(
            BASE_DIR
            / "figures"
            / "parcours_patients"
            / config_name
            / diagnosis
            / "any_admission"
        ):
            os.mkdir(
                BASE_DIR
                / "figures"
                / "parcours_patients"
                / config_name
                / diagnosis
                / "any_admission"
            )
        # Sankey emergency admission
        fig = viz_sankey_plot(
            sankey_data=sankey_data,
            diagnosis=diagnosis,
            type_visit="I",  # hospit
            mode_entree="2-URG",  # arrived in emergency
            max_step=3,
            diagnosed_in_emergency=True,  # coded in emergency
            summarize_last_step=True,
        )
        fig.update_layout(
            font_size=20,
            width=1000,
        )
        plotly.offline.plot(
            fig,
            filename=str(
                BASE_DIR
                / "figures"
                / "parcours_patients"
                / config_name
                / diagnosis
                / "emergency_admission"
                / "sankey.html"
            ),
        )

        # Sankey all
        fig = viz_sankey_plot(
            sankey_data=sankey_data,
            diagnosis=diagnosis,
            type_visit="I",  # hospit
            mode_entree=None,  # arrived in emergency
            max_step=3,
            diagnosed_in_emergency=False,  # not only coded in emergency
        )
        fig.update_layout(
            font_size=20,
            width=1000,
        )
        plotly.offline.plot(
            fig,
            filename=str(
                BASE_DIR
                / "figures"
                / "parcours_patients"
                / config_name
                / diagnosis
                / "any_admission"
                / "sankey.html"
            ),
        )

        # Histogram VTE emergency first code
        if diagnosis == "VTE":
            chart, table = viz_histogram_first(
                data=histogram_data_first,
                category=None,
                mode_entree="2-URG",
                type_visite="I",
                remove_pediatrics=True,
                remove_urgence=False,
            )
            chart.save(
                BASE_DIR
                / "figures"
                / "parcours_patients"
                / config_name
                / diagnosis
                / "emergency_admission"
                / "histogram_general_first_coded.html"
            )
            table.to_csv(
                BASE_DIR
                / "figures"
                / "parcours_patients"
                / config_name
                / diagnosis
                / "emergency_admission"
                / "histogram_general_first_coded_table.csv"
            )

            chart_surgery, table_surgery = viz_histogram_first(
                data=histogram_data_first,
                category="SURGERY",
                mode_entree="2-URG",
                type_visite="I",
                remove_pediatrics=True,
                remove_urgence=False,
            )
            chart_surgery.save(
                BASE_DIR
                / "figures"
                / "parcours_patients"
                / config_name
                / diagnosis
                / "emergency_admission"
                / "histogram_surgery_first_coded.html"
            )
            table_surgery.to_csv(
                BASE_DIR
                / "figures"
                / "parcours_patients"
                / config_name
                / diagnosis
                / "emergency_admission"
                / "histogram_surgery_table_first_coded.csv"
            )

            chart_internal, table_internal = viz_histogram_first(
                data=histogram_data_first,
                category="INTERNAL MEDICINE",
                mode_entree="2-URG",
                type_visite="I",
                remove_pediatrics=True,
                remove_urgence=False,
            )
            chart_internal.save(
                BASE_DIR
                / "figures"
                / "parcours_patients"
                / config_name
                / diagnosis
                / "emergency_admission"
                / "histogram_internal_first_coded.html"
            )
            table_internal.to_csv(
                BASE_DIR
                / "figures"
                / "parcours_patients"
                / config_name
                / diagnosis
                / "emergency_admission"
                / "histogram_internal_first_coded_table.csv"
            )

        # Histogram emergency
        chart, table = viz_histogram(
            data=histogram_data,
            diagnosis=diagnosis,
            category=None,
            mode_entree="2-URG",
            type_visite="I",
            remove_unknown=True,
            remove_urgence=False,
            urgence_first=False,
        )
        chart.save(
            BASE_DIR
            / "figures"
            / "parcours_patients"
            / config_name
            / diagnosis
            / "emergency_admission"
            / "histogram_general.html"
        )
        table.to_csv(
            BASE_DIR
            / "figures"
            / "parcours_patients"
            / config_name
            / diagnosis
            / "emergency_admission"
            / "histogram_general_table.csv"
        )

        chart_surgery, table_surgery = viz_histogram(
            data=histogram_data,
            diagnosis=diagnosis,
            category="SURGERY",
            mode_entree="2-URG",
            type_visite="I",
            remove_unknown=True,
            remove_urgence=False,
            urgence_first=False,
        )
        chart_surgery.save(
            BASE_DIR
            / "figures"
            / "parcours_patients"
            / config_name
            / diagnosis
            / "emergency_admission"
            / "histogram_surgery.html"
        )
        table_surgery.to_csv(
            BASE_DIR
            / "figures"
            / "parcours_patients"
            / config_name
            / diagnosis
            / "emergency_admission"
            / "histogram_surgery_table.csv"
        )

        chart_internal, table_internal = viz_histogram(
            data=histogram_data,
            diagnosis=diagnosis,
            category="INTERNAL MEDICINE",
            mode_entree="2-URG",
            type_visite="I",
            remove_unknown=True,
            remove_urgence=False,
            urgence_first=False,
        )
        chart_internal.save(
            BASE_DIR
            / "figures"
            / "parcours_patients"
            / config_name
            / diagnosis
            / "emergency_admission"
            / "histogram_internal.html"
        )
        table_internal.to_csv(
            BASE_DIR
            / "figures"
            / "parcours_patients"
            / config_name
            / diagnosis
            / "emergency_admission"
            / "histogram_internal_table.csv"
        )

        # Histogram VTE any admission first code
        if diagnosis == "VTE":
            chart, table = viz_histogram_first(
                data=histogram_data_first,
                category=None,
                mode_entree=None,
                type_visite="I",
                remove_pediatrics=True,
                remove_urgence=False,
            )
            chart.save(
                BASE_DIR
                / "figures"
                / "parcours_patients"
                / config_name
                / diagnosis
                / "any_admission"
                / "histogram_general_first_coded.html"
            )
            table.to_csv(
                BASE_DIR
                / "figures"
                / "parcours_patients"
                / config_name
                / diagnosis
                / "any_admission"
                / "histogram_general_first_coded_table.csv"
            )

            chart_surgery, table_surgery = viz_histogram_first(
                data=histogram_data_first,
                category="SURGERY",
                mode_entree=None,
                type_visite="I",
                remove_pediatrics=True,
                remove_urgence=False,
            )
            chart_surgery.save(
                BASE_DIR
                / "figures"
                / "parcours_patients"
                / config_name
                / diagnosis
                / "any_admission"
                / "histogram_surgery_first_coded.html"
            )
            table_surgery.to_csv(
                BASE_DIR
                / "figures"
                / "parcours_patients"
                / config_name
                / diagnosis
                / "any_admission"
                / "histogram_surgery_table_first_coded.csv"
            )

            chart_internal, table_internal = viz_histogram_first(
                data=histogram_data_first,
                category="INTERNAL MEDICINE",
                mode_entree=None,
                type_visite="I",
                remove_pediatrics=True,
                remove_urgence=False,
            )
            chart_internal.save(
                BASE_DIR
                / "figures"
                / "parcours_patients"
                / config_name
                / diagnosis
                / "any_admission"
                / "histogram_internal_first_coded.html"
            )
            table_internal.to_csv(
                BASE_DIR
                / "figures"
                / "parcours_patients"
                / config_name
                / diagnosis
                / "any_admission"
                / "histogram_internal_first_coded_table.csv"
            )

        # Histogram any admission
        chart, table = viz_histogram(
            data=histogram_data,
            diagnosis=diagnosis,
            category=None,
            mode_entree=None,
            type_visite="I",
            remove_unknown=True,
            remove_urgence=False,
            urgence_first=False,
        )
        chart.save(
            BASE_DIR
            / "figures"
            / "parcours_patients"
            / config_name
            / diagnosis
            / "any_admission"
            / "histogram_general.html"
        )
        table.to_csv(
            BASE_DIR
            / "figures"
            / "parcours_patients"
            / config_name
            / diagnosis
            / "any_admission"
            / "histogram_general_table.csv"
        )

        chart_surgery, table_surgery = viz_histogram(
            data=histogram_data,
            diagnosis=diagnosis,
            category="SURGERY",
            mode_entree="2-URG",
            type_visite="I",
            remove_unknown=True,
            remove_urgence=False,
            urgence_first=False,
        )
        chart_surgery.save(
            BASE_DIR
            / "figures"
            / "parcours_patients"
            / config_name
            / diagnosis
            / "any_admission"
            / "histogram_surgery.html"
        )
        table_surgery.to_csv(
            BASE_DIR
            / "figures"
            / "parcours_patients"
            / config_name
            / diagnosis
            / "any_admission"
            / "histogram_surgery_table.csv"
        )

        chart_internal, table_internal = viz_histogram(
            data=histogram_data,
            diagnosis=diagnosis,
            category="INTERNAL MEDICINE",
            mode_entree="2-URG",
            type_visite="I",
            remove_unknown=True,
            remove_urgence=False,
            urgence_first=False,
        )
        chart_internal.save(
            BASE_DIR
            / "figures"
            / "parcours_patients"
            / config_name
            / diagnosis
            / "any_admission"
            / "histogram_internal.html"
        )
        table_internal.to_csv(
            BASE_DIR
            / "figures"
            / "parcours_patients"
            / config_name
            / diagnosis
            / "any_admission"
            / "histogram_internal_table.csv"
        )


if __name__ == "__main__":
    typer.run(main)

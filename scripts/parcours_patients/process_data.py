import os
import sys

from edstoolbox import SparkApp
from loguru import logger
from rich import print

from cse_180031 import BASE_DIR
from cse_180031.parcours_patient import (
    aggregate_histogram_data,
    aggregate_histogram_data_first,
    pivot_table,
    process_data,
)

app = SparkApp("parcours_patients")


@app.submit
def run(spark, sql, config):
    config_name = config["name"]["name"]
    if config["debug"]["debug"]:
        logger.remove()
        logger.add(sys.stderr, level="DEBUG")
    # Load data
    load_data_conf = config["load_data"]
    print("Loading data from database {}...".format(load_data_conf["database"]))

    sql("use {}".format(load_data_conf["database"]))
    patient = sql(
        f"""
            SELECT
            patient_num AS patient_id,
            sex_cd AS gender,
            vital_status_cd AS vital_status
            FROM {load_data_conf["patient_table"]}
            """
    )
    visit = sql(
        f"""
            SELECT
            encounter_num AS visit_id,
            patient_num AS patient_id,
            sourcesystem_cd AS source,
            type_visite,
            mode_entree,
            mode_sortie,
            length_of_stay,
            start_date AS visit_start_date,
            age_visit_in_years_num AS age
            FROM {load_data_conf["visit_table"]}
            """
    )
    visit_detail = sql(
        f"""
            SELECT
            instance_num AS visit_detail_id,
            encounter_num AS visit_id,
            start_date AS visit_detail_start_date,
            concept_cd
            FROM {load_data_conf["visit_detail_table"]}
            """
    )
    pmsi = sql(
        f"""
            SELECT
            encounter_num AS visit_id,
            sourcesystem_cd AS source,
            tval_char AS diag_type,
            concept_cd AS cim10,
            start_date AS pmsi_start_date,
            location_cd
            FROM {load_data_conf["pmsi_table"]}
            """
    )
    concept = sql(
        f"""
            SELECT
            concept_cd AS location_cd,
            name_char AS location_name
            FROM {load_data_conf["concept_table"]}
            """
    )
    # Select MTEV styas
    extraction_conf = config["MTEV_selection"]
    diagnosis = config["MTEV_diagnosis"]
    other_diagnosis = config["other_diagnosis"]
    print(
        "Selecting MTEV stays between {} and {}".format(
            extraction_conf["start_study"], extraction_conf["end_study"]
        )
    )
    visit_detail, pmsi, first_coded_VTE = process_data(
        visit_detail=visit_detail,
        visit=visit,
        patient=patient,
        pmsi=pmsi,
        concept=concept,
        source_pmsi=extraction_conf.get("source_pmsi"),
        min_age=extraction_conf["min_age"],
        start_study=extraction_conf["start_study"],
        end_study=extraction_conf["end_study"],
        list_cim_10_VTE=diagnosis["VTE"],
        list_cim_10_DVT=diagnosis["DVT"],
        list_cim_10_DVT_without_superficial=diagnosis["DVT_without_superficial"],
        list_cim_10_PE=diagnosis["PE"],
        list_cim_10_DVT_lower_limb=diagnosis["DVT_lower_limb"],
        list_cim_10_DVT_lower_limb_without_superficial=diagnosis[
            "DVT_lower_limb_without_superficial"
        ],
        list_cim_10_DVT_portal_vein=diagnosis["DVT_portal_vein"],
        list_cim_10_DVT_cerebral=diagnosis["DVT_cerebral"],
        list_cim_10_DVT_nonspecific_location=diagnosis["DVT_nonspecific_location"],
        list_cim_10_cancer=other_diagnosis["cancer"],
        diag_list=extraction_conf["diag_list"],
        spark=spark,
    )

    if not os.path.isdir(BASE_DIR / "data" / "parcours_patient" / config_name):
        os.mkdir(BASE_DIR / "data" / "parcours_patient" / config_name)

    # Histogrammes
    print("Aggregate PMSI by department for Histogram...")
    histogram_data = aggregate_histogram_data(pmsi=pmsi)

    histogram_data.to_pickle(
        BASE_DIR / "data" / "parcours_patient" / config_name / "histogram_data.pkl"
    )

    # Histogrammes VTE first coded
    print("Aggregate PMSI by department for Histogram first coded...")
    histogram_data_first = aggregate_histogram_data_first(
        first_coded_VTE=first_coded_VTE
    )
    histogram_data_first.to_pickle(
        BASE_DIR
        / "data"
        / "parcours_patient"
        / config_name
        / "histogram_data_first.pkl"
    )

    # Pivot table for Sankey
    print("Pivoting table for Sankey...")
    sankey_data = pivot_table(visit_detail)
    sankey_data.to_pickle(
        BASE_DIR / "data" / "parcours_patient" / config_name / "sankey_data.pkl"
    )


if __name__ == "__main__":
    app.run()

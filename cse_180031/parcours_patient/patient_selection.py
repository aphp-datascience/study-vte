from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window

from cse_180031.utils_cse import get_ref_strcut


def process_data(
    visit_detail: DataFrame,
    visit: DataFrame,
    patient: DataFrame,
    pmsi: DataFrame,
    concept: DataFrame,
    source_pmsi: str,
    min_age: float,
    start_study: str,
    end_study: str,
    list_cim_10_VTE: List[str],
    list_cim_10_DVT: List[str],
    list_cim_10_DVT_without_superficial: List[str],
    list_cim_10_PE: List[str],
    list_cim_10_DVT_lower_limb: List[str],
    list_cim_10_DVT_lower_limb_without_superficial: List[str],
    list_cim_10_DVT_nonspecific_location: List[str],
    list_cim_10_DVT_portal_vein: List[str],
    list_cim_10_DVT_cerebral: List[str],
    list_cim_10_cancer: List[str],
    diag_list: List[str],
    spark,
):
    ref_struct = get_ref_strcut()
    ref_struct = spark.createDataFrame(ref_struct)
    visit = filter_date(visit, start_study, end_study)
    visit = filter_age(visit, min_age)

    visit_detail = add_service(visit_detail, ref_struct)
    visit_detail, pmsi = filter_cim10(
        pmsi=pmsi,
        visit_detail=visit_detail,
        source_pmsi=source_pmsi,
        list_cim_10_VTE=list_cim_10_VTE,
        list_cim_10_DVT=list_cim_10_DVT,
        list_cim_10_DVT_without_superficial=list_cim_10_DVT_without_superficial,
        list_cim_10_PE=list_cim_10_PE,
        list_cim_10_DVT_lower_limb=list_cim_10_DVT_lower_limb,
        list_cim_10_DVT_lower_limb_without_superficial=list_cim_10_DVT_lower_limb_without_superficial,
        list_cim_10_DVT_nonspecific_location=list_cim_10_DVT_nonspecific_location,
        list_cim_10_DVT_portal_vein=list_cim_10_DVT_portal_vein,
        list_cim_10_DVT_cerebral=list_cim_10_DVT_cerebral,
        list_cim_10_cancer=list_cim_10_cancer,
        diag_list=diag_list,
    )

    visit_detail, pmsi = add_patient_info(visit_detail, pmsi, patient, visit)

    visit_detail, pmsi, first_coded_VTE = extract_urgence_mtev(
        visit_detail, pmsi, ref_struct, concept
    )

    return visit_detail, pmsi, first_coded_VTE


def filter_cim10(
    pmsi: DataFrame,
    visit_detail: DataFrame,
    source_pmsi: str,
    list_cim_10_VTE: List[str],
    list_cim_10_DVT: List[str],
    list_cim_10_DVT_without_superficial: List[str],
    list_cim_10_PE: List[str],
    list_cim_10_DVT_lower_limb: List[str],
    list_cim_10_DVT_lower_limb_without_superficial: List[str],
    list_cim_10_DVT_nonspecific_location: List[str],
    list_cim_10_DVT_portal_vein: List[str],
    list_cim_10_DVT_cerebral: List[str],
    list_cim_10_cancer: List[str],
    diag_list: List[str],
):
    regex_cancer = r"^(" + "|".join(list_cim_10_cancer) + ")"
    if source_pmsi:
        pmsi = pmsi.filter((col("source") == source_pmsi))
    pmsi = pmsi.drop("source")
    pmsi = pmsi.filter(col("diag_type").isin(diag_list))
    pmsi = pmsi.withColumn("cim10", F.split(pmsi["cim10"], ":").getItem(1))
    pmsi = (
        pmsi.withColumn(
            "cim10_VTE",
            F.when(
                col("cim10").isin(list_cim_10_VTE),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "cim10_thrombo_embolie",
            F.when(
                col("cim10").isin(list_cim_10_DVT),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "cim10_thrombo_embolie_without_superficial",
            F.when(
                col("cim10").isin(list_cim_10_DVT_without_superficial),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "cim10_embolie",
            F.when(
                col("cim10").isin(list_cim_10_PE),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "cim10_DVT_lower_limb",
            F.when(
                col("cim10").isin(list_cim_10_DVT_lower_limb),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "cim10_DVT_lower_limb_without_superficial",
            F.when(
                col("cim10").isin(list_cim_10_DVT_lower_limb_without_superficial),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "cim10_DVT_nonspecific_location",
            F.when(
                col("cim10").isin(list_cim_10_DVT_nonspecific_location),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "cim10_DVT_portal_vein",
            F.when(
                col("cim10").isin(list_cim_10_DVT_portal_vein),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "cim10_DVT_cerebral",
            F.when(
                col("cim10").isin(list_cim_10_DVT_cerebral),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "cim10_cancer",
            F.when(
                col("cim10").rlike(regex_cancer),
                1,
            ).otherwise(0),
        )
        .drop("cim10")
        .drop("diag_type")
    )
    pmsi_mtev = pmsi.groupBy("visit_id").agg(
        F.max(col("cim10_VTE")).alias("visit_VTE"),
        F.max(col("cim10_thrombo_embolie")).alias("visit_DVT"),
        F.max(col("cim10_thrombo_embolie_without_superficial")).alias(
            "visit_DVT_without_superficial"
        ),
        F.max(col("cim10_embolie")).alias("visit_PE"),
        F.max(col("cim10_DVT_lower_limb")).alias("visit_DVT_lower_limb"),
        F.max(col("cim10_DVT_lower_limb_without_superficial")).alias(
            "visit_DVT_lower_limb_without_superficial"
        ),
        F.max(col("cim10_DVT_nonspecific_location")).alias(
            "visit_DVT_nonspecific_location"
        ),
        F.max(col("cim10_DVT_portal_vein")).alias("visit_DVT_portal_vein"),
        F.max(col("cim10_DVT_cerebral")).alias("visit_DVT_cerebral"),
        F.max(col("cim10_cancer")).alias("visit_cancer"),
    )
    pmsi_mtev = pmsi_mtev.filter(col("visit_VTE") == 1)
    pmsi = pmsi.join(pmsi_mtev, on="visit_id", how="inner")
    visit_detail = pmsi_mtev.join(visit_detail, on="visit_id", how="left")
    return visit_detail, pmsi


def filter_date(visit: DataFrame, start_study, end_study):
    return visit.filter(
        (col("visit_start_date") >= start_study) & (col("visit_start_date") < end_study)
    )


def filter_type_visite(visit: DataFrame, type_visite: str):
    return visit.filter(col("type_visite") == type_visite)


def filter_age(visit: DataFrame, min_age: float):
    return visit.filter(col("age") > min_age)


def extract_urgence_mtev(
    visit_detail: DataFrame,
    pmsi: DataFrame,
    ref_struct: DataFrame,
    concept: DataFrame,
):
    pmsi = pmsi.join(concept, on="location_cd", how="left")
    pmsi = pmsi.withColumn("cd_ufr", F.split(pmsi["location_cd"], ":").getItem(1)).drop(
        pmsi.location_cd
    )

    pmsi = pmsi.join(ref_struct, on="cd_ufr", how="left")

    # MTEV aux urgences

    pmsi = pmsi.withColumn(
        "cim10_VTE_urgence",
        F.when(
            (col("category") == "EMERGENCY") & (col("cim10_VTE") == 1),
            1,
        ).otherwise(0),
    )
    pmsi = pmsi.withColumn(
        "cim10_thrombo_embolie_urgence",
        F.when(
            (col("category") == "EMERGENCY") & (col("cim10_thrombo_embolie") == 1),
            1,
        ).otherwise(0),
    )
    pmsi = pmsi.withColumn(
        "cim10_thrombo_embolie_without_superficial_urgence",
        F.when(
            (col("category") == "EMERGENCY")
            & (col("cim10_thrombo_embolie_without_superficial") == 1),
            1,
        ).otherwise(0),
    )
    pmsi = pmsi.withColumn(
        "cim10_embolie_urgence",
        F.when(
            (col("category") == "EMERGENCY") & (col("cim10_embolie") == 1),
            1,
        ).otherwise(0),
    )
    pmsi = pmsi.withColumn(
        "cim10_DVT_lower_limb_urgence",
        F.when(
            (col("category") == "EMERGENCY") & (col("cim10_DVT_lower_limb") == 1),
            1,
        ).otherwise(0),
    )
    pmsi = pmsi.withColumn(
        "cim10_DVT_lower_limb_without_superficial_urgence",
        F.when(
            (col("category") == "EMERGENCY")
            & (col("cim10_DVT_lower_limb_without_superficial") == 1),
            1,
        ).otherwise(0),
    )
    pmsi = pmsi.withColumn(
        "cim10_DVT_nonspecific_location_urgence",
        F.when(
            (col("category") == "EMERGENCY")
            & (col("cim10_DVT_nonspecific_location") == 1),
            1,
        ).otherwise(0),
    )
    pmsi = pmsi.withColumn(
        "cim10_DVT_portal_vein_urgence",
        F.when(
            (col("category") == "EMERGENCY") & (col("cim10_DVT_portal_vein") == 1),
            1,
        ).otherwise(0),
    )
    pmsi = pmsi.withColumn(
        "cim10_DVT_cerebral_urgence",
        F.when(
            (col("category") == "EMERGENCY") & (col("cim10_DVT_cerebral") == 1),
            1,
        ).otherwise(0),
    )

    # MTEV 1ere visite aux urgences

    first_visit = Window.partitionBy(pmsi.visit_id).orderBy(pmsi.pmsi_start_date)

    pmsi = pmsi.withColumn(
        "cim10_VTE_urgence_first",
        F.when(
            (F.first("category").over(first_visit) == "EMERGENCY")
            & (F.first("cim10_VTE").over(first_visit) == 1),
            1,
        ).otherwise(0),
    )
    pmsi = pmsi.withColumn(
        "cim10_thrombo_embolie_without_superficial_urgence_first",
        F.when(
            (F.first("category").over(first_visit) == "EMERGENCY")
            & (
                F.first("cim10_thrombo_embolie_without_superficial").over(first_visit)
                == 1
            ),
            1,
        ).otherwise(0),
    )
    pmsi = pmsi.withColumn(
        "cim10_thrombo_embolie_urgence_first",
        F.when(
            (F.first("category").over(first_visit) == "EMERGENCY")
            & (F.first("cim10_thrombo_embolie").over(first_visit) == 1),
            1,
        ).otherwise(0),
    )
    pmsi = pmsi.withColumn(
        "cim10_embolie_urgence_first",
        F.when(
            (F.first("category").over(first_visit) == "EMERGENCY")
            & (F.first("cim10_embolie").over(first_visit) == 1),
            1,
        ).otherwise(0),
    )
    pmsi = pmsi.withColumn(
        "cim10_DVT_lower_limb_urgence_first",
        F.when(
            (F.first("category").over(first_visit) == "EMERGENCY")
            & (F.first("cim10_DVT_lower_limb").over(first_visit) == 1),
            1,
        ).otherwise(0),
    )
    pmsi = pmsi.withColumn(
        "cim10_DVT_lower_limb_without_superficial_urgence_first",
        F.when(
            (F.first("category").over(first_visit) == "EMERGENCY")
            & (
                F.first("cim10_DVT_lower_limb_without_superficial").over(first_visit)
                == 1
            ),
            1,
        ).otherwise(0),
    )
    pmsi = pmsi.withColumn(
        "cim10_DVT_nonspecific_location_urgence_first",
        F.when(
            (F.first("category").over(first_visit) == "EMERGENCY")
            & (F.first("cim10_DVT_nonspecific_location").over(first_visit) == 1),
            1,
        ).otherwise(0),
    )
    pmsi = pmsi.withColumn(
        "cim10_DVT_portal_vein_urgence_first",
        F.when(
            (F.first("category").over(first_visit) == "EMERGENCY")
            & (F.first("cim10_DVT_portal_vein").over(first_visit) == 1),
            1,
        ).otherwise(0),
    )
    pmsi = pmsi.withColumn(
        "cim10_DVT_cerebral_urgence_first",
        F.when(
            (F.first("category").over(first_visit) == "EMERGENCY")
            & (F.first("cim10_DVT_cerebral").over(first_visit) == 1),
            1,
        ).otherwise(0),
    )

    pmsi_urgence = pmsi.groupBy("visit_id").agg(
        F.max(col("cim10_VTE_urgence")).alias("visit_VTE_urgence"),
        F.max(col("cim10_thrombo_embolie_urgence")).alias("visit_DVT_urgence"),
        F.max(col("cim10_thrombo_embolie_without_superficial_urgence")).alias(
            "visit_DVT_without_superficial_urgence"
        ),
        F.max(col("cim10_embolie_urgence")).alias("visit_PE_urgence"),
        F.max(col("cim10_DVT_lower_limb_urgence")).alias(
            "visit_DVT_lower_limb_urgence"
        ),
        F.max(col("cim10_DVT_lower_limb_without_superficial_urgence")).alias(
            "visit_DVT_lower_limb_without_superficial_urgence"
        ),
        F.max(col("cim10_DVT_nonspecific_location_urgence")).alias(
            "visit_DVT_nonspecific_location_urgence"
        ),
        F.max(col("cim10_DVT_portal_vein_urgence")).alias(
            "visit_DVT_portal_vein_urgence"
        ),
        F.max(col("cim10_DVT_cerebral_urgence")).alias(
            "visit_DVT_cerebral_urgence",
        ),
        F.max(col("cim10_VTE_urgence_first")).alias("visit_VTE_urgence_first"),
        F.max(col("cim10_thrombo_embolie_without_superficial_urgence_first")).alias(
            "visit_DVT_without_superficial_urgence_first"
        ),
        F.max(col("cim10_thrombo_embolie_urgence_first")).alias(
            "visit_DVT_urgence_first"
        ),
        F.max(col("cim10_embolie_urgence_first")).alias("visit_PE_urgence_first"),
        F.max(col("cim10_DVT_lower_limb_urgence_first")).alias(
            "visit_DVT_lower_limb_urgence_first"
        ),
        F.max(col("cim10_DVT_lower_limb_without_superficial_urgence_first")).alias(
            "visit_DVT_lower_limb_without_superficial_urgence_first"
        ),
        F.max(col("cim10_DVT_nonspecific_location_urgence_first")).alias(
            "visit_DVT_nonspecific_location_urgence_first"
        ),
        F.max(col("cim10_DVT_portal_vein_urgence_first")).alias(
            "visit_DVT_portal_vein_urgence_first"
        ),
        F.max(col("cim10_DVT_cerebral_urgence_first")).alias(
            "visit_DVT_cerebral_urgence_first",
        ),
    )

    pmsi = pmsi.join(pmsi_urgence, on="visit_id").drop(
        "cim10_VTE_urgence",
        "cim10_thrombo_embolie_urgence",
        "cim10_thrombo_embolie_without_superficial_urgence",
        "cim10_embolie_urgence",
        "cim10_DVT_lower_limb_urgence",
        "cim10_DVT_lower_limb_without_superficial_urgence",
        "cim10_DVT_nonspecific_location_urgence",
        "cim10_DVT_portal_vein_urgence",
        "cim10_DVT_cerebral_urgence",
        "cim10_VTE_urgence_first",
        "cim10_thrombo_embolie_urgence_first",
        "cim10_thrombo_embolie_without_superficial_urgence_first",
        "cim10_embolie_urgence_first",
        "cim10_DVT_lower_limb_urgence_first",
        "cim10_DVT_lower_limb_without_superficial_urgence_first",
        "cim10_DVT_nonspecific_location_urgence_first",
        "cim10_DVT_portal_vein_urgence_first",
        "cim10_DVT_cerebral_urgence_first",
    )
    visit_detail = pmsi_urgence.join(visit_detail, on="visit_id", how="left")

    # Get the first VTE code by location category (Emergency, Intensive care...etc.)
    visit_detail_date = visit_detail.groupBy(
        ["visit_id", "category", "sub_category"]
    ).agg(F.min(col("visit_detail_start_date")).alias("visit_detail_start_date"))
    first_coded_VTE = (
        pmsi.select(
            [
                "cim10_VTE",
                "pmsi_start_date",
                "visit_id",
                "category",
                "sub_category",
                "type_visite",
                "mode_entree",
            ]
        )
        .filter(~(col("cim10_VTE") == 0))
        .filter(~(col("category").isNull()))
        .filter(~(col("category") == "NOT SPECIFIED"))
    )

    first_coded_VTE = first_coded_VTE.join(
        visit_detail_date, on=["visit_id", "category", "sub_category"], how="left"
    )
    first_coded_VTE = first_coded_VTE.withColumn(
        "mixed_date",
        F.when(
            col("visit_detail_start_date").isNull(), col("pmsi_start_date")
        ).otherwise(col("visit_detail_start_date")),
    )
    first_code = Window.partitionBy(first_coded_VTE.visit_id).orderBy(
        first_coded_VTE.mixed_date
    )
    first_coded_VTE = first_coded_VTE.withColumn(
        "row", F.row_number().over(first_code)
    ).filter(col("row") == 1)
    first_coded_VTE = first_coded_VTE.groupBy(
        [
            "visit_id",
            "category",
            "sub_category",
            "type_visite",
            "mode_entree",
        ]
    ).agg(F.max(col("cim10_VTE")).alias("visit_VTE_per_cat_first"))
    first_coded_VTE = first_coded_VTE.cache()

    first_coded_VTE_no_loc = (
        pmsi.select(
            [
                "cim10_VTE",
                "visit_id",
                "type_visite",
                "mode_entree",
            ]
        )
        .filter(~(col("cim10_VTE") == 0))
        .dropDuplicates(["visit_id"])
        .withColumn("category", F.lit("NOT SPECIFIED"))
        .withColumn("sub_category", F.lit("NOT SPECIFIED"))
        .withColumnRenamed("cim10_VTE", "visit_VTE_per_cat_first")
        .join(
            first_coded_VTE.select("visit_id"),
            on=["visit_id"],
            how="leftanti",
        )
    )
    first_coded_VTE = first_coded_VTE.unionByName(first_coded_VTE_no_loc)
    return visit_detail, pmsi, first_coded_VTE


def add_service(visit_detail: DataFrame, ref_struct: DataFrame):
    visit_detail = visit_detail.withColumn(
        "cd_ufr", F.split(visit_detail["concept_cd"], ":").getItem(1)
    ).drop(visit_detail.concept_cd)

    visit_detail = visit_detail.join(ref_struct, on="cd_ufr", how="left")
    visit_detail = visit_detail.dropDuplicates(["visit_id", "cd_ufr"])

    visit_detail = visit_detail.drop(
        "start_validity_date", "end_validity_date", "cd_ufr"
    )

    return visit_detail


def add_patient_info(
    visit_detail: DataFrame, pmsi: DataFrame, patient: DataFrame, visit: DataFrame
):
    visit = visit.filter((col("source") == "ORBIS")).drop("source")
    visit = visit.join(patient, on="patient_id", how="left")
    pmsi = pmsi.join(visit, on="visit_id", how="inner")
    visit_detail = visit_detail.join(visit, on="visit_id", how="inner")
    return visit_detail, pmsi

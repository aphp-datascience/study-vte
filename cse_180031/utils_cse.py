import pandas as pd
from structure_classification.cc9.retrieve_struct import StructQualif

from cse_180031 import BASE_DIR


def get_ref_strcut():
    sq = StructQualif(
        axis="organisation_medicale",
        db="i2b2",
    )
    ref_struct = sq.ref_struct
    ref_struct["department"] = ref_struct.uf_lib_cc9.astype(str)
    ref_struct["activity"] = ref_struct.cd_champ_pmsi.astype(str)
    ref_struct_uf = ref_struct[
        ["cd_uf", "dt_deb_uf", "dt_fin_uf", "department", "activity"]
    ].copy()
    ref_struct_uf = ref_struct_uf.rename(
        columns={
            "cd_uf": "cd_ufr",
            "dt_deb_uf": "start_validity_date",
            "dt_fin_uf": "end_validity_date",
        }
    )
    ref_struct_us = ref_struct[
        ["cd_us", "dt_deb_us", "dt_fin_us", "department", "activity"]
    ].copy()
    ref_struct_us = ref_struct_us.rename(
        columns={
            "cd_us": "cd_ufr",
            "dt_deb_us": "start_validity_date",
            "dt_fin_us": "end_validity_date",
        }
    )
    ref_struct = pd.concat([ref_struct_uf, ref_struct_us])
    ref_struct["department"] = ref_struct.department.mask(
        ref_struct.department == "nan", "NOT SPECIFIED"
    )
    ref_struct["department"] = ref_struct.department.mask(
        ref_struct.department.isna(), "NOT SPECIFIED"
    )
    ref_struct["activity"] = ref_struct.activity.mask(
        ref_struct.activity == "nan", "NOT SPECIFIED"
    )
    ref_struct["activity"] = ref_struct.activity.mask(
        ref_struct.activity.isna(), "NOT SPECIFIED"
    )
    uf_not_specified = (
        pd.read_csv(BASE_DIR / "data" / "UF_NOT_SPECIFIED.csv", sep=",")
        .drop(
            columns=[
                "location_name",
                "mode_entree",
                "type_visite",
                "Number of stays",
                "Percentage",
            ]
        )
        .rename(columns={"location_name": "department"})
    )
    uf_not_specified.cd_ufr = uf_not_specified.cd_ufr.astype(str)
    uf_not_specified.cd_ufr = uf_not_specified.cd_ufr.mask(
        uf_not_specified.cd_ufr.str.len() == 5, "0" + uf_not_specified.cd_ufr
    )
    uf_not_specified.cd_ufr = uf_not_specified.cd_ufr.mask(
        uf_not_specified.cd_ufr.str.len() == 4, "00" + uf_not_specified.cd_ufr
    )
    uf_not_specified["start_validity_date"] = None
    uf_not_specified["end_validity_date"] = None
    uf_not_specified["activity"] = "MCO"
    uf_not_specified["activity"] = uf_not_specified["activity"].mask(
        uf_not_specified.category == "NOT SPECIFIED", "NOT SPECIFIED"
    )
    uf_not_specified["sub_category"] = "NOT SPECIFIED"
    department_category = pd.read_csv(
        BASE_DIR / "data" / "department_category.csv", sep=";"
    ).drop(columns="stay_count")
    department_category["category"] = department_category["category"].astype(str)
    department_category["sub_category"] = department_category["sub_category"].astype(
        str
    )
    ref_struct = ref_struct.merge(department_category, on="department", how="left")
    ref_struct["category"] = ref_struct.category.mask(
        ref_struct.category == "nan", "NOT SPECIFIED"
    )
    ref_struct["category"] = ref_struct.category.mask(
        ref_struct.category.isna(), "NOT SPECIFIED"
    )
    ref_struct["sub_category"] = ref_struct.sub_category.mask(
        ref_struct.sub_category == "nan", "NOT SPECIFIED"
    )
    ref_struct["sub_category"] = ref_struct.sub_category.mask(
        ref_struct.sub_category.isna(), "NOT SPECIFIED"
    )
    ref_struct = ref_struct.merge(
        uf_not_specified[["cd_ufr", "category"]],
        on="cd_ufr",
        how="left",
        suffixes=("", "_sup"),
    )
    ref_struct["category"] = ref_struct.category.where(
        ref_struct.category_sup.isna(), ref_struct.category_sup
    )
    uf_not_specified = uf_not_specified[
        ~uf_not_specified.cd_ufr.isin(ref_struct.cd_ufr.unique())
    ]
    ref_struct = ref_struct.drop(columns="category_sup")
    ref_struct = pd.concat([ref_struct, uf_not_specified])
    ref_struct["activity"] = ref_struct["activity"].mask(
        ref_struct.category == "REHABILITATION", "SSR"
    )
    ref_struct["category"] = ref_struct["category"].mask(
        ref_struct.category == "SSR", "REHABILITATION"
    )
    ref_struct.department = ref_struct.department.astype(str)
    return ref_struct

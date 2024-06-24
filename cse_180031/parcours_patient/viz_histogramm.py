import altair as alt
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col


def aggregate_histogram_data_first(first_coded_VTE: DataFrame):
    histogram_data_first = (
        first_coded_VTE.groupBy(
            "category",
            "sub_category",
            "visit_VTE_per_cat_first",
            "mode_entree",
            "type_visite",
        )
        .agg(F.countDistinct("visit_id").alias("Number of stays"))
        .sort(col("Number of stays").desc())
        .toPandas()
    )
    return histogram_data_first


def aggregate_histogram_data(pmsi: DataFrame):
    histogram_data = pmsi.groupBy(
        "visit_id",
        "category",
        "sub_category",
        "visit_VTE",
        "visit_DVT",
        "visit_DVT_without_superficial",
        "visit_PE",
        "visit_DVT_lower_limb",
        "visit_DVT_lower_limb_without_superficial",
        "visit_DVT_nonspecific_location",
        "visit_DVT_portal_vein",
        "visit_DVT_cerebral",
        "visit_VTE_urgence",
        "visit_DVT_urgence",
        "visit_DVT_without_superficial_urgence",
        "visit_PE_urgence",
        "visit_DVT_lower_limb_urgence",
        "visit_DVT_lower_limb_without_superficial_urgence",
        "visit_DVT_nonspecific_location_urgence",
        "visit_DVT_portal_vein_urgence",
        "visit_DVT_cerebral_urgence",
        "visit_VTE_urgence_first",
        "visit_DVT_urgence_first",
        "visit_DVT_without_superficial_urgence_first",
        "visit_PE_urgence_first",
        "visit_DVT_lower_limb_urgence_first",
        "visit_DVT_lower_limb_without_superficial_urgence_first",
        "visit_DVT_nonspecific_location_urgence_first",
        "visit_DVT_portal_vein_urgence_first",
        "visit_DVT_cerebral_urgence_first",
        "mode_entree",
        "mode_sortie",
        "length_of_stay",
        "age",
        "gender",
        "type_visite",
    ).agg(
        F.max(col("cim10_VTE")).alias("visit_VTE_per_cat"),
        F.max(col("cim10_thrombo_embolie")).alias("visit_DVT_per_cat"),
        F.max(col("cim10_thrombo_embolie_without_superficial")).alias(
            "visit_DVT_without_superficial_per_cat"
        ),
        F.max(col("cim10_embolie")).alias("visit_PE_per_cat"),
        F.max(col("cim10_DVT_lower_limb")).alias("visit_DVT_lower_limb_per_cat"),
        F.max(col("cim10_DVT_lower_limb_without_superficial")).alias(
            "visit_DVT_lower_limb_without_superficial_per_cat"
        ),
        F.max(col("cim10_DVT_nonspecific_location")).alias(
            "visit_DVT_nonspecific_location_per_cat"
        ),
        F.max(col("cim10_DVT_portal_vein")).alias("visit_DVT_portal_vein_per_cat"),
        F.max(col("cim10_DVT_cerebral")).alias(
            "visit_DVT_cerebral_per_cat",
        ),
    )

    histogram_data = (
        histogram_data.groupby(
            "category",
            "sub_category",
            "visit_VTE",
            "visit_DVT",
            "visit_DVT_without_superficial",
            "visit_PE",
            "visit_DVT_lower_limb",
            "visit_DVT_lower_limb_without_superficial",
            "visit_DVT_nonspecific_location",
            "visit_DVT_portal_vein",
            "visit_DVT_cerebral",
            "visit_VTE_urgence",
            "visit_DVT_urgence",
            "visit_DVT_without_superficial_urgence",
            "visit_PE_urgence",
            "visit_DVT_lower_limb_urgence",
            "visit_DVT_lower_limb_without_superficial_urgence",
            "visit_DVT_nonspecific_location_urgence",
            "visit_DVT_portal_vein_urgence",
            "visit_DVT_cerebral_urgence",
            "visit_VTE_urgence_first",
            "visit_DVT_urgence_first",
            "visit_DVT_without_superficial_urgence_first",
            "visit_PE_urgence_first",
            "visit_DVT_lower_limb_urgence_first",
            "visit_DVT_lower_limb_without_superficial_urgence_first",
            "visit_DVT_nonspecific_location_urgence_first",
            "visit_DVT_portal_vein_urgence_first",
            "visit_DVT_cerebral_urgence_first",
            "visit_VTE_per_cat",
            "visit_DVT_per_cat",
            "visit_DVT_without_superficial_per_cat",
            "visit_PE_per_cat",
            "visit_DVT_lower_limb_per_cat",
            "visit_DVT_lower_limb_without_superficial_per_cat",
            "visit_DVT_nonspecific_location_per_cat",
            "visit_DVT_portal_vein_per_cat",
            "visit_DVT_cerebral_per_cat",
            "mode_entree",
            "mode_sortie",
            "length_of_stay",
            "age",
            "gender",
            "type_visite",
        )
        .agg(F.countDistinct("visit_id").alias("Number of stays"))
        .sort(col("Number of stays").desc())
        .toPandas()
    )
    histogram_data["visit_DVT"] = histogram_data["visit_DVT"].where(
        histogram_data["visit_PE"] == 0, 0
    )
    histogram_data["visit_DVT_without_superficial"] = histogram_data[
        "visit_DVT_without_superficial"
    ].where(histogram_data["visit_PE"] == 0, 0)
    histogram_data["visit_DVT_lower_limb"] = histogram_data[
        "visit_DVT_lower_limb"
    ].where(histogram_data["visit_PE"] == 0, 0)
    histogram_data["visit_DVT_lower_limb_without_superficial"] = histogram_data[
        "visit_DVT_lower_limb_without_superficial"
    ].where(histogram_data["visit_PE"] == 0, 0)
    histogram_data["visit_DVT_nonspecific_location"] = histogram_data[
        "visit_DVT_nonspecific_location"
    ].where(histogram_data["visit_PE"] == 0, 0)
    histogram_data["visit_DVT_portal_vein"] = histogram_data[
        "visit_DVT_portal_vein"
    ].where(histogram_data["visit_PE"] == 0, 0)
    histogram_data["visit_DVT_cerebral"] = histogram_data["visit_DVT_cerebral"].where(
        histogram_data["visit_PE"] == 0, 0
    )
    histogram_data["visit_DVT_other_location"] = histogram_data["visit_VTE"].where(
        (histogram_data["visit_PE"] == 0)
        & (histogram_data["visit_DVT_lower_limb"] == 0),
        0,
    )
    histogram_data["visit_DVT_urgence"] = histogram_data["visit_DVT_urgence"].where(
        histogram_data["visit_PE_urgence"] == 0, 0
    )
    histogram_data["visit_DVT_without_superficial_urgence"] = histogram_data[
        "visit_DVT_without_superficial_urgence"
    ].where(histogram_data["visit_PE_urgence"] == 0, 0)
    histogram_data["visit_DVT_lower_limb_urgence"] = histogram_data[
        "visit_DVT_lower_limb_urgence"
    ].where(histogram_data["visit_PE_urgence"] == 0, 0)
    histogram_data["visit_DVT_lower_limb_without_superficial_urgence"] = histogram_data[
        "visit_DVT_lower_limb_without_superficial_urgence"
    ].where(histogram_data["visit_PE_urgence"] == 0, 0)
    histogram_data["visit_DVT_nonspecific_location_urgence"] = histogram_data[
        "visit_DVT_nonspecific_location_urgence"
    ].where(histogram_data["visit_PE_urgence"] == 0, 0)
    histogram_data["visit_DVT_portal_vein_urgence"] = histogram_data[
        "visit_DVT_portal_vein_urgence"
    ].where(histogram_data["visit_PE_urgence"] == 0, 0)
    histogram_data["visit_DVT_cerebral_urgence"] = histogram_data[
        "visit_DVT_cerebral_urgence"
    ].where(histogram_data["visit_PE_urgence"] == 0, 0)
    histogram_data["visit_DVT_other_location_urgence"] = histogram_data[
        "visit_VTE_urgence"
    ].where(
        (histogram_data["visit_PE_urgence"] == 0)
        & (histogram_data["visit_DVT_lower_limb_urgence"] == 0),
        0,
    )
    histogram_data["visit_DVT_urgence_first"] = histogram_data[
        "visit_DVT_urgence_first"
    ].where(histogram_data["visit_PE_urgence_first"] == 0, 0)
    histogram_data["visit_DVT_without_superficial_urgence_first"] = histogram_data[
        "visit_DVT_without_superficial_urgence_first"
    ].where(histogram_data["visit_PE_urgence_first"] == 0, 0)
    histogram_data["visit_DVT_lower_limb_urgence_first"] = histogram_data[
        "visit_DVT_lower_limb_urgence_first"
    ].where(histogram_data["visit_PE_urgence_first"] == 0, 0)
    histogram_data[
        "visit_DVT_lower_limb_without_superficial_urgence_first"
    ] = histogram_data["visit_DVT_lower_limb_without_superficial_urgence_first"].where(
        histogram_data["visit_PE_urgence_first"] == 0, 0
    )
    histogram_data["visit_DVT_nonspecific_location_urgence_first"] = histogram_data[
        "visit_DVT_nonspecific_location_urgence_first"
    ].where(histogram_data["visit_PE_urgence_first"] == 0, 0)
    histogram_data["visit_DVT_portal_vein_urgence_first"] = histogram_data[
        "visit_DVT_portal_vein_urgence_first"
    ].where(histogram_data["visit_PE_urgence_first"] == 0, 0)
    histogram_data["visit_DVT_cerebral_urgence_first"] = histogram_data[
        "visit_DVT_cerebral_urgence_first"
    ].where(histogram_data["visit_PE_urgence_first"] == 0, 0)
    histogram_data["visit_DVT_other_location_urgence_first"] = histogram_data[
        "visit_VTE_urgence_first"
    ].where(
        (histogram_data["visit_PE_urgence_first"] == 0)
        & (histogram_data["visit_DVT_lower_limb_urgence_first"] == 0),
        0,
    )
    histogram_data["visit_DVT_per_cat"] = histogram_data["visit_DVT_per_cat"].where(
        histogram_data["visit_PE_per_cat"] == 0, 0
    )
    histogram_data["visit_DVT_without_superficial_per_cat"] = histogram_data[
        "visit_DVT_without_superficial_per_cat"
    ].where(histogram_data["visit_PE_per_cat"] == 0, 0)
    histogram_data["visit_DVT_lower_limb_per_cat"] = histogram_data[
        "visit_DVT_lower_limb_per_cat"
    ].where(histogram_data["visit_PE_per_cat"] == 0, 0)
    histogram_data["visit_DVT_lower_limb_without_superficial_per_cat"] = histogram_data[
        "visit_DVT_lower_limb_without_superficial_per_cat"
    ].where(histogram_data["visit_PE_per_cat"] == 0, 0)
    histogram_data["visit_DVT_nonspecific_location_per_cat"] = histogram_data[
        "visit_DVT_nonspecific_location_per_cat"
    ].where(histogram_data["visit_PE_per_cat"] == 0, 0)
    histogram_data["visit_DVT_portal_vein_per_cat"] = histogram_data[
        "visit_DVT_portal_vein_per_cat"
    ].where(histogram_data["visit_PE_per_cat"] == 0, 0)
    histogram_data["visit_DVT_cerebral_per_cat"] = histogram_data[
        "visit_DVT_cerebral_per_cat"
    ].where(histogram_data["visit_PE_per_cat"] == 0, 0)
    histogram_data["visit_DVT_other_location_per_cat"] = histogram_data[
        "visit_VTE_per_cat"
    ].where(
        (histogram_data["visit_PE_per_cat"] == 0)
        & (histogram_data["visit_DVT_lower_limb_per_cat"] == 0),
        0,
    )
    return histogram_data


def viz_histogram_first(
    data,
    category: str = None,
    mode_entree: str = "2-URG",
    type_visite: str = "I",
    remove_pediatrics: bool = False,
    remove_urgence: bool = False,
):
    alt.data_transformers.disable_max_rows()

    # Diagnosis
    data = data[data["visit_VTE_per_cat_first"] == 1]

    # Mode d'entrée
    if mode_entree:
        data = data[data.mode_entree == mode_entree]
    if mode_entree == "2-URG":
        title_entree = "emergency"
    else:
        title_entree = "any"

    # Type visite
    if type_visite:
        data = data[data.type_visite == type_visite]

    # Remove NOT SPECIFIED
    if remove_pediatrics:
        data = data[~(data.category == "PEDIATRICS")]

    # Remove Urgence
    if remove_urgence:
        data = data[~(data.category == "EMERGENCY")]

    # Rename some category
    data.category = data.category.replace(" PNEUMOLOGY", "PULMONOLOGY")
    data.category = data.category.replace("GYNECOLOGY", "OBSTETRICS AND GYNECOLOGY")
    data = data.groupby(
        ["category", "sub_category", "Number of stays"], as_index=False, dropna=False
    ).agg({"Number of stays": "sum"})
    if category:
        data = data[data.category == category]
        base = (
            alt.Chart(data)
            .transform_aggregate(
                sum_stays="sum(Number of stays)", groupby=["sub_category"]
            )
            .transform_joinaggregate(
                sum_stays_per_cat="sum(sum_stays)", groupby=["sub_category"]
            )
            .transform_joinaggregate(total_stays="sum(sum_stays)")
            .transform_calculate(precentage=alt.datum.sum_stays / alt.datum.total_stays)
            .encode(
                y=alt.Y("sum(sum_stays):Q", title="Number of hospitalizations"),
                x=alt.X(
                    "sub_category:N",
                    title="Speciality",
                    sort={
                        "field": "sum_stays_per_cat",
                        "op": "max",
                        "order": "descending",
                    },
                    axis=alt.Axis(labelAngle=-45, grid=True, labelLimit=300),
                ),
            )
        )
        bars = (
            base.mark_bar()
            .encode(
                y=alt.Y("sum_stays:Q", title="Number of hospitalizations"),
                tooltip=alt.Tooltip("sum(sum_stays):Q"),
            )
            .properties(
                width=750,
                title="Number of hospitalizations for VTE after {} admission per speciality of {}".format(
                    title_entree, category.lower()
                ),
            )
        )
        text = base.mark_text(
            align="center",
            baseline="middle",
            dy=-10,
            color="black",
        ).encode(
            y=alt.Y("sum(sum_stays):Q", title=""),
            text=alt.Text(
                "max(precentage):Q",
                format=".1%",
            ),
        )
        chart = bars + text
    else:
        base = (
            alt.Chart(data)
            .transform_aggregate(sum_stays="sum(Number of stays)", groupby=["category"])
            .transform_joinaggregate(
                sum_stays_per_cat="sum(sum_stays)", groupby=["category"]
            )
            .transform_joinaggregate(total_stays="sum(sum_stays)")
            .transform_calculate(precentage=alt.datum.sum_stays / alt.datum.total_stays)
            .encode(
                x=alt.X(
                    "category:N",
                    title="Department",
                    sort={
                        "field": "sum_stays_per_cat",
                        "op": "max",
                        "order": "descending",
                    },
                    axis=alt.Axis(labelAngle=-45, grid=True, labelLimit=300),
                ),
                y=alt.Y("sum(sum_stays):Q", title="Number of hospitalizations"),
            )
        )
        bars = (
            base.mark_bar()
            .encode(
                tooltip=alt.Tooltip("sum(sum_stays):Q"),
            )
            .properties(
                width=750,
                # title="Number of hospitalizations for VTE after {} admission per department".format(
                #     title_entree
                # ),
            )
        )
        text = base.mark_text(
            align="center", baseline="middle", dy=-10, color="black", fontSize=16
        ).encode(
            text=alt.Text(
                "max(precentage):Q",
                format=".1%",
            ),
        )
        chart = bars + text
    return (
        chart.configure_axis(labelFontSize=16, titleFontSize=17)
        .configure_legend(labelFontSize=16, titleFontSize=17, orient="bottom")
        .configure_title(fontSize=18)
        .configure_view(strokeWidth=0),
        data,
    )


def viz_histogram(
    data,
    diagnosis: str = "VTE",
    category: str = None,
    mode_entree: str = "2-URG",
    type_visite: str = "I",
    remove_unknown: bool = True,
    remove_urgence: bool = True,
    urgence_first: bool = False,
):
    alt.data_transformers.disable_max_rows()

    # Diagnosis
    data = data[data["visit_{}".format(diagnosis)] == 1]

    # Mode d'entrée
    if mode_entree:
        data = data[data.mode_entree == mode_entree]

    # Type visite
    if type_visite:
        data = data[data.type_visite == type_visite]

    # urgence_first
    first = "_first" if urgence_first else ""

    # MTEV acquis/urgence
    data["type_MTEV"] = "Unknown"
    data["type_MTEV"] = data["type_MTEV"].mask(
        (data["visit_{}_per_cat".format(diagnosis)] == 1),
        "Coded in the department",
    )
    data["type_MTEV"] = data["type_MTEV"].mask(
        (data["visit_{}_urgence{}".format(diagnosis, first)] == 1),
        "Coded in an emergency department",
    )
    data["type_MTEV"] = data["type_MTEV"].mask(
        (data["visit_{}_urgence{}".format(diagnosis, first)] == 0)
        & (data["visit_{}_per_cat".format(diagnosis)] == 0),
        "Coded in an other department",
    )

    # Remove NOT SPECIFIED
    if remove_unknown:
        data = data[~(data.category == "NOT SPECIFIED")]
        data = data[~(data.category == "PEDIATRICS")]
        data = data[~(data.category.isna())]

    # Remove Urgence
    if remove_urgence:
        data = data[~(data.category == "EMERGENCY")]

    data = data.groupby(
        ["category", "sub_category", "type_MTEV"], as_index=False, dropna=False
    ).agg({"Number of stays": "sum"})
    if category:
        data = data[data.category == category]
        base = (
            alt.Chart(data)
            .transform_aggregate(
                sum_stays="sum(Number of stays)", groupby=["sub_category", "type_MTEV"]
            )
            .transform_joinaggregate(
                sum_stays_per_cat="sum(sum_stays)", groupby=["sub_category"]
            )
            .transform_joinaggregate(total_stays="sum(sum_stays)")
            .transform_calculate(precentage=alt.datum.sum_stays / alt.datum.total_stays)
            .encode(
                y=alt.Y("sum(sum_stays):Q", title="Number of administrative records"),
                x=alt.X(
                    "sub_category:N",
                    title="Speciality",
                    sort={
                        "field": "sum_stays_per_cat",
                        "op": "max",
                        "order": "descending",
                    },
                    axis=alt.Axis(labelAngle=-45, grid=True, labelLimit=300),
                ),
            )
        )
        bars = (
            base.mark_bar()
            .encode(
                y=alt.Y("sum_stays:Q", title="Number of administrative records"),
                order=alt.Order("type_MTEV:N"),
                tooltip=alt.Tooltip("sum(sum_stays):Q"),
                color=alt.Color(
                    "type_MTEV:N",
                    legend=alt.Legend(title=None, labelLimit=300),
                    sort=[
                        "Coded in the department",
                        "Coded in an other department",
                        "Coded in an emergency department",
                    ],
                ),
            )
            .properties(
                width=750,
                title="Number of administrative records per speciality of {}".format(
                    category.lower()
                ),
            )
        )
        text = base.mark_text(
            align="center",
            baseline="middle",
            dy=-10,
            color="black",
        ).encode(
            y=alt.Y("sum(sum_stays):Q", title=""),
            text=alt.Text(
                "max(precentage):Q",
                format=".1%",
            ),
        )
        chart = bars + text
    else:
        base = (
            alt.Chart(data)
            .transform_aggregate(
                sum_stays="sum(Number of stays)", groupby=["category", "type_MTEV"]
            )
            .transform_joinaggregate(
                sum_stays_per_cat="sum(sum_stays)", groupby=["category"]
            )
            .transform_joinaggregate(total_stays="sum(sum_stays)")
            .transform_calculate(precentage=alt.datum.sum_stays / alt.datum.total_stays)
            .encode(
                x=alt.X(
                    "category:N",
                    title="Department",
                    sort={
                        "field": "sum_stays_per_cat",
                        "op": "max",
                        "order": "descending",
                    },
                    axis=alt.Axis(labelAngle=-45, grid=True),
                ),
                y=alt.Y("sum(sum_stays):Q", title=""),
            )
        )
        bars = (
            base.mark_bar()
            .encode(
                order=alt.Order("type_MTEV:N"),
                tooltip=alt.Tooltip("sum(sum_stays):Q"),
                color=alt.Color(
                    "type_MTEV:N",
                    legend=alt.Legend(title=None, labelLimit=300),
                    sort=[
                        "Coded in the department",
                        "Coded in an other department",
                        "Coded in an emergency department",
                    ],
                ),
            )
            .properties(
                width=750,
                title="Number of administrative records per service",
            )
        )
        text = base.mark_text(
            align="center", baseline="middle", dy=-10, color="black", fontSize=16
        ).encode(
            text=alt.Text(
                "max(precentage):Q",
                format=".1%",
            ),
        )
        chart = bars + text
    return (
        chart.configure_axis(labelFontSize=16, titleFontSize=17)
        .configure_legend(labelFontSize=16, titleFontSize=17, orient="bottom")
        .configure_title(fontSize=18)
        .configure_view(strokeWidth=0),
        data,
    )

from typing import List

import pandas as pd
import plotly.graph_objects as go
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window


def pivot_table(visit_detail: DataFrame):
    last_visit = Window.partitionBy(visit_detail.visit_id).orderBy(
        visit_detail.visit_detail_start_date.desc()
    )
    window_visit = Window.partitionBy(visit_detail.visit_id).orderBy(
        visit_detail.visit_detail_start_date
    )
    visit_detail = visit_detail.withColumn(
        "final_activity", F.first("activity").over(last_visit)
    )
    visit_detail = visit_detail.withColumn(
        "visit_rank", F.row_number().over(window_visit)
    ).drop("visit_detail_start_date")

    sankey_data = (
        visit_detail.groupBy(
            "visit_id",
            "patient_id",
            "visit_VTE",
            "visit_DVT",
            "visit_DVT_without_superficial",
            "visit_PE",
            "visit_DVT_lower_limb",
            "visit_DVT_lower_limb_without_superficial",
            "visit_DVT_nonspecific_location",
            "visit_DVT_portal_vein",
            "visit_DVT_cerebral",
            "visit_cancer",
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
            "type_visite",
            "mode_entree",
            "mode_sortie",
            "length_of_stay",
            "gender",
            "vital_status",
            "age",
            "final_activity",
            "visit_rank",
        )
        .agg(F.first("category").alias("category"))
        .groupBy(
            "visit_id",
            "patient_id",
            "visit_VTE",
            "visit_DVT",
            "visit_DVT_without_superficial",
            "visit_PE",
            "visit_DVT_lower_limb",
            "visit_DVT_lower_limb_without_superficial",
            "visit_DVT_nonspecific_location",
            "visit_DVT_portal_vein",
            "visit_DVT_cerebral",
            "visit_cancer",
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
            "type_visite",
            "mode_entree",
            "mode_sortie",
            "length_of_stay",
            "gender",
            "vital_status",
            "age",
            "final_activity",
        )
        .pivot("visit_rank")
        .agg(F.first("category"))
        .toPandas()
    )

    sankey_data["visit_DVT"] = sankey_data["visit_DVT"].where(
        sankey_data["visit_PE"] == 0, 0
    )
    sankey_data["visit_DVT_without_superficial"] = sankey_data[
        "visit_DVT_without_superficial"
    ].where(sankey_data["visit_PE"] == 0, 0)
    sankey_data["visit_DVT_lower_limb"] = sankey_data["visit_DVT_lower_limb"].where(
        sankey_data["visit_PE"] == 0, 0
    )
    sankey_data["visit_DVT_lower_limb_without_superficial"] = sankey_data[
        "visit_DVT_lower_limb_without_superficial"
    ].where(sankey_data["visit_PE"] == 0, 0)
    sankey_data["visit_DVT_nonspecific_location"] = sankey_data[
        "visit_DVT_nonspecific_location"
    ].where(sankey_data["visit_PE"] == 0, 0)
    sankey_data["visit_DVT_portal_vein"] = sankey_data["visit_DVT_portal_vein"].where(
        sankey_data["visit_PE"] == 0, 0
    )
    sankey_data["visit_DVT_cerebral"] = sankey_data["visit_DVT_cerebral"].where(
        sankey_data["visit_PE"] == 0, 0
    )
    sankey_data["visit_DVT_other_location"] = sankey_data["visit_VTE"].where(
        (sankey_data["visit_PE"] == 0) & (sankey_data["visit_DVT_lower_limb"] == 0), 0
    )
    sankey_data["visit_DVT_urgence"] = sankey_data["visit_DVT_urgence"].where(
        sankey_data["visit_PE_urgence"] == 0, 0
    )
    sankey_data["visit_DVT_without_superficial_urgence"] = sankey_data[
        "visit_DVT_without_superficial_urgence"
    ].where(sankey_data["visit_PE_urgence"] == 0, 0)
    sankey_data["visit_DVT_lower_limb_urgence"] = sankey_data[
        "visit_DVT_lower_limb_urgence"
    ].where(sankey_data["visit_PE_urgence"] == 0, 0)
    sankey_data["visit_DVT_lower_limb_without_superficial_urgence"] = sankey_data[
        "visit_DVT_lower_limb_without_superficial_urgence"
    ].where(sankey_data["visit_PE_urgence"] == 0, 0)
    sankey_data["visit_DVT_nonspecific_location_urgence"] = sankey_data[
        "visit_DVT_nonspecific_location_urgence"
    ].where(sankey_data["visit_PE_urgence"] == 0, 0)
    sankey_data["visit_DVT_portal_vein_urgence"] = sankey_data[
        "visit_DVT_portal_vein_urgence"
    ].where(sankey_data["visit_PE_urgence"] == 0, 0)
    sankey_data["visit_DVT_cerebral_urgence"] = sankey_data[
        "visit_DVT_cerebral_urgence"
    ].where(sankey_data["visit_PE_urgence"] == 0, 0)
    sankey_data["visit_DVT_other_location_urgence"] = sankey_data[
        "visit_VTE_urgence"
    ].where(
        (sankey_data["visit_PE_urgence"] == 0)
        & (sankey_data["visit_DVT_lower_limb_urgence"] == 0),
        0,
    )
    sankey_data["visit_DVT_urgence_first"] = sankey_data[
        "visit_DVT_urgence_first"
    ].where(sankey_data["visit_PE_urgence_first"] == 0, 0)
    sankey_data["visit_DVT_without_superficial_urgence_first"] = sankey_data[
        "visit_DVT_without_superficial_urgence_first"
    ].where(sankey_data["visit_PE_urgence_first"] == 0, 0)
    sankey_data["visit_DVT_lower_limb_urgence_first"] = sankey_data[
        "visit_DVT_lower_limb_urgence_first"
    ].where(sankey_data["visit_PE_urgence_first"] == 0, 0)
    sankey_data["visit_DVT_lower_limb_without_superficial_urgence_first"] = sankey_data[
        "visit_DVT_lower_limb_without_superficial_urgence_first"
    ].where(sankey_data["visit_PE_urgence_first"] == 0, 0)
    sankey_data["visit_DVT_nonspecific_location_urgence_first"] = sankey_data[
        "visit_DVT_nonspecific_location_urgence_first"
    ].where(sankey_data["visit_PE_urgence_first"] == 0, 0)
    sankey_data["visit_DVT_portal_vein_urgence_first"] = sankey_data[
        "visit_DVT_portal_vein_urgence_first"
    ].where(sankey_data["visit_PE_urgence_first"] == 0, 0)
    sankey_data["visit_DVT_cerebral_urgence_first"] = sankey_data[
        "visit_DVT_cerebral_urgence_first"
    ].where(sankey_data["visit_PE_urgence_first"] == 0, 0)
    sankey_data["visit_DVT_other_location_urgence_first"] = sankey_data[
        "visit_VTE_urgence_first"
    ].where(
        (sankey_data["visit_PE_urgence_first"] == 0)
        & (sankey_data["visit_DVT_lower_limb_urgence_first"] == 0),
        0,
    )
    return sankey_data


def viz_sankey_plot(
    sankey_data: DataFrame,
    type_visit: str = "I",
    diagnosis: str = "VTE",
    mode_entree: str = "2-URG",
    max_step: int = 3,
    diagnosed_in_emergency: bool = True,
    emergency_first: bool = False,
    summarize_last_step: bool = True,
    with_cancer: bool = False,
    length_of_stay: float = None,
):
    data = sankey_data.copy()
    # Aggregate final status and final activity
    data["final_status"] = "OTHER DISCHARGE"
    data["final_status"] = data["final_status"].mask(
        data.mode_sortie == "6-DC", "DEATH"
    )
    data["final_status"] = data["final_status"].mask(
        data.mode_sortie == "1-DOMI", "HOME"
    )
    data["final_status"] = data["final_status"].mask(
        data.final_activity == "SSR", "SSR"
    )
    data["final_status"] = data["final_status"].mask(
        data.final_activity == "PSY", "PSY"
    )

    data = data.drop(columns=["final_activity", "patient_id"])

    # Type visite
    if type_visit:
        data = data[data.type_visite == type_visit]
    data = data.drop(columns="type_visite")

    # Mode d'entrée
    if mode_entree:
        data = data[data.mode_entree == mode_entree]
    data = data.drop(columns="mode_entree")

    # Urgence
    if diagnosed_in_emergency:
        first = "_first" if emergency_first else ""
        data = data[data["visit_{}_urgence{}".format(diagnosis, first)] == 1]
        data["1"] = "EMERGENCY"
        data["2"] = data["2"].mask(
            (data["1"] == "EMERGENCY") & (data["2"] == "EMERGENCY"), data["3"]
        )
    data["2"] = data["2"].mask(
        data["2"].isin(["RHEUMATOLOGY", "GYNECOLOGY", "NEPHROLOGY"]), "OTHER"
    )
    # Diagnosis
    data = data[data["visit_{}".format(diagnosis)] == 1]

    # Type visite
    if with_cancer:
        data = data[data.visit_cancer == 1]

    # Add final status as final step
    def replace_final_step(row):
        n_steps = data.shape[1] - 38
        status = False
        for i in range(n_steps):
            col = n_steps - i
            if row[str(col)] is not None and not status:
                row[str(col + 1)] = row["final_status"]
                status = True
            elif row[str(col)] is None and status:
                row[str(col)] = "OTHER"
            elif col == 1 and not status:
                row[str(col)] = "OTHER"
        return row

    data = data.apply(replace_final_step, axis=1)

    # Summarize steps
    max_step = str(max_step)
    if summarize_last_step:
        data[max_step] = data[max_step].where(
            data[max_step].isin(
                ["HOME", "DEATH", "SSR", "PSY", "OTHER DISCHARGE", None]
            ),
            "OTHER DEPARTMENT",
        )

    # Rename some category
    for step in range(int(max_step)):
        step = str(step + 1)
        data[step] = data[step].replace(" PNEUMOLOGY", "PULMONOLOGY")
        data[step] = data[step].replace("SSR", "REHABILITATION")

    # Length of stay <= 2
    if length_of_stay:
        data["2"] = data["2"].mask(
            (data.length_of_stay <= length_of_stay) & (data["2"] != "HOME"),
            "HOSPIT -48H",
        )
        data["3"] = data["3"].mask(data.length_of_stay <= length_of_stay, None)

    df = data[
        [
            "visit_id",
        ].copy()
        + data.columns[
            (data.columns.str.extract("(\d+[.\d]*)").astype(float) <= int(max_step))[0]
        ].to_list()
    ].copy()

    for column in df.columns[1:]:
        df[column] = df[column] + "_" + column

    steps = list(df.columns[1:])
    colors = [
        "rgba(78, 121, 167, 0.5)",
        "rgba(242, 142, 43, 0.5)",
        "rgba(225, 87, 89, 0.5)",
        "rgba(118, 183, 178, 0.5)",
        "rgba(89, 161, 79, 0.5)",
        "rgba(237, 201, 72, 0.5)",
        "rgba(176, 122, 161, 0.5)",
        "rgba(255, 157, 167, 0.5)",
        "rgba(156, 117, 95, 0.5)",
        "rgba(186, 176, 172, 0.5)",
        "rgba(186, 176, 172, 0.5)",
        "rgba(186, 176, 172, 0.5)",
        "rgba(186, 176, 172, 0.5)",
        "rgba(186, 176, 172, 0.5)",
        "rgba(186, 176, 172, 0.5)",
        "rgba(186, 176, 172, 0.5)",
        "rgba(186, 176, 172, 0.5)",
        "rgba(186, 176, 172, 0.5)",
        "rgba(186, 176, 172, 0.5)",
        "rgba(186, 176, 172, 0.5)",
        "rgba(186, 176, 172, 0.5)",
    ]
    nodes_dict = dict(node=[], color=[], x=[], y=[])

    for i, step in enumerate(steps):
        items = list(df[step].dropna().unique())
        nodes_dict["node"].extend(items)
        nodes_dict["color"].extend(colors[: len(items)])
        nodes_dict["x"].extend([0.25 if i == 1 else i / (len(steps) - 1)] * len(items))
        nodes_dict["y"].extend([0.1] * len(items))

    nodes = pd.DataFrame(nodes_dict)
    nodes["index"] = range(len(nodes))
    dfs = []
    for s1, s2 in zip(steps[:-1], steps[1:]):
        df_step = df[["visit_id", s1, s2]].groupby([s1, s2], as_index=False).count()
        df_step.columns = ["node", "target", "value"]
        df_step = df_step.merge(nodes[["color", "node"]], on="node")
        df_step.columns = ["source", "target", "value", "color"]

        dfs.append(df_step)

    dfsankey = pd.concat(dfs).replace(
        {row.node: row["index"] for _, row in nodes.iterrows()}
    )
    nodes.node = nodes.node.str.split("_").str[0]

    fig = go.Figure(
        data=[
            go.Sankey(
                arrangement="snap",
                node=dict(
                    pad=14,
                    thickness=50,
                    line=dict(color="black", width=0.5),
                    label=nodes.node,
                    color=nodes.color,
                    x=nodes.x,
                    y=nodes.y,
                ),
                link=dict(
                    source=dfsankey.source,
                    target=dfsankey.target,
                    value=dfsankey.value,
                    color=dfsankey.color,
                ),
            )
        ]
    )
    return fig

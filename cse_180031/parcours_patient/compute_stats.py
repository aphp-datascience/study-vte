import numpy as np
import pandas as pd


def compute_stats_per_cat_from_sankey_emergency(sankey_data: pd.DataFrame):
    stats = {}
    data = (
        sankey_data[
            (sankey_data.mode_entree == "2-URG") & (sankey_data.type_visite == "I")
        ]
        .drop(columns="type_visite")
        .drop(columns="mode_entree")
        .copy()
    )
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

    # Urgence
    data = data[data["visit_VTE_urgence"] == 1]
    data["1"] = "EMERGENCY"
    data["2"] = data["2"].mask(
        (data["1"] == "EMERGENCY") & (data["2"] == "EMERGENCY"), data["3"]
    )
    data["2"] = data["2"].mask(
        data["2"].isin(["RHEUMATOLOGY", "GYNECOLOGY", "NEPHROLOGY"]), "OTHER"
    )
    # Diagnosis
    data = data[data["visit_VTE"] == 1]

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

    # Rename some category
    for step in range(3):
        step = str(step + 1)
        data[step] = data[step].replace(" PNEUMOLOGY", "PULMONOLOGY")

    data_emergency = data.copy()
    data = data[
        data["2"].isin(
            [
                "INTERNAL MEDICINE",
                "CARDIOVASCULAR",
                "INTENSIVE CARE",
                "HOME",
                "GASTROENTEROLOGY",
                "PULMONOLOGY",
                "GERIATRICS",
                "SURGERY",
            ]
        )
    ]
    data_emergency["2"] = data_emergency["1"]
    data = pd.concat([data, data_emergency])

    # Number of stays
    total = (
        data.groupby(["2"])[
            [
                "visit_VTE_urgence",
                "visit_PE_urgence",
                "visit_DVT_lower_limb_urgence",
                "visit_DVT_other_location_urgence",
            ]
        ]
        .sum()
        .rename(
            columns={
                "visit_VTE_urgence": "VTE",
                "visit_PE_urgence": "PE (+/- DVT)",
                "visit_DVT_lower_limb_urgence": "Lower limb DVT",
                "visit_DVT_other_location_urgence": "DVT other location",
            },
        )
    ).T
    total.columns.name = None
    total = pd.concat({"": total})
    stats["Total (no. of stays)"] = total

    # Number of cancer patients
    cancer_patients = data[data.visit_cancer == 1].copy()
    cancer_patients = (
        cancer_patients.groupby(["2"])[
            [
                "visit_VTE_urgence",
                "visit_PE_urgence",
                "visit_DVT_lower_limb_urgence",
                "visit_DVT_other_location_urgence",
            ]
        ]
        .sum()
        .rename(
            columns={
                "visit_VTE_urgence": "VTE",
                "visit_PE_urgence": "PE (+/- DVT)",
                "visit_DVT_lower_limb_urgence": "Lower limb DVT",
                "visit_DVT_other_location_urgence": "DVT other location",
            },
        )
    ).T
    cancer_patients.columns.name = None
    cancer_patients = pd.concat({"": cancer_patients})
    stats["With cancer (no. of stays)"] = cancer_patients

    # SEX RATIO
    gender = (
        data.groupby(["2", "gender"])[
            [
                "visit_VTE_urgence",
                "visit_PE_urgence",
                "visit_DVT_lower_limb_urgence",
                "visit_DVT_other_location_urgence",
            ]
        ]
        .sum()
        .rename(
            columns={
                "visit_VTE_urgence": "VTE",
                "visit_PE_urgence": "PE (+/- DVT)",
                "visit_DVT_lower_limb_urgence": "Lower limb DVT",
                "visit_DVT_other_location_urgence": "DVT other location",
            },
            # index={0: "Total"},
        )
        .copy()
    ).swaplevel()
    ratio = round(gender.loc["M"] / gender.loc["W"], 2)
    gender = (
        ratio.astype(str)
        + " ("
        + gender.loc["M"].astype(str)
        + "/"
        + gender.loc["W"].astype(str)
        + ")"
    ).T
    gender.columns.name = None
    gender = pd.concat({"Ratio (men/women)": gender})
    stats["Gender (no. of stays)"] = gender

    # AGE AT VISIT (YEARS)
    age_raw = data.copy()
    age_stats = []
    for cat in data["2"].unique():
        age = age_raw[age_raw["2"] == cat]
        age_vte = age[age.visit_VTE_urgence == 1]
        vte_mean = (
            str(round(age_vte.age.mean(), 2))
            + " ("
            + str(round(age_vte.age.std(), 2))
            + ")"
        )
        q75, q25 = np.percentile(age_vte["age"], [75, 25])
        vte_median = (
            str(round(age_vte.age.median()))
            + " ["
            + str(round(q25))
            + "-"
            + str(round(q75))
            + "]"
        )
        age_pe = age[age.visit_PE == 1]
        pe_mean = (
            str(round(age_pe.age.mean(), 2))
            + " ("
            + str(round(age_pe.age.std(), 2))
            + ")"
        )
        q75, q25 = np.percentile(age_pe["age"], [75, 25])
        pe_median = (
            str(round(age_pe.age.median()))
            + " ["
            + str(round(q25))
            + "-"
            + str(round(q75))
            + "]"
        )
        age_pe = age[age.visit_PE_urgence == 1]
        pe_mean = (
            str(round(age_pe.age.mean(), 2))
            + " ("
            + str(round(age_pe.age.std(), 2))
            + ")"
        )
        q75, q25 = np.percentile(age_pe["age"], [75, 25])
        pe_median = (
            str(round(age_pe.age.median()))
            + " ["
            + str(round(q25))
            + "-"
            + str(round(q75))
            + "]"
        )
        age_dvt_lower_limb = age[age.visit_DVT_lower_limb_urgence == 1]
        dvt_lower_limb_mean = (
            str(round(age_dvt_lower_limb.age.mean(), 2))
            + " ("
            + str(round(age_dvt_lower_limb.age.std(), 2))
            + ")"
        )
        q75, q25 = np.percentile(age_dvt_lower_limb["age"], [75, 25])
        dvt_lower_limb_median = (
            str(round(age_dvt_lower_limb.age.median()))
            + " ["
            + str(round(q25))
            + "-"
            + str(round(q75))
            + "]"
        )
        age_dvt_other_location = age[age.visit_DVT_other_location_urgence == 1]
        dvt_other_location_mean = (
            str(round(age_dvt_other_location.age.mean(), 2))
            + " ("
            + str(round(age_dvt_other_location.age.std(), 2))
            + ")"
        )
        q75, q25 = np.percentile(age_dvt_other_location["age"], [75, 25])
        dvt_other_location_median = (
            str(round(age_dvt_other_location.age.median()))
            + " ["
            + str(round(q25))
            + "-"
            + str(round(q75))
            + "]"
        )
        age_at_visit = pd.DataFrame(
            [
                vte_median,
                vte_mean,
                pe_median,
                pe_mean,
                dvt_lower_limb_median,
                dvt_lower_limb_mean,
                dvt_other_location_median,
                dvt_other_location_mean,
            ],
            index=pd.MultiIndex.from_product(
                [
                    ["VTE", "PE (+/- DVT)", "Lower limb DVT", "DVT other location"],
                    ["Median[IQR]", "Mean(sd)"],
                ]
            ),
            columns=[cat],
        )
        age_stats.append(age_at_visit)
    age_stats = pd.concat(age_stats, axis=1)
    stats["Age at visit (years)"] = age_stats

    # LENGTH OF STAY (DAYS)
    length_raw = data.copy()
    length_stats = []
    for cat in data["2"].unique():
        length = length_raw[length_raw["2"] == cat]

        length_vte = length[length.visit_VTE_urgence == 1]
        vte_mean = (
            str(round(length_vte.length_of_stay.mean(), 2))
            + " ("
            + str(round(length_vte.length_of_stay.std(), 2))
            + ")"
        )
        q75, q25 = np.nanpercentile(length_vte.length_of_stay, [75, 25])
        vte_median = (
            str(round(length_vte.length_of_stay.median()))
            + " ["
            + str(round(q25))
            + "-"
            + str(round(q75))
            + "]"
        )
        length_pe = length[length.visit_PE_urgence == 1]
        pe_mean = (
            str(round(length_pe.length_of_stay.mean(), 2))
            + " ("
            + str(round(length_pe.length_of_stay.std(), 2))
            + ")"
        )
        q75, q25 = np.nanpercentile(length_pe.length_of_stay, [75, 25])
        pe_median = (
            str(round(length_pe.length_of_stay.median()))
            + " ["
            + str(round(q25))
            + "-"
            + str(round(q75))
            + "]"
        )
        length_dvt_lower_limb = length[length.visit_DVT_lower_limb_urgence == 1]
        dvt_lower_limb_mean = (
            str(round(length_dvt_lower_limb.length_of_stay.mean(), 2))
            + " ("
            + str(round(length_dvt_lower_limb.length_of_stay.std(), 2))
            + ")"
        )
        q75, q25 = np.nanpercentile(length_dvt_lower_limb.length_of_stay, [75, 25])
        dvt_lower_limb_median = (
            str(round(length_dvt_lower_limb.length_of_stay.median()))
            + " ["
            + str(round(q25))
            + "-"
            + str(round(q75))
            + "]"
        )
        length_dvt_other_location = length[length.visit_DVT_other_location_urgence == 1]
        dvt_other_location_mean = (
            str(round(length_dvt_other_location.length_of_stay.mean(), 2))
            + " ("
            + str(round(length_dvt_other_location.length_of_stay.std(), 2))
            + ")"
        )
        q75, q25 = np.nanpercentile(length_dvt_other_location.length_of_stay, [75, 25])
        dvt_other_location_median = (
            str(round(length_dvt_other_location.length_of_stay.median()))
            + " ["
            + str(round(q25))
            + "-"
            + str(round(q75))
            + "]"
        )
        length_of_stay = pd.DataFrame(
            [
                vte_median,
                vte_mean,
                pe_median,
                pe_mean,
                dvt_lower_limb_median,
                dvt_lower_limb_mean,
                dvt_other_location_median,
                dvt_other_location_mean,
            ],
            index=pd.MultiIndex.from_product(
                [
                    ["VTE", "PE (+/- DVT)", "Lower limb DVT", "DVT other location"],
                    ["Median[IQR]", "Mean(sd)"],
                ]
            ),
            columns=[cat],
        )
        length_stats.append(length_of_stay)
    length_stats = pd.concat(length_stats, axis=1)
    stats["Length of stay (days)"] = length_stats

    # DURATION OF STAY (no stays)
    length_raw = data.copy()
    length_stats = []
    for cat in data["2"].unique():
        length = length_raw[length_raw["2"] == cat]

        length_vte = length[length.visit_VTE_urgence == 1]
        total_vte = int(length_vte.visit_id.nunique())
        vte_24h = int(length_vte[length_vte.length_of_stay < 1.0].visit_id.nunique())
        vte_24h = str(
            str(vte_24h) + " (" + str(round(vte_24h / total_vte * 100, 2)) + "%)"
        )
        vte_24h_48h = int(
            length_vte[
                (length_vte.length_of_stay >= 1.0) & (length_vte.length_of_stay <= 2.0)
            ].visit_id.nunique()
        )
        vte_24h_48h = str(
            str(vte_24h_48h)
            + " ("
            + str(round(vte_24h_48h / total_vte * 100, 2))
            + "%)"
        )
        vte_48h = int(length_vte[length_vte.length_of_stay > 2.0].visit_id.nunique())
        vte_48h = str(
            str(vte_48h) + " (" + str(round(vte_48h / total_vte * 100, 2)) + "%)"
        )

        length_pe = length[length.visit_PE_urgence == 1]
        total_pe = int(length_pe.visit_id.nunique())
        pe_24h = int(length_pe[length_pe.length_of_stay < 1.0].visit_id.nunique())
        pe_24h = str(str(pe_24h) + " (" + str(round(pe_24h / total_pe * 100, 2)) + "%)")
        pe_24h_48h = int(
            length_pe[
                (length_pe.length_of_stay >= 1.0) & (length_pe.length_of_stay <= 2.0)
            ].visit_id.nunique()
        )
        pe_24h_48h = str(
            str(pe_24h_48h) + " (" + str(round(pe_24h_48h / total_pe * 100, 2)) + "%)"
        )
        pe_48h = int(length_pe[length_pe.length_of_stay > 2.0].visit_id.nunique())
        pe_48h = str(str(pe_48h) + " (" + str(round(pe_48h / total_pe * 100, 2)) + "%)")

        length_dvt_lower_limb = length[length.visit_DVT_lower_limb_urgence == 1]
        total_dvt_lower_limb = int(length_dvt_lower_limb.visit_id.nunique())
        dvt_lower_limb_24h = int(
            length_dvt_lower_limb[
                length_dvt_lower_limb.length_of_stay < 1.0
            ].visit_id.nunique()
        )
        dvt_lower_limb_24h = str(
            str(dvt_lower_limb_24h)
            + " ("
            + str(round(dvt_lower_limb_24h / total_dvt_lower_limb * 100, 2))
            + "%)"
        )
        dvt_lower_limb_24h_48h = int(
            length_dvt_lower_limb[
                (length_dvt_lower_limb.length_of_stay >= 1.0)
                & (length_dvt_lower_limb.length_of_stay <= 2.0)
            ].visit_id.nunique()
        )
        dvt_lower_limb_24h_48h = str(
            str(dvt_lower_limb_24h_48h)
            + " ("
            + str(round(dvt_lower_limb_24h_48h / total_dvt_lower_limb * 100, 2))
            + "%)"
        )
        dvt_lower_limb_48h = int(
            length_dvt_lower_limb[
                length_dvt_lower_limb.length_of_stay > 2.0
            ].visit_id.nunique()
        )
        dvt_lower_limb_48h = str(
            str(dvt_lower_limb_48h)
            + " ("
            + str(round(dvt_lower_limb_48h / total_dvt_lower_limb * 100, 2))
            + "%)"
        )

        length_dvt_other_location = length[length.visit_DVT_other_location_urgence == 1]
        total_dvt_other_location = int(length_dvt_other_location.visit_id.nunique())
        dvt_other_location_24h = int(
            length_dvt_other_location[
                length_dvt_other_location.length_of_stay < 1.0
            ].visit_id.nunique()
        )
        dvt_other_location_24h = str(
            str(dvt_other_location_24h)
            + " ("
            + str(round(dvt_other_location_24h / total_dvt_other_location * 100, 2))
            + "%)"
        )
        dvt_other_location_24h_48h = int(
            length_dvt_other_location[
                (length_dvt_other_location.length_of_stay >= 1.0)
                & (length_dvt_other_location.length_of_stay <= 2.0)
            ].visit_id.nunique()
        )
        dvt_other_location_24h_48h = str(
            str(dvt_other_location_24h_48h)
            + " ("
            + str(round(dvt_other_location_24h_48h / total_dvt_other_location * 100, 2))
            + "%)"
        )
        dvt_other_location_48h = int(
            length_dvt_other_location[
                length_dvt_other_location.length_of_stay > 2.0
            ].visit_id.nunique()
        )
        dvt_other_location_48h = str(
            str(dvt_other_location_48h)
            + " ("
            + str(round(dvt_other_location_48h / total_dvt_other_location * 100, 2))
            + "%)"
        )

        length_of_stay = pd.DataFrame(
            [
                vte_24h,
                vte_24h_48h,
                vte_48h,
                pe_24h,
                pe_24h_48h,
                pe_48h,
                dvt_lower_limb_24h,
                dvt_lower_limb_24h_48h,
                dvt_lower_limb_48h,
                dvt_other_location_24h,
                dvt_other_location_24h_48h,
                dvt_other_location_48h,
            ],
            index=pd.MultiIndex.from_product(
                [
                    ["VTE", "PE (+/- DVT)", "Lower limb DVT", "DVT other location"],
                    ["<24h(%)", "24h-48h(%)", ">48h(%)"],
                ]
            ),
            columns=[cat],
        )
        length_stats.append(length_of_stay)
    length_stats = pd.concat(length_stats, axis=1)
    stats["Length of stay (no. of stays)"] = length_stats
    return pd.concat(stats)[
        [
            "EMERGENCY",
            "INTERNAL MEDICINE",
            "CARDIOVASCULAR",
            "INTENSIVE CARE",
            "GASTROENTEROLOGY",
            "SURGERY",
            "GERIATRICS",
            "PULMONOLOGY",
            "HOME",
        ]
    ]


def compute_stats_from_sankey_emergency(sankey_data: pd.DataFrame):
    stats = {}

    # Total
    total = sankey_data[
        (sankey_data.mode_entree == "2-URG") & (sankey_data.type_visite == "I")
    ].copy()

    stats["Total"] = (
        total.groupby(["type_visite"], as_index=False)[
            [
                "visit_VTE_urgence",
                "visit_PE_urgence",
                "visit_DVT_lower_limb_urgence",
                "visit_DVT_other_location_urgence",
            ]
        ]
        .sum()
        .rename(
            columns={
                "visit_VTE_urgence": "VTE",
                "visit_PE_urgence": "PE (+/- DVT)",
                "visit_DVT_lower_limb_urgence": "Lower limb DVT",
                "visit_DVT_other_location_urgence": "DVT other location",
            },
            index={0: "Total"},
        )
        .drop(columns="type_visite")
    )

    # Last known vital status
    vital_status = sankey_data[
        (sankey_data.mode_entree == "2-URG") & (sankey_data.type_visite == "I")
    ].copy()
    vital_status["Last known vital status"] = "Alive"
    vital_status["Last known vital status"] = vital_status[
        "Last known vital status"
    ].mask(vital_status.vital_status == "D", "Dead")

    vital_status = (
        vital_status.groupby(["Last known vital status"])
        .agg(
            {
                "visit_VTE_urgence": "sum",
                "visit_PE_urgence": "sum",
                "visit_DVT_lower_limb_urgence": "sum",
                "visit_DVT_other_location_urgence": "sum",
            }
        )
        .rename(
            columns={
                "visit_VTE_urgence": "VTE",
                "visit_PE_urgence": "PE (+/- DVT)",
                "visit_DVT_lower_limb_urgence": "Lower limb DVT",
                "visit_DVT_other_location_urgence": "DVT other location",
            }
        )
    )
    vital_status = vital_status.apply(
        axis=1,
        func=lambda x: x.astype(str)
        + " ("
        + (x * 100 / stats["Total"].loc["Total"]).map("{:,.1f}".format)
        + " %)",
    )
    stats["Last known vital status"] = vital_status.sort_index()

    # Hospital discharge mode
    hosp_discharge = sankey_data[
        (sankey_data.mode_entree == "2-URG") & (sankey_data.type_visite == "I")
    ].copy()
    hosp_discharge["Hospital discharge mode"] = "Other discharge"
    hosp_discharge["Hospital discharge mode"] = hosp_discharge[
        "Hospital discharge mode"
    ].mask(hosp_discharge.mode_sortie == "6-DC", "Death")
    hosp_discharge["Hospital discharge mode"] = hosp_discharge[
        "Hospital discharge mode"
    ].mask(hosp_discharge.mode_sortie == "1-DOMI", "Home")

    hosp_discharge = (
        hosp_discharge.groupby(["Hospital discharge mode"])
        .agg(
            {
                "visit_VTE_urgence": "sum",
                "visit_PE_urgence": "sum",
                "visit_DVT_lower_limb_urgence": "sum",
                "visit_DVT_other_location_urgence": "sum",
            }
        )
        .rename(
            columns={
                "visit_VTE_urgence": "VTE",
                "visit_PE_urgence": "PE (+/- DVT)",
                "visit_DVT_lower_limb_urgence": "Lower limb DVT",
                "visit_DVT_other_location_urgence": "DVT other location",
            }
        )
    )
    hosp_discharge = hosp_discharge.apply(
        axis=1,
        func=lambda x: x.astype(str)
        + " ("
        + (x * 100 / stats["Total"].loc["Total"]).map("{:,.1f}".format)
        + " %)",
    )
    stats["Hospital discharge mode"] = hosp_discharge.sort_index()

    # SEX RATIO
    gender = sankey_data[
        (sankey_data.mode_entree == "2-URG") & (sankey_data.type_visite == "I")
    ].copy()
    gender = (
        gender.groupby(["gender"])
        .agg(
            {
                "visit_VTE_urgence": "sum",
                "visit_PE_urgence": "sum",
                "visit_DVT_lower_limb_urgence": "sum",
                "visit_DVT_other_location_urgence": "sum",
            }
        )
        .rename(
            columns={
                "visit_VTE_urgence": "VTE",
                "visit_PE_urgence": "PE (+/- DVT)",
                "visit_DVT_lower_limb_urgence": "Lower limb DVT",
                "visit_DVT_other_location_urgence": "DVT other location",
            }
        )
    )
    gender.loc["Ratio"] = round(gender.loc["M"] / gender.loc["W"], 2)
    gender.loc["M"] = gender.loc["M"].astype(int).astype(str)
    gender.loc["W"] = gender.loc["W"].astype(int).astype(str)
    gender.loc["Ratio (men/women)"] = (
        gender.loc["M"]
        + "/"
        + gender.loc["W"]
        + " ("
        + gender.loc["Ratio"].astype(str)
        + ")"
    )
    gender = gender.drop(index=["M", "W", "Ratio"])
    stats["Gender"] = gender

    # LENGTH OF STAY (DAYS)
    length = sankey_data[
        (sankey_data.mode_entree == "2-URG") & (sankey_data.type_visite == "I")
    ].copy()

    length_vte = length[length.visit_VTE_urgence == 1]
    vte_mean = (
        str(round(length_vte.length_of_stay.mean(), 2))
        + " ("
        + str(round(length_vte.length_of_stay.std(), 2))
        + ")"
    )
    q75, q25 = np.nanpercentile(length_vte.length_of_stay, [75, 25])
    vte_median = (
        str(round(length_vte.length_of_stay.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    length_pe = length[length.visit_PE_urgence == 1]
    pe_mean = (
        str(round(length_pe.length_of_stay.mean(), 2))
        + " ("
        + str(round(length_pe.length_of_stay.std(), 2))
        + ")"
    )
    q75, q25 = np.nanpercentile(length_pe.length_of_stay, [75, 25])
    pe_median = (
        str(round(length_pe.length_of_stay.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    length_dvt_lower_limb = length[length.visit_DVT_lower_limb_urgence == 1]
    dvt_lower_limb_mean = (
        str(round(length_dvt_lower_limb.length_of_stay.mean(), 2))
        + " ("
        + str(round(length_dvt_lower_limb.length_of_stay.std(), 2))
        + ")"
    )
    q75, q25 = np.nanpercentile(length_dvt_lower_limb.length_of_stay, [75, 25])
    dvt_lower_limb_median = (
        str(round(length_dvt_lower_limb.length_of_stay.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    length_dvt_other_location = length[length.visit_DVT_other_location_urgence == 1]
    dvt_other_location_mean = (
        str(round(length_dvt_other_location.length_of_stay.mean(), 2))
        + " ("
        + str(round(length_dvt_other_location.length_of_stay.std(), 2))
        + ")"
    )
    q75, q25 = np.nanpercentile(length_dvt_other_location.length_of_stay, [75, 25])
    dvt_other_location_median = (
        str(round(length_dvt_other_location.length_of_stay.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )

    length_of_stay = pd.DataFrame(
        {
            "VTE": [vte_median, vte_mean],
            "PE (+/- DVT)": [pe_median, pe_mean],
            "Lower limb DVT": [dvt_lower_limb_median, dvt_lower_limb_mean],
            "DVT other location": [
                dvt_other_location_median,
                dvt_other_location_mean,
            ],
        },
        index=["Median[IQR]", "Mean(sd)"],
    )
    stats["Length of stay (Days)"] = length_of_stay

    # AGE AT VISIT (YEARS)
    age = sankey_data[
        (sankey_data.mode_entree == "2-URG") & (sankey_data.type_visite == "I")
    ].copy()

    age_vte = age[age.visit_VTE_urgence == 1]
    vte_mean = (
        str(round(age_vte.age.mean(), 2))
        + " ("
        + str(round(age_vte.age.std(), 2))
        + ")"
    )
    q75, q25 = np.percentile(age_vte["age"], [75, 25])
    vte_median = (
        str(round(age_vte.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_pe = age[age.visit_PE == 1]
    pe_mean = (
        str(round(age_pe.age.mean(), 2)) + " (" + str(round(age_pe.age.std(), 2)) + ")"
    )
    q75, q25 = np.percentile(age_pe["age"], [75, 25])
    pe_median = (
        str(round(age_pe.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_pe = age[age.visit_PE_urgence == 1]
    pe_mean = (
        str(round(age_pe.age.mean(), 2)) + " (" + str(round(age_pe.age.std(), 2)) + ")"
    )
    q75, q25 = np.percentile(age_pe["age"], [75, 25])
    pe_median = (
        str(round(age_pe.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_dvt_lower_limb = age[age.visit_DVT_lower_limb_urgence == 1]
    dvt_lower_limb_mean = (
        str(round(age_dvt_lower_limb.age.mean(), 2))
        + " ("
        + str(round(age_dvt_lower_limb.age.std(), 2))
        + ")"
    )
    q75, q25 = np.percentile(age_dvt_lower_limb["age"], [75, 25])
    dvt_lower_limb_median = (
        str(round(age_dvt_lower_limb.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_dvt_other_location = age[age.visit_DVT_other_location_urgence == 1]
    dvt_other_location_mean = (
        str(round(age_dvt_other_location.age.mean(), 2))
        + " ("
        + str(round(age_dvt_other_location.age.std(), 2))
        + ")"
    )
    q75, q25 = np.percentile(age_dvt_other_location["age"], [75, 25])
    dvt_other_location_median = (
        str(round(age_dvt_other_location.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_at_visit = pd.DataFrame(
        {
            "VTE": [vte_median, vte_mean],
            "PE (+/- DVT)": [pe_median, pe_mean],
            "Lower limb DVT": [dvt_lower_limb_median, dvt_lower_limb_mean],
            "DVT other location": [
                dvt_other_location_median,
                dvt_other_location_mean,
            ],
        },
        index=["Median[IQR]", "Mean(sd)"],
    )
    stats["Age at visit (Years)"] = age_at_visit

    return pd.concat(stats)


def compute_stats_from_sankey_emergency_supplementary(sankey_data: pd.DataFrame):
    stats = {}

    # Total
    total = sankey_data[
        (sankey_data.mode_entree == "2-URG") & (sankey_data.type_visite == "I")
    ].copy()

    stats["Total"] = (
        total.groupby(["type_visite"], as_index=False)[
            [
                "visit_VTE_urgence",
                "visit_PE_urgence",
                "visit_DVT_urgence",
                "visit_DVT_without_superficial_urgence",
                "visit_DVT_lower_limb_urgence",
                "visit_DVT_lower_limb_without_superficial_urgence",
                "visit_DVT_cerebral_urgence",
                "visit_DVT_portal_vein_urgence",
                "visit_DVT_nonspecific_location_urgence",
            ]
        ]
        .sum()
        .rename(
            columns={
                "visit_VTE_urgence": "VTE",
                "visit_PE_urgence": "PE (+/- DVT)",
                "visit_DVT_urgence": "DVT",
                "visit_DVT_without_superficial_urgence": "DVT without superficial",
                "visit_DVT_lower_limb_urgence": "Lower limb DVT",
                "visit_DVT_lower_limb_without_superficial_urgence": "Lower limb DVT without superficial",
                "visit_DVT_cerebral_urgence": "DVT cerebral",
                "visit_DVT_portal_vein_urgence": "DVT portal vein",
                "visit_DVT_nonspecific_location_urgence": "DVT non specific location",
            },
            index={0: "Total"},
        )
        .drop(columns="type_visite")
    )

    # Last known vital status
    vital_status = sankey_data[
        (sankey_data.mode_entree == "2-URG") & (sankey_data.type_visite == "I")
    ].copy()
    vital_status["Last known vital status"] = "Alive"
    vital_status["Last known vital status"] = vital_status[
        "Last known vital status"
    ].mask(vital_status.vital_status == "D", "Dead")

    vital_status = (
        vital_status.groupby(["Last known vital status"])
        .agg(
            {
                "visit_VTE_urgence": "sum",
                "visit_PE_urgence": "sum",
                "visit_DVT_urgence": "sum",
                "visit_DVT_without_superficial_urgence": "sum",
                "visit_DVT_lower_limb_urgence": "sum",
                "visit_DVT_lower_limb_without_superficial_urgence": "sum",
                "visit_DVT_cerebral_urgence": "sum",
                "visit_DVT_portal_vein_urgence": "sum",
                "visit_DVT_nonspecific_location_urgence": "sum",
            }
        )
        .rename(
            columns={
                "visit_VTE_urgence": "VTE",
                "visit_PE_urgence": "PE (+/- DVT)",
                "visit_DVT_urgence": "DVT",
                "visit_DVT_without_superficial_urgence": "DVT without superficial",
                "visit_DVT_lower_limb_urgence": "Lower limb DVT",
                "visit_DVT_lower_limb_without_superficial_urgence": "Lower limb DVT without superficial",
                "visit_DVT_cerebral_urgence": "DVT cerebral",
                "visit_DVT_portal_vein_urgence": "DVT portal vein",
                "visit_DVT_nonspecific_location_urgence": "DVT non specific location",
            }
        )
    )
    vital_status = vital_status.apply(
        axis=1,
        func=lambda x: x.astype(str)
        + " ("
        + (x * 100 / stats["Total"].loc["Total"]).map("{:,.1f}".format)
        + " %)",
    )
    stats["Last known vital status"] = vital_status.sort_index()

    # Hospital discharge mode
    hosp_discharge = sankey_data[
        (sankey_data.mode_entree == "2-URG") & (sankey_data.type_visite == "I")
    ].copy()
    hosp_discharge["Hospital discharge mode"] = "Other discharge"
    hosp_discharge["Hospital discharge mode"] = hosp_discharge[
        "Hospital discharge mode"
    ].mask(hosp_discharge.mode_sortie == "6-DC", "Death")
    hosp_discharge["Hospital discharge mode"] = hosp_discharge[
        "Hospital discharge mode"
    ].mask(hosp_discharge.mode_sortie == "1-DOMI", "Home")

    hosp_discharge = (
        hosp_discharge.groupby(["Hospital discharge mode"])
        .agg(
            {
                "visit_VTE_urgence": "sum",
                "visit_PE_urgence": "sum",
                "visit_DVT_urgence": "sum",
                "visit_DVT_without_superficial_urgence": "sum",
                "visit_DVT_lower_limb_urgence": "sum",
                "visit_DVT_lower_limb_without_superficial_urgence": "sum",
                "visit_DVT_cerebral_urgence": "sum",
                "visit_DVT_portal_vein_urgence": "sum",
                "visit_DVT_nonspecific_location_urgence": "sum",
            }
        )
        .rename(
            columns={
                "visit_VTE_urgence": "VTE",
                "visit_PE_urgence": "PE (+/- DVT)",
                "visit_DVT_urgence": "DVT",
                "visit_DVT_without_superficial_urgence": "DVT without superficial",
                "visit_DVT_lower_limb_urgence": "Lower limb DVT",
                "visit_DVT_lower_limb_without_superficial_urgence": "Lower limb DVT without superficial",
                "visit_DVT_cerebral_urgence": "DVT cerebral",
                "visit_DVT_portal_vein_urgence": "DVT portal vein",
                "visit_DVT_nonspecific_location_urgence": "DVT non specific location",
            }
        )
    )
    hosp_discharge = hosp_discharge.apply(
        axis=1,
        func=lambda x: x.astype(str)
        + " ("
        + (x * 100 / stats["Total"].loc["Total"]).map("{:,.1f}".format)
        + " %)",
    )
    stats["Hospital discharge mode"] = hosp_discharge.sort_index()

    # SEX RATIO
    gender = sankey_data[
        (sankey_data.mode_entree == "2-URG") & (sankey_data.type_visite == "I")
    ].copy()
    gender = (
        gender.groupby(["gender"])
        .agg(
            {
                "visit_VTE_urgence": "sum",
                "visit_PE_urgence": "sum",
                "visit_DVT_urgence": "sum",
                "visit_DVT_without_superficial_urgence": "sum",
                "visit_DVT_lower_limb_urgence": "sum",
                "visit_DVT_lower_limb_without_superficial_urgence": "sum",
                "visit_DVT_cerebral_urgence": "sum",
                "visit_DVT_portal_vein_urgence": "sum",
                "visit_DVT_nonspecific_location_urgence": "sum",
            }
        )
        .rename(
            columns={
                "visit_VTE_urgence": "VTE",
                "visit_PE_urgence": "PE (+/- DVT)",
                "visit_DVT_urgence": "DVT",
                "visit_DVT_without_superficial_urgence": "DVT without superficial",
                "visit_DVT_lower_limb_urgence": "Lower limb DVT",
                "visit_DVT_lower_limb_without_superficial_urgence": "Lower limb DVT without superficial",
                "visit_DVT_cerebral_urgence": "DVT cerebral",
                "visit_DVT_portal_vein_urgence": "DVT portal vein",
                "visit_DVT_nonspecific_location_urgence": "DVT non specific location",
            }
        )
    )
    gender.loc["Ratio"] = round(gender.loc["M"] / gender.loc["W"], 2)
    gender.loc["M"] = gender.loc["M"].astype(int).astype(str)
    gender.loc["W"] = gender.loc["W"].astype(int).astype(str)
    gender.loc["Ratio (men/women)"] = (
        gender.loc["M"]
        + "/"
        + gender.loc["W"]
        + " ("
        + gender.loc["Ratio"].astype(str)
        + ")"
    )
    gender = gender.drop(index=["M", "W", "Ratio"])
    stats["Gender"] = gender

    # LENGTH OF STAY (DAYS)
    length = sankey_data[
        (sankey_data.mode_entree == "2-URG") & (sankey_data.type_visite == "I")
    ].copy()

    length_vte = length[length.visit_VTE_urgence == 1]
    vte_mean = (
        str(round(length_vte.length_of_stay.mean(), 2))
        + " ("
        + str(round(length_vte.length_of_stay.std(), 2))
        + ")"
    )
    q75, q25 = np.nanpercentile(length_vte.length_of_stay, [75, 25])
    vte_median = (
        str(round(length_vte.length_of_stay.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    length_pe = length[length.visit_PE_urgence == 1]
    pe_mean = (
        str(round(length_pe.length_of_stay.mean(), 2))
        + " ("
        + str(round(length_pe.length_of_stay.std(), 2))
        + ")"
    )
    q75, q25 = np.nanpercentile(length_pe.length_of_stay, [75, 25])
    pe_median = (
        str(round(length_pe.length_of_stay.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    length_dvt = length[length.visit_DVT_urgence == 1]
    dvt_mean = (
        str(round(length_dvt.length_of_stay.mean(), 2))
        + " ("
        + str(round(length_dvt.length_of_stay.std(), 2))
        + ")"
    )
    q75, q25 = np.nanpercentile(length_dvt.length_of_stay, [75, 25])
    dvt_median = (
        str(round(length_dvt.length_of_stay.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    length_dvt_without_superficial = length[
        length.visit_DVT_without_superficial_urgence == 1
    ]
    dvt_without_superficial_mean = (
        str(round(length_dvt_without_superficial.length_of_stay.mean(), 2))
        + " ("
        + str(round(length_dvt_without_superficial.length_of_stay.std(), 2))
        + ")"
    )
    q75, q25 = np.nanpercentile(length_dvt_without_superficial.length_of_stay, [75, 25])
    dvt_without_superficial_median = (
        str(round(length_dvt_without_superficial.length_of_stay.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    length_dvt_lower_limb = length[length.visit_DVT_lower_limb_urgence == 1]
    dvt_lower_limb_mean = (
        str(round(length_dvt_lower_limb.length_of_stay.mean(), 2))
        + " ("
        + str(round(length_dvt_lower_limb.length_of_stay.std(), 2))
        + ")"
    )
    q75, q25 = np.nanpercentile(length_dvt_lower_limb.length_of_stay, [75, 25])
    dvt_lower_limb_median = (
        str(round(length_dvt_lower_limb.length_of_stay.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    length_dvt_lower_limb_without_superficial = length[
        length.visit_DVT_lower_limb_without_superficial_urgence == 1
    ]
    dvt_lower_limb_without_superficial_mean = (
        str(round(length_dvt_lower_limb_without_superficial.length_of_stay.mean(), 2))
        + " ("
        + str(round(length_dvt_lower_limb_without_superficial.length_of_stay.std(), 2))
        + ")"
    )
    q75, q25 = np.nanpercentile(
        length_dvt_lower_limb_without_superficial.length_of_stay, [75, 25]
    )
    dvt_lower_limb_without_superficial_median = (
        str(round(length_dvt_lower_limb_without_superficial.length_of_stay.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    length_dvt_cerebral = length[length.visit_DVT_cerebral_urgence == 1]
    dvt_cerebral_mean = (
        str(round(length_dvt_cerebral.length_of_stay.mean(), 2))
        + " ("
        + str(round(length_dvt_cerebral.length_of_stay.std(), 2))
        + ")"
    )
    q75, q25 = np.nanpercentile(length_dvt_cerebral.length_of_stay, [75, 25])
    dvt_cerebral_median = (
        str(round(length_dvt_cerebral.length_of_stay.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    length_dvt_portal_vein = length[length.visit_DVT_portal_vein_urgence == 1]
    dvt_portal_vein_mean = (
        str(round(length_dvt_portal_vein.length_of_stay.mean(), 2))
        + " ("
        + str(round(length_dvt_portal_vein.length_of_stay.std(), 2))
        + ")"
    )
    q75, q25 = np.nanpercentile(length_dvt_portal_vein.length_of_stay, [75, 25])
    dvt_portal_vein_median = (
        str(round(length_dvt_portal_vein.length_of_stay.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    length_dvt_nonspecific_location = length[
        length.visit_DVT_nonspecific_location_urgence == 1
    ]
    dvt_nonspecific_location_mean = (
        str(round(length_dvt_nonspecific_location.length_of_stay.mean(), 2))
        + " ("
        + str(round(length_dvt_nonspecific_location.length_of_stay.std(), 2))
        + ")"
    )
    q75, q25 = np.nanpercentile(
        length_dvt_nonspecific_location.length_of_stay, [75, 25]
    )
    dvt_nonspecific_location_median = (
        str(round(length_dvt_nonspecific_location.length_of_stay.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )

    length_of_stay = pd.DataFrame(
        {
            "VTE": [vte_median, vte_mean],
            "PE (+/- DVT)": [pe_median, pe_mean],
            "DVT": [dvt_median, dvt_mean],
            "DVT without superficial": [
                dvt_without_superficial_median,
                dvt_without_superficial_mean,
            ],
            "Lower limb DVT": [dvt_lower_limb_median, dvt_lower_limb_mean],
            "Lower limb DVT without superficial": [
                dvt_lower_limb_without_superficial_median,
                dvt_lower_limb_without_superficial_mean,
            ],
            "DVT cerebral": [dvt_cerebral_median, dvt_cerebral_mean],
            "DVT portal vein": [dvt_portal_vein_median, dvt_portal_vein_mean],
            "DVT non specific location": [
                dvt_nonspecific_location_median,
                dvt_nonspecific_location_mean,
            ],
        },
        index=["Median[IQR]", "Mean(sd)"],
    )
    stats["Length of stay (Days)"] = length_of_stay

    # AGE AT VISIT (YEARS)
    age = sankey_data[
        (sankey_data.mode_entree == "2-URG") & (sankey_data.type_visite == "I")
    ].copy()

    age_vte = age[age.visit_VTE_urgence == 1]
    vte_mean = (
        str(round(age_vte.age.mean(), 2))
        + " ("
        + str(round(age_vte.age.std(), 2))
        + ")"
    )
    q75, q25 = np.percentile(age_vte["age"], [75, 25])
    vte_median = (
        str(round(age_vte.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_pe = age[age.visit_PE_urgence == 1]
    pe_mean = (
        str(round(age_pe.age.mean(), 2)) + " (" + str(round(age_pe.age.std(), 2)) + ")"
    )
    q75, q25 = np.percentile(age_pe["age"], [75, 25])
    pe_median = (
        str(round(age_pe.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_dvt = age[age.visit_DVT_urgence == 1]
    dvt_mean = (
        str(round(age_dvt.age.mean(), 2))
        + " ("
        + str(round(age_dvt.age.std(), 2))
        + ")"
    )
    q75, q25 = np.percentile(age_dvt["age"], [75, 25])
    dvt_median = (
        str(round(age_dvt.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_dvt_without_superficial = age[age.visit_DVT_without_superficial_urgence == 1]
    dvt_without_superficial_mean = (
        str(round(age_dvt_without_superficial.age.mean(), 2))
        + " ("
        + str(round(age_dvt_without_superficial.age.std(), 2))
        + ")"
    )
    q75, q25 = np.percentile(age_dvt_without_superficial["age"], [75, 25])
    dvt_without_superficial_median = (
        str(round(age_dvt_without_superficial.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_dvt_lower_limb = age[age.visit_DVT_lower_limb_urgence == 1]
    dvt_lower_limb_mean = (
        str(round(age_dvt_lower_limb.age.mean(), 2))
        + " ("
        + str(round(age_dvt_lower_limb.age.std(), 2))
        + ")"
    )
    q75, q25 = np.percentile(age_dvt_lower_limb["age"], [75, 25])
    dvt_lower_limb_median = (
        str(round(age_dvt_lower_limb.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_dvt_lower_limb_without_superficial = age[
        age.visit_DVT_lower_limb_without_superficial_urgence == 1
    ]
    dvt_lower_limb_without_superficial_mean = (
        str(round(age_dvt_lower_limb_without_superficial.age.mean(), 2))
        + " ("
        + str(round(age_dvt_lower_limb_without_superficial.age.std(), 2))
        + ")"
    )
    q75, q25 = np.percentile(age_dvt_lower_limb_without_superficial["age"], [75, 25])
    dvt_lower_limb_without_superficial_median = (
        str(round(age_dvt_lower_limb_without_superficial.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_dvt_cerebral = age[age.visit_DVT_cerebral_urgence == 1]
    dvt_cerebral_mean = (
        str(round(age_dvt_cerebral.age.mean(), 2))
        + " ("
        + str(round(age_dvt_cerebral.age.std(), 2))
        + ")"
    )
    q75, q25 = np.percentile(age_dvt_cerebral["age"], [75, 25])
    dvt_cerebral_median = (
        str(round(age_dvt_cerebral.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_dvt_portal_vein = age[age.visit_DVT_portal_vein_urgence == 1]
    dvt_portal_vein_mean = (
        str(round(age_dvt_portal_vein.age.mean(), 2))
        + " ("
        + str(round(age_dvt_portal_vein.age.std(), 2))
        + ")"
    )
    q75, q25 = np.percentile(age_dvt_portal_vein["age"], [75, 25])
    dvt_portal_vein_median = (
        str(round(age_dvt_portal_vein.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_dvt_nonspecific_location = age[age.visit_DVT_nonspecific_location_urgence == 1]
    dvt_nonspecific_location_mean = (
        str(round(age_dvt_nonspecific_location.age.mean(), 2))
        + " ("
        + str(round(age_dvt_nonspecific_location.age.std(), 2))
        + ")"
    )
    q75, q25 = np.percentile(age_dvt_nonspecific_location["age"], [75, 25])
    dvt_nonspecific_location_median = (
        str(round(age_dvt_nonspecific_location.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_at_visit = pd.DataFrame(
        {
            "VTE": [vte_median, vte_mean],
            "PE (+/- DVT)": [pe_median, pe_mean],
            "DVT": [dvt_median, dvt_mean],
            "DVT without superficial": [
                dvt_without_superficial_median,
                dvt_without_superficial_mean,
            ],
            "Lower limb DVT": [dvt_lower_limb_median, dvt_lower_limb_mean],
            "Lower limb DVT without superficial": [
                dvt_lower_limb_without_superficial_median,
                dvt_lower_limb_without_superficial_mean,
            ],
            "DVT cerebral": [dvt_cerebral_median, dvt_cerebral_mean],
            "DVT portal vein": [dvt_portal_vein_median, dvt_portal_vein_mean],
            "DVT non specific location": [
                dvt_nonspecific_location_median,
                dvt_nonspecific_location_mean,
            ],
        },
        index=["Median[IQR]", "Mean(sd)"],
    )
    stats["Age at visit (Years)"] = age_at_visit

    return pd.concat(stats)


def compute_stats_from_sankey_all(sankey_data: pd.DataFrame):
    stats = {}

    # Total
    total = sankey_data[sankey_data.type_visite == "I"].copy()

    stats["Total"] = (
        total.groupby(["type_visite"], as_index=False)[
            [
                "visit_VTE",
                "visit_PE",
                "visit_DVT_lower_limb",
                "visit_DVT_portal_vein",
                "visit_DVT_cerebral",
            ]
        ]
        .sum()
        .rename(
            columns={
                "visit_VTE": "VTE",
                "visit_PE": "PE (+/- DVT)",
                "visit_DVT_lower_limb": "Lower limb DVT",
                "visit_DVT_portal_vein": "Portal vein DVT",
                "visit_DVT_cerebral": "Cerebral DVT",
            },
            index={0: "Total"},
        )
        .drop(columns="type_visite")
    )

    # Last known vital status
    vital_status = sankey_data[sankey_data.type_visite == "I"].copy()
    vital_status["Last known vital status"] = "Alive"
    vital_status["Last known vital status"] = vital_status[
        "Last known vital status"
    ].mask(vital_status.vital_status == "D", "Dead")

    vital_status = (
        vital_status.groupby(["Last known vital status"])
        .agg(
            {
                "visit_VTE": "sum",
                "visit_PE": "sum",
                "visit_DVT_lower_limb": "sum",
                "visit_DVT_portal_vein": "sum",
                "visit_DVT_cerebral": "sum",
            }
        )
        .rename(
            columns={
                "visit_VTE": "VTE",
                "visit_PE": "PE (+/- DVT)",
                "visit_DVT_lower_limb": "Lower limb DVT",
                "visit_DVT_portal_vein": "Portal vein DVT",
                "visit_DVT_cerebral": "Cerebral DVT",
            }
        )
    )
    vital_status = vital_status.apply(
        axis=1,
        func=lambda x: x.astype(str)
        + " ("
        + (x * 100 / stats["Total"].loc["Total"]).map("{:,.1f}".format)
        + " %)",
    )
    stats["Last known vital status"] = vital_status.sort_index()

    # Hospital entry mode
    hosp_entry = sankey_data[sankey_data.type_visite == "I"].copy()
    hosp_entry["Hospital entry mode"] = "Other entry"
    hosp_entry["Hospital entry mode"] = hosp_entry["Hospital entry mode"].mask(
        hosp_entry.mode_entree == "2-URG", "Emergency"
    )

    hosp_entry = (
        hosp_entry.groupby(["Hospital entry mode"])
        .agg(
            {
                "visit_VTE": "sum",
                "visit_PE": "sum",
                "visit_DVT_lower_limb": "sum",
                "visit_DVT_portal_vein": "sum",
                "visit_DVT_cerebral": "sum",
            }
        )
        .rename(
            columns={
                "visit_VTE": "VTE",
                "visit_PE": "PE (+/- DVT)",
                "visit_DVT_lower_limb": "Lower limb DVT",
                "visit_DVT_portal_vein": "Portal vein DVT",
                "visit_DVT_cerebral": "Cerebral DVT",
            }
        )
    )
    hosp_entry = hosp_entry.apply(
        axis=1,
        func=lambda x: x.astype(str)
        + " ("
        + (x * 100 / stats["Total"].loc["Total"]).map("{:,.1f}".format)
        + " %)",
    )
    stats["Hospital entry mode"] = hosp_entry.sort_index()

    # Hospital discharge mode
    hosp_discharge = sankey_data[sankey_data.type_visite == "I"].copy()
    hosp_discharge["Hospital discharge mode"] = "Other discharge"
    hosp_discharge["Hospital discharge mode"] = hosp_discharge[
        "Hospital discharge mode"
    ].mask(hosp_discharge.mode_sortie.isna(), "Missing data")
    hosp_discharge["Hospital discharge mode"] = hosp_discharge[
        "Hospital discharge mode"
    ].mask(hosp_discharge.mode_sortie == "6-DC", "Death")
    hosp_discharge["Hospital discharge mode"] = hosp_discharge[
        "Hospital discharge mode"
    ].mask(hosp_discharge.mode_sortie == "1-DOMI", "Home")

    hosp_discharge = (
        hosp_discharge.groupby(["Hospital discharge mode"])
        .agg(
            {
                "visit_VTE": "sum",
                "visit_PE": "sum",
                "visit_DVT_lower_limb": "sum",
                "visit_DVT_portal_vein": "sum",
                "visit_DVT_cerebral": "sum",
            }
        )
        .rename(
            columns={
                "visit_VTE": "VTE",
                "visit_PE": "PE (+/- DVT)",
                "visit_DVT_lower_limb": "Lower limb DVT",
                "visit_DVT_portal_vein": "Portal vein DVT",
                "visit_DVT_cerebral": "Cerebral DVT",
            }
        )
    )
    hosp_discharge.loc["Available data"] = (
        stats["Total"].loc["Total"] - hosp_discharge.loc["Missing data"]
    )
    hosp_discharge = hosp_discharge.drop("Missing data")

    hosp_discharge = hosp_discharge.apply(
        axis=1,
        func=lambda x: x.astype(str)
        + " ("
        + (x * 100 / stats["Total"].loc["Total"]).map("{:,.1f}".format)
        + " %)",
    )
    stats["Hospital discharge mode"] = hosp_discharge.sort_index()

    # SEX RATIO
    gender = sankey_data[sankey_data.type_visite == "I"].copy()
    gender = (
        gender.groupby(["gender"])
        .agg(
            {
                "visit_VTE": "sum",
                "visit_PE": "sum",
                "visit_DVT_lower_limb": "sum",
                "visit_DVT_portal_vein": "sum",
                "visit_DVT_cerebral": "sum",
            }
        )
        .rename(
            columns={
                "visit_VTE": "VTE",
                "visit_PE": "PE (+/- DVT)",
                "visit_DVT_lower_limb": "Lower limb DVT",
                "visit_DVT_portal_vein": "Portal vein DVT",
                "visit_DVT_cerebral": "Cerebral DVT",
            }
        )
    )
    gender.loc["Ratio"] = round(gender.loc["M"] / gender.loc["W"], 2)
    gender.loc["M"] = gender.loc["M"].astype(int).astype(str)
    gender.loc["W"] = gender.loc["W"].astype(int).astype(str)
    gender.loc["Ratio (men/women)"] = (
        gender.loc["M"]
        + "/"
        + gender.loc["W"]
        + " ("
        + gender.loc["Ratio"].astype(str)
        + ")"
    )
    gender = gender.drop(index=["M", "W", "Ratio"])
    stats["Gender"] = gender

    # LENGTH OF STAY (DAYS)
    length = sankey_data[sankey_data.type_visite == "I"].copy()

    vte_mean = (
        str(round(length.length_of_stay.mean(), 2))
        + " ("
        + str(round(length.length_of_stay.std(), 2))
        + ")"
    )
    q75, q25 = np.nanpercentile(length.length_of_stay, [75, 25])
    vte_median = (
        str(round(length.length_of_stay.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    length_pe = length[length.visit_PE == 1]
    pe_mean = (
        str(round(length_pe.length_of_stay.mean(), 2))
        + " ("
        + str(round(length_pe.length_of_stay.std(), 2))
        + ")"
    )
    q75, q25 = np.nanpercentile(length_pe.length_of_stay, [75, 25])
    pe_median = (
        str(round(length_pe.length_of_stay.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    length_dvt_lower_limb = length[length.visit_DVT_lower_limb == 1]
    dvt_lower_limb_mean = (
        str(round(length_dvt_lower_limb.length_of_stay.mean(), 2))
        + " ("
        + str(round(length_dvt_lower_limb.length_of_stay.std(), 2))
        + ")"
    )
    q75, q25 = np.nanpercentile(length_dvt_lower_limb.length_of_stay, [75, 25])
    dvt_lower_limb_median = (
        str(round(length_dvt_lower_limb.length_of_stay.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    length_dvt_portal_vein = length[length.visit_DVT_portal_vein == 1]
    dvt_portal_vein_mean = (
        str(round(length_dvt_portal_vein.length_of_stay.mean(), 2))
        + " ("
        + str(round(length_dvt_portal_vein.length_of_stay.std(), 2))
        + ")"
    )
    q75, q25 = np.nanpercentile(length_dvt_portal_vein.length_of_stay, [75, 25])
    dvt_portal_vein_median = (
        str(round(length_dvt_portal_vein.length_of_stay.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    length_dvt_cerebral = length[length.visit_DVT_cerebral == 1]
    dvt_cerebral_mean = (
        str(round(length_dvt_cerebral.length_of_stay.mean(), 2))
        + " ("
        + str(round(length_dvt_cerebral.length_of_stay.std(), 2))
        + ")"
    )
    q75, q25 = np.nanpercentile(length_dvt_cerebral.length_of_stay, [75, 25])
    dvt_cerebral_median = (
        str(round(length_dvt_cerebral.length_of_stay.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )

    length_of_stay = pd.DataFrame(
        {
            "VTE": [vte_median, vte_mean],
            "PE (+/- DVT)": [pe_median, pe_mean],
            "Lower limb DVT": [dvt_lower_limb_median, dvt_lower_limb_mean],
            "Portal vein DVT": [dvt_portal_vein_median, dvt_portal_vein_mean],
            "Cerebral DVT": [dvt_cerebral_median, dvt_cerebral_mean],
        },
        index=["Median[IQR]", "Mean(sd)"],
    )
    stats["Length of stay (Days)"] = length_of_stay

    # AGE AT VISIT (YEARS)
    age = sankey_data[sankey_data.type_visite == "I"].copy()

    vte_mean = str(round(age.age.mean(), 2)) + " (" + str(round(age.age.std(), 2)) + ")"
    q75, q25 = np.percentile(age["age"], [75, 25])
    vte_median = (
        str(round(age.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_pe = age[age.visit_PE == 1]
    pe_mean = (
        str(round(age_pe.age.mean(), 2)) + " (" + str(round(age_pe.age.std(), 2)) + ")"
    )
    q75, q25 = np.percentile(age_pe["age"], [75, 25])
    pe_median = (
        str(round(age_pe.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_pe = age[age.visit_PE == 1]
    pe_mean = (
        str(round(age_pe.age.mean(), 2)) + " (" + str(round(age_pe.age.std(), 2)) + ")"
    )
    q75, q25 = np.percentile(age_pe["age"], [75, 25])
    pe_median = (
        str(round(age_pe.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_dvt_lower_limb = age[age.visit_DVT_lower_limb == 1]
    dvt_lower_limb_mean = (
        str(round(age_dvt_lower_limb.age.mean(), 2))
        + " ("
        + str(round(age_dvt_lower_limb.age.std(), 2))
        + ")"
    )
    q75, q25 = np.percentile(age_dvt_lower_limb["age"], [75, 25])
    dvt_lower_limb_median = (
        str(round(age_dvt_lower_limb.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_dvt_portal_vein = age[age.visit_DVT_portal_vein == 1]
    dvt_portal_vein_mean = (
        str(round(age_dvt_portal_vein.age.mean(), 2))
        + " ("
        + str(round(age_dvt_portal_vein.age.std(), 2))
        + ")"
    )
    q75, q25 = np.percentile(age_dvt_portal_vein["age"], [75, 25])
    dvt_portal_vein_median = (
        str(round(age_dvt_portal_vein.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_dvt_cerebral = age[age.visit_DVT_cerebral == 1]
    dvt_cerebral_mean = (
        str(round(age_dvt_cerebral.age.mean(), 2))
        + " ("
        + str(round(age_dvt_cerebral.age.std(), 2))
        + ")"
    )
    q75, q25 = np.percentile(age_dvt_cerebral["age"], [75, 25])
    dvt_cerebral_median = (
        str(round(age_dvt_cerebral.age.median()))
        + " ["
        + str(round(q25))
        + "-"
        + str(round(q75))
        + "]"
    )
    age_at_visit = pd.DataFrame(
        {
            "VTE": [vte_median, vte_mean],
            "PE (+/- DVT)": [pe_median, pe_mean],
            "Lower limb DVT": [dvt_lower_limb_median, dvt_lower_limb_mean],
            "Portal vein DVT": [dvt_portal_vein_median, dvt_portal_vein_mean],
            "Cerebral DVT": [dvt_cerebral_median, dvt_cerebral_mean],
        },
        index=["Median[IQR]", "Mean(sd)"],
    )
    stats["Age at visit (Years)"] = age_at_visit

    return pd.concat(stats)

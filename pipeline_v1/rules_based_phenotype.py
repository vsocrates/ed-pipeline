import copy
import datetime
from distutils import util
import json
import os
import pickle
import re

from imblearn.over_sampling import SMOTE, SMOTENC
import matplotlib.pyplot as plt
import numpy as np
from numpy import set_printoptions
import pandas as pd
from pandas import read_csv
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    lit,
    lower,
    regexp_replace,
    substring,
    to_date,
    trim,
    upper,
    when,
)
import sklearn
from sklearn.calibration import CalibratedClassifierCV, calibration_curve
from sklearn.ensemble import ExtraTreesClassifier, RandomForestClassifier
from sklearn.feature_selection import RFE, SelectKBest, chi2, f_classif
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    auc,
    brier_score_loss,
    classification_report,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_curve,
)
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.preprocessing import Normalizer, StandardScaler
from sklearn.svm import LinearSVC
from sklearn.tree import DecisionTreeClassifier, plot_tree
import xgboost as xgb

opioid_phenotyping_omop = {
    "care_site": "/projects/cch/opioid-phenotyping/data/raw/omop/care_site",
    "condition_occurence": "/projects/cch/opioid-phenotyping/data/raw/omop/condition_occurrence",
    "drug_exposure": "/projects/cch/opioid-phenotyping/data/raw/omop/drug_exposure",
    "location": "/projects/cch/opioid-phenotyping/data/raw/omop/location",
    "measurement": "/projects/cch/opioid-phenotyping/data/raw/omop/measurement",
    "note": "/projects/cch/opioid-phenotyping/data/raw/omop/note",
    "person": "/projects/cch/opioid-phenotyping/data/raw/omop/person",
    "procedure_occurence": "/projects/cch/opioid-phenotyping/data/raw/omop/procedure_occurrence",
    "specimen": "/projects/cch/opioid-phenotyping/data/raw/omop/specimen",
    "visit_occurence": "/projects/cch/opioid-phenotyping/data/raw/omop/visit_occurrence",
}


def read_in_targets():
    df_kevin = pd.read_csv("files/df_kevin_full_updated.csv")
    # there are duplicate rows with different data, will only take first
    df_kevin = df_kevin.groupby("visit_occurrence_id").first().reset_index()
    # print(len(df_kevin['visit_occurrence_id'].unique()))
    # Get our list of visit_occurrence_ids
    VO_ids = df_kevin["visit_occurrence_id"].unique().tolist()
    # Lets fix the columns to make them either 1 or 0
    df_kevin.loc[
        :, list(set(df_kevin.columns.tolist()) - set(["visit_occurrence_id"]))
    ] = df_kevin[list(set(df_kevin.columns.tolist()) - set(["visit_occurrence_id"]))].applymap(
        lambda x: 1 if x == "yes" else 0
    )
    # Print out counts for positive targets
    df_kevin.sum()
    return df_kevin, VO_ids


def get_tables():
    # Get required tables
    diagnosis_extract = spark.read.parquet("{}diagnosis_extract".format(base_data_loc))
    medications = spark.read.parquet("{}medications".format(base_data_loc))
    measure_extract = spark.read.parquet("{}measure_extract".format(base_data_loc))
    medications_classification = spark.read.parquet(
        "{}medication_classification".format(base_data_loc)
    )
    urine_toxicology = spark.read.parquet("{}urine_toxicology".format(base_data_loc))
    blood_culture = spark.read.parquet("{}blood_culture".format(base_data_loc))
    alcohol = spark.read.parquet("{}alcohol".format(base_data_loc))
    VO = spark.read.parquet(opioid_phenotyping_omop["visit_occurence"])

    # Get tables with respect of the Visit Occurence ids
    # fix age to reflect for visit occurence end date
    PER = spark.read.parquet(opioid_phenotyping_omop["person"])
    BirthYearDF = (
        VO.where(col("visit_occurrence_id").isin(VO_ids))
        .select("visit_occurrence_id", "person_id", "visit_end_datetime")
        .join(PER, "person_id")
        .select(
            "visit_occurrence_id",
            f.year(PER.birth_datetime).alias("BirthYear"),
            "visit_end_datetime",
        )
    )

    # get prior visit occurrences
    VO_ids_w_prior = (
        VO.where(col("visit_occurrence_id").isin(VO_ids))
        .select("person_id", "visit_end_datetime")
        .alias("a")
        .join(VO.alias("b"), "person_id")
        .filter(
            (col("a.person_id") == col("b.person_id"))
            & (col("b.visit_end_datetime") <= col("a.visit_end_datetime"))
        )
        .select("b.visit_occurrence_id")
        .toPandas()["visit_occurrence_id"]
        .tolist()
    )
    # Get cheif Complaints
    CC = (
        diagnosis_extract.where(col("visit_occurrence_id").isin(VO_ids))
        .select("visit_occurrence_id", "chief_complaint")
        .withColumn("count", lit(1))
        .groupby("visit_occurrence_id")
        .pivot("chief_complaint")
        .sum("count")
        .fillna(0)
    )
    # Fix columns to contain special prefix
    CC = CC.select(
        [c if c == "visit_occurrence_id" else col(c).alias("CCTBL_" + c) for c in CC.columns]
    )
    # Maybe refactor to function?
    # Lets try to get all levels of icd codes there are a total of 6 characters for icd10
    DE_lst = [None] * 8
    for i in [1, 2, 3, 4, 5, 6, 7, 8]:
        DE_tmp = (
            diagnosis_extract.where(col("visit_occurrence_id").isin(VO_ids))
            .withColumn(
                "condition_source_value",
                when(
                    col("condition_source_value").rlike("[A-TV-Z][0-9][0-9AB]\.?[0-9A-TV-Z]{0,4}"),
                    substring(upper(col("condition_source_value")), 1, i),
                ).otherwise(upper(col("condition_source_value"))),
            )
            .select("visit_occurrence_id", "condition_source_value")
            .withColumn("count", lit(1))
            .groupby(col("visit_occurrence_id"))
            .pivot("condition_source_value")
            .sum("count")
            .fillna(0)
        )
        # this dot (.) is problematic, cant call without backticks replacing with underscore
        DE_tmp = DE_tmp.toDF(*(c.replace(".", "_") for c in DE_tmp.columns))
        DE_lst[i - 1] = DE_tmp
    del DE_lst[3]

    DE = None
    for el in DE_lst:
        if DE == None:
            DE = el
        else:
            prev_cols = DE.columns
            curr_cols = el.columns
            del prev_cols[prev_cols.index("visit_occurrence_id")]
            select_cols = list(set(curr_cols) - set(prev_cols))
            DE = DE.join(el.select([x for x in select_cols]), "visit_occurrence_id", "left")

    # this dot (.) is problematic, cant call without backticks replacing with underscore
    DE = DE.toDF(*(c.replace(".", "_") for c in DE.columns))
    DE = DE.select(
        [c if c == "visit_occurrence_id" else col(c).alias("DETBL_" + c) for c in DE.columns]
    )
    # Join Chief complaint with the Diagnosis Extract
    DE_new = DE.join(CC, "visit_occurrence_id", "left")

    # Lets get all the levels of ATC for medication
    MED_lst = [None] * 5
    for ATC_LEVEL in [1, 2, 3, 4, 5]:
        MED_tmp = (
            medications.where(col("visit_occurrence_id").isin(VO_ids))
            .join(medications_classification, "drug_concept_id")
            .select("visit_occurrence_id", "ATC_{}".format(ATC_LEVEL))
            .withColumn("count", lit(1))
            .groupby(col("visit_occurrence_id"))
            .pivot("ATC_{}".format(ATC_LEVEL))
            .sum("count")
            .fillna(0)
        )
        # this dot (.) is problematic, cant call without backticks replacing with underscore
        MED_tmp = MED_tmp.toDF(*(c.replace(".", "_") for c in MED_tmp.columns))
        MED_lst[ATC_LEVEL - 1] = MED_tmp

    MED = None
    for el in MED_lst:
        if MED == None:
            MED = el
        else:
            prev_cols = MED.columns
            curr_cols = el.columns
            del prev_cols[prev_cols.index("visit_occurrence_id")]
            select_cols = list(set(curr_cols) - set(prev_cols))
            MED = MED.join(el.select([x for x in select_cols]), "visit_occurrence_id", "left")

    UT = (
        urine_toxicology.where(
            (col("visit_occurrence_id").isin(VO_ids)) & (col("isSubstancePositive") == "True")
        )
        .select("visit_occurrence_id", "Substance", "isSubstancePositive")
        .withColumn("count", lit(1))
        .groupby("visit_occurrence_id")
        .pivot("Substance")
        .sum("count")
        .fillna(0)
    )

    BC = (
        blood_culture.where(
            (col("visit_occurrence_id").isin(VO_ids)) & (col("no_growth_in_specimen") == "False")
        )
        .select("visit_occurrence_id", "culture_growth")
        .withColumn("count", lit(1))
        .groupby("visit_occurrence_id")
        .pivot("culture_growth")
        .sum("count")
        .fillna(0)
    )

    ACH = (
        alcohol.where(
            (col("visit_occurrence_id").isin(VO_ids)) & (col("alcohol_positive") == "True")
        )
        .select("visit_occurrence_id", "alcohol_positive")
        .withColumn("count", lit(1))
        .groupby("visit_occurrence_id")
        .sum("count")
        .alias("alcohol_positive")
        .fillna(0)
        .withColumnRenamed("sum(count)", "alcohol_positive")
    )

    ME = measure_extract.where(col("visit_occurrence_id").isin(VO_ids)).fillna(0)

    # change the alias for rest of the tables
    MED_new = MED.select(
        [c if c == "visit_occurrence_id" else col(c).alias("MEDTBL_" + c) for c in MED.columns]
    )
    ME_new = ME.select(
        [c if c == "visit_occurrence_id" else col(c).alias("METBL_" + c) for c in ME.columns]
    )
    UT_new = UT.select(
        [c if c == "visit_occurrence_id" else col(c).alias("UTTBL_" + c) for c in UT.columns]
    )
    BC_new = BC.select(
        [c if c == "visit_occurrence_id" else col(c).alias("BCTBL_" + c) for c in BC.columns]
    )
    ACH_new = ACH.select(
        [c if c == "visit_occurrence_id" else col(c).alias("ACHTBL_" + c) for c in ACH.columns]
    )

    # .join(ME_new, 'visit_occurrence_id', 'left') \ #Remove vitals/measurements
    res = (
        DE_new.join(MED_new, "visit_occurrence_id", "left")
        .join(UT_new, "visit_occurrence_id", "left")
        .join(BC_new, "visit_occurrence_id", "left")
        .join(ACH_new, "visit_occurrence_id", "left")
        .join(BirthYearDF, "visit_occurrence_id", "left")
    )

    # Clean up column names
    res = res.select([col(c).alias(re.sub(r"[ ,;{}()\n\t=]+", "", c)) for c in res.columns])
    res = res.fillna(0)

    # If we generate a new DF with the small subset of VO_ids we just put it into a df
    df = res.toPandas()
    return df


# Get best features
def get_best_features(num_feats):
    ICD_LEVEL = "ALL"
    ATC_LEVEL = "ALL"
    for target in list(set(df_kevin.columns.tolist()) - set(["visit_occurrence_id"])):
        print(target)
        pdf = (
            df_kevin[["visit_occurrence_id", target]]
            .join(df.set_index("visit_occurrence_id"), on="visit_occurrence_id")
            .fillna(0)
        )
        pdf = pdf.drop("visit_occurrence_id", axis=1)
        X = pdf.loc[:, pdf.columns != target]
        Y = pdf[target]
        # Chi
        from sklearn.feature_selection import SelectKBest, chi2
        from sklearn.preprocessing import MinMaxScaler

        X_norm = MinMaxScaler().fit_transform(X)
        chi_selector = SelectKBest(chi2, k=num_feats)
        chi_selector.fit(X, Y)
        chi_support = chi_selector.get_support()
        chi_feature = X.loc[:, chi_support].columns.tolist()
        print(str(len(chi_feature)), "selected features")
        print(chi_feature)

        # RFE
        from sklearn.feature_selection import RFE
        from sklearn.linear_model import LogisticRegression

        rfe_selector = RFE(
            estimator=LogisticRegression(penalty="l1", C=0.3, solver="liblinear"),
            n_features_to_select=num_feats,
            step=10,
            verbose=5,
        )
        rfe_selector.fit(X, Y)
        rfe_support = rfe_selector.get_support()
        rfe_feature = X.loc[:, rfe_support].columns.tolist()
        print(str(len(rfe_feature)), "selected features")
        print(rfe_feature)

        # LR
        from sklearn.feature_selection import SelectFromModel
        from sklearn.linear_model import LogisticRegression

        embeded_lr_selector = SelectFromModel(
            LogisticRegression(penalty="l1", C=0.2, solver="liblinear"), max_features=num_feats
        )
        embeded_lr_selector.fit(X, Y)
        embeded_lr_support = embeded_lr_selector.get_support()
        embeded_lr_feature = X.loc[:, embeded_lr_support].columns.tolist()
        print(str(len(embeded_lr_feature)), "selected features")
        print(embeded_lr_feature)

        # RF
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.feature_selection import SelectFromModel

        embeded_rf_selector = SelectFromModel(
            RandomForestClassifier(n_estimators=300), max_features=num_feats
        )
        embeded_rf_selector.fit(X, Y)
        embeded_rf_support = embeded_rf_selector.get_support()
        embeded_rf_feature = X.loc[:, embeded_rf_support].columns.tolist()
        print(str(len(embeded_rf_feature)), "selected features")
        print(embeded_rf_feature)

        # XGBOOST
        from sklearn.feature_selection import SelectFromModel
        from xgboost import XGBClassifier

        xgb = XGBClassifier(n_estimators=500)
        embeded_xgb_selector = SelectFromModel(xgb, max_features=num_feats)
        embeded_xgb_selector.fit(X, Y)
        embeded_xgb_support = embeded_xgb_selector.get_support()
        embeded_xgb_feature = X.loc[:, embeded_xgb_support].columns.tolist()
        print(str(len(embeded_xgb_feature)), "selected features")
        print(embeded_xgb_feature)

        # Shap
        import shap
        import xgboost

        # create a train/test split
        model = XGBClassifier(n_estimators=1000)
        model.fit(X, Y)
        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X)
        shap_sum = np.abs(shap_values).mean(axis=0)
        importance_df = pd.DataFrame([X.columns.tolist(), shap_sum.tolist()]).T
        importance_df.columns = ["column_name", "shap_importance"]
        importance_df = importance_df.sort_values("shap_importance", ascending=False)
        shap_feature = importance_df["column_name"][0:40].tolist()
        print(shap_feature)
        shap_support = X.columns.isin(shap_feature)
        print(shap_support)

        # Collecting results
        pd.set_option("display.max_rows", None)
        # put all selection together
        feature_name = X.columns
        feature_selection_df = pd.DataFrame(
            {
                "Feature": feature_name,
                "xChi-2": chi_support,
                "RFE": rfe_support,
                "Logistics": embeded_lr_support,
                "Random Forest": embeded_rf_support,
                "GradientBoosting": embeded_xgb_support,
                "shap_imp": shap_support,
            }
        )
        # count the selected times for each feature
        feature_selection_df["Total"] = np.sum(feature_selection_df, axis=1)
        # display the top 100
        feature_selection_df = feature_selection_df.sort_values(
            ["Total", "Feature"], ascending=False
        )
        feature_selection_df.index = range(1, len(feature_selection_df) + 1)
        feature_selection_df.head(num_feats)
        # Saving results
        feature_selection_df.to_csv(
            "feature_selection_results/feature_list_{}_target_{}_ICD_{}_ATC_{}.csv".format(
                num_feats, target, ICD_LEVEL, ATC_LEVEL
            )
        )


# Read in all features
def read_best_features(num_feats):
    top_n = num_feats
    ICD_LEVEL = "ALL"
    ATC_LEVEL = "ALL"
    targets = list(set(df_kevin.columns.tolist()) - set(["visit_occurrence_id"]))
    feature_list_dict = {}
    feature_df_dict = {}
    for target in targets:
        feat_df = pd.read_csv(
            "feature_selection_results/feature_list_{}_target_{}_ICD_{}_ATC_{}.csv".format(
                num_feats, target, ICD_LEVEL, ATC_LEVEL
            ),
            index_col=0,
        )
        feature_list_dict[target] = feat_df[~feat_df.Feature.str.contains("person_id")][:top_n][
            "Feature"
        ].to_list()
        feature_df_dict[target] = feat_df
    return feature_list_dict, feature_df_dict


def read_ICD_ATC_codes():
    # ICD codes are used for diagnosis
    f = open("files/icd10cm_order_2021.txt", "r")
    txt = f.read().split("\n")
    icd_codes = {}
    for line in txt:
        # print(line)
        code = line[6:14].strip()
        code = code[:3] + "." + code[3:]
        icd_codes[code] = line[16:77].strip()

    ATC = pd.read_csv("files/ATC.csv")
    ATC["ATC_code"] = ATC["Class ID"].apply(lambda x: x.split("/")[-1])
    return icd_codes, ATC


def human_read_feats(f):
    feats_copy = copy.deepcopy(f)
    feats = copy.deepcopy(f)
    for idx, feat in enumerate(feats_copy):
        if "DETBL_" in feat:
            splits = feat.split("_")
            if len(splits) > 2:
                icd = splits[1] + "." + splits[2]
            else:
                icd = splits[1] + "."
            if icd in icd_codes:
                # print(icd_codes[icd])
                feats[idx] = "DETBL_" + icd_codes[icd]
        elif "MEDTBL_" in feat:
            atc_code = feat.split("_")[1]
            if atc_code in ATC["ATC_code"].values:
                feats[idx] = (
                    "MEDTBL_" + ATC[ATC["ATC_code"] == atc_code]["Preferred Label"].values[0]
                )
    return feats


def plot_calibration_curve(
    est, name_, fig_index, X_train, X_val, X_test, y_train, y_val, y_test, cv, target
):
    """Plot calibration curve for est w/o and with calibration."""
    # Calibrated with isotonic calibration
    isotonic = CalibratedClassifierCV(est, cv=cv, method="isotonic", n_jobs=-1)

    # Calibrated with sigmoid calibration
    sigmoid = CalibratedClassifierCV(est, cv=cv, method="sigmoid", n_jobs=-1)

    # Logistic regression with no calibration as baseline
    lr = LogisticRegression(C=1.0)

    fig = plt.figure(fig_index, figsize=(10, 10))
    ax1 = plt.subplot2grid((3, 1), (0, 0), rowspan=2)
    ax2 = plt.subplot2grid((3, 1), (2, 0))

    ax1.plot([0, 1], [0, 1], "k:", label="Perfectly calibrated")
    for clf, name in [
        (lr, "Logistic"),
        (est, name_),
        (isotonic, name_ + " + Isotonic"),
        (sigmoid, name_ + " + Sigmoid"),
    ]:
        if name == "Logistic":
            clf.fit(X_train, y_train)
        elif name == name_ + " + Isotonic" or name == name_ + " + Sigmoid":
            clf.fit(X_val, y_val)  # Have to calibrate using test data
        y_pred = clf.predict(X_test)
        if hasattr(clf, "predict_proba"):
            prob_pos = clf.predict_proba(X_test)[:, 1]
        else:  # use decision function
            prob_pos = clf.decision_function(X_test)
            prob_pos = (prob_pos - prob_pos.min()) / (prob_pos.max() - prob_pos.min())

        clf_score = brier_score_loss(y_test, prob_pos, pos_label=y_test.max())
        print("%s:" % name)
        print("\tBrier: %1.3f" % (clf_score))
        print("\tPrecision: %1.3f" % precision_score(y_test, y_pred))
        print("\tRecall: %1.3f" % recall_score(y_test, y_pred))
        print("\tF1: %1.3f\n" % f1_score(y_test, y_pred))

        fraction_of_positives, mean_predicted_value = calibration_curve(
            y_test, prob_pos, n_bins=10
        )

        ax1.plot(
            mean_predicted_value,
            fraction_of_positives,
            "s-",
            label="%s (%1.3f)" % (name, clf_score),
        )

        ax2.hist(prob_pos, range=(0, 1), bins=10, label=name, histtype="step", lw=2)

    ax1.set_ylabel("Fraction of positives")
    ax1.set_ylim([-0.05, 1.05])
    ax1.legend(loc="lower right")
    ax1.set_title("Calibration plots  (reliability curve)")

    ax2.set_xlabel("Mean predicted value")
    ax2.set_ylabel("Count")
    ax2.legend(loc="upper center", ncol=2)

    plt.tight_layout()
    plt.savefig(
        "feature_selection_results/Calibration_plot_{}_target_{}_ICD_{}_ATC_{}.png".format(
            num_feats, target, ICD_LEVEL, ATC_LEVEL
        ),
        dpi=300,
    )
    plt.show()


def plot_PR_curve(y_test, probs, target):
    # calculate precision-recall curve
    precision, recall, thresholds = precision_recall_curve(y_test, probs[:, 1])
    # calculate precision-recall AUC
    pr_auc = auc(recall, precision)

    plt.title(
        "PR_Curve_AUC_{}_target_{}_ICD_{}_ATC_{}".format(num_feats, target, ICD_LEVEL, ATC_LEVEL)
    )
    plt.plot(recall, precision, "b", label="AUC = %0.2f" % pr_auc)
    # calculate the no skill line as the proportion of the positive class
    no_skill = len(y_test[y_test == 1]) / len(
        y_test
    )  # Essentially the fraction of positive classes/total number of examples
    # plot the no skill precision-recall curve
    plt.plot([0, 1], [no_skill, no_skill], "r--", label="No Skill = %0.2f" % no_skill)
    plt.legend(loc="lower right")
    plt.xlim([0, 1])
    plt.ylim([0, 1])
    plt.xlabel("Recall")
    plt.ylabel("Precision")
    plt.savefig(
        "feature_selection_results/PR_Curve_AUC_{}_target_{}_ICD_{}_ATC_{}.png".format(
            num_feats, target, ICD_LEVEL, ATC_LEVEL
        ),
        dpi=300,
    )
    plt.show()


def plot_ROC_curve(y_test, probs, target):
    fpr, tpr, thresholds = roc_curve(y_test, probs[:, 1], pos_label=1)
    roc_auc = auc(fpr, tpr)
    # method I: plt
    import matplotlib.pyplot as plt

    plt.title(
        "ROC_Curve_AUC_{}_target_{}_ICD_{}_ATC_{}".format(num_feats, target, ICD_LEVEL, ATC_LEVEL)
    )
    plt.plot(fpr, tpr, "b", label="AUC = %0.2f" % roc_auc)
    plt.legend(loc="lower right")
    plt.plot([0, 1], [0, 1], "r--")
    plt.xlim([0, 1])
    plt.ylim([0, 1])
    plt.ylabel("True Positive Rate")
    plt.xlabel("False Positive Rate")
    plt.savefig(
        "feature_selection_results/ROC_Curve_AUC_{}_target_{}_ICD_{}_ATC_{}.png".format(
            num_feats, target, ICD_LEVEL, ATC_LEVEL
        ),
        dpi=300,
    )
    plt.show()


def train_model():
    models = {}
    plt.rcParams["figure.dpi"] = 150
    SEED = 42
    param_grid = {
        "max_depth": np.arange(2, 6),
        "max_features": np.arange(50, 151),
        "min_samples_leaf": np.arange(11, 16),
    }  # , 'min_samples_split': np.arange(20, 30)}

    for target, feats in feature_list_dict.items():
        if target == "hallucinogen_yn":
            continue
        """f2 = []
        for x in feats:
            if ('DETBL_' in x or 'MEDTBL_' in x) and len(x) > 8:
                f2.append(x)
            elif ('DETBL_' not in x and 'MEDTBL_' not in x):
                f2.append(x)
        feats = f2"""
        print("Target: ", target)
        # print('Feature set: ', feats)
        pdf = (
            df_kevin[["visit_occurrence_id", target]]
            .join(df.set_index("visit_occurrence_id"), on="visit_occurrence_id")
            .fillna(0)
        )
        pdf = pdf.drop("visit_occurrence_id", axis=1)
        X = pdf.loc[:, feats]
        Y = pdf[target]
        # X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state=SEED, stratify=Y)
        X_train, X_tmp, y_train, y_tmp = train_test_split(
            X, Y, test_size=0.2, random_state=SEED, stratify=Y
        )
        X_val, X_test, y_val, y_test = train_test_split(
            X_tmp, y_tmp, test_size=0.5, random_state=SEED, stratify=y_tmp
        )
        print(len(X_train))
        # We must standardize after we split as we dont want to leak any information
        # colsToStandardize = list(set([c for c in X_train.columns.tolist() if 'METBL_' in c]))
        # normalizer = Normalizer()
        # X_train.loc[:,colsToStandardize] = normalizer.fit_transform(X_train.loc[:,colsToStandardize])
        # X_test.loc[:,colsToStandardize] = normalizer.transform(X_test.loc[:,colsToStandardize])
        # Need to ensure that our feats are in the cols to binarize for categorical
        # sm = SMOTENC(random_state=SEED, categorical_features=[feats.index(x) for x in feats if x in colsToBinarize], \
        #             sampling_strategy='auto', n_jobs=-1)
        # X_train, y_train = sm.fit_resample(X_train, y_train)
        print(len(X_train))
        print("=====FITTING CLASSIFIER=====")
        tree_clf = DecisionTreeClassifier(random_state=SEED)
        tree_clf = GridSearchCV(tree_clf, param_grid, scoring="roc_auc")
        tree_clf.fit(X_train, y_train)
        models[target] = tree_clf
        y_pred = tree_clf.predict(X_test)
        scores = tree_clf.predict_proba(X_test)
        print(classification_report(y_test, y_pred))
        CR_dict = classification_report(y_test, y_pred, output_dict=True)
        with open(
            "feature_selection_results/CR_{}_target_{}_ICD_{}_ATC_{}.json".format(
                num_feats, target, ICD_LEVEL, ATC_LEVEL
            ),
            "w",
        ) as fp:
            json.dump(CR_dict, fp)
        # Plots
        plot_ROC_curve(y_test, scores, target)
        plot_PR_curve(y_test, scores, target)
        plot_calibration_curve(
            tree_clf.best_estimator_,
            "Best Tree",
            1,
            X_train,
            X_val,
            X_test,
            y_train,
            y_val,
            y_test,
            "prefit",
            target,
        )
        plt.figure(figsize=(24, 12))
        plt.rcParams["figure.dpi"] = 150
        plot_tree(
            tree_clf.best_estimator_,
            feature_names=human_read_feats(feats),
            fontsize=5,
            filled=False,
            rounded=True,
        )  # left is true
        plt.savefig(
            "feature_selection_results/tree_{}_target_{}_ICD_{}_ATC_{}.png".format(
                num_feats, target, ICD_LEVEL, ATC_LEVEL
            ),
            dpi=150,
        )
        plt.show()
    return models


if __name__ == "__main__":
    # Set PYSPARK_PYTHON
    os.environ[
        "PYSPARK_PYTHON"
    ] = "./temp_envs/py3env/bin/python"  # this has to be here so we can execute a udf

    # Zip Modules
    os.system("zip modules.zip -r modules/*.py")
    spark = SparkSession.builder.appName("OpioidPhenotyping").enableHiveSupport().getOrCreate()

    spark.sparkContext.addPyFile("/home/cdsw/modules.zip")

    try:
        from cchlib import copymerge
    except ImportError:
        raise ImportError("Error importing cchlib module. Run build.py first")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    # Pivot tables and prepare for feature extraction
    column = "visit_occurrence_id"
    base_data_loc = "/projects/cch/opioid-phenotyping/data/interim/"
    ICD_LEVEL = "ALL"
    ATC_LEVEL = "ALL"
    num_feats = 150
    # Get data
    print("Reading in Targets")
    df_kevin, VO_ids = read_in_targets()
    print("Reading in Tables")
    df = get_tables()

    # Save the csv
    print("save the CSV")
    df.to_csv("opioid_feats_df.csv")
    # If we need to read the df, below is the code
    # df = pd.read_csv('opioid_feats_df.csv', index_col=0, parse_dates=['visit_end_datetime'])

    # get age with reference to the visit end date
    df["age"] = df["visit_end_datetime"].apply(lambda x: x.year) - df["BirthYear"]
    del df["BirthYear"]
    del df["visit_end_datetime"]
    # Change column names in df
    print(len([c for c in df.columns.tolist() if "DETBL_" in c]))
    print(len([c for c in df.columns.tolist() if "MEDTBL_" in c]))
    print(len([c for c in df.columns.tolist() if "CCTBL_" in c]))
    print(len([c for c in df.columns.tolist() if "UTTBL_" in c]))
    print(len([c for c in df.columns.tolist() if "BCTBL_" in c]))
    print(len([c for c in df.columns.tolist() if "ACHTBL_" in c]))

    # Binarize columns
    colsToBinarize = list(
        set(
            [c for c in df.columns.tolist() if "DETBL_" in c]
            + [c for c in df.columns.tolist() if "MEDTBL_" in c]
            + [c for c in df.columns.tolist() if "CCTBL_" in c]
            + [c for c in df.columns.tolist() if "UTTBL_" in c]
            + [c for c in df.columns.tolist() if "BCTBL_" in c]
            + [c for c in df.columns.tolist() if "ACHTBL_" in c]
        )
    )
    df[colsToBinarize] = df[colsToBinarize].applymap(lambda x: 1 if x >= 1 else 0)

    # remove ICD codes that show up less than 100 times
    ICDtoDrop = [
        col
        for col, val in df[[x for x in df.columns if "DETBL_" in x]].sum().iteritems()
        if val < 100
    ]
    df = df.drop(ICDtoDrop, axis=1)
    # get the best features and save them to csv
    print("Getting best features")
    get_best_features(num_feats)
    # read the csv file in
    feature_list_dict, feature_df_dict = read_best_features(num_feats)
    icd_codes, ATC = read_ICD_ATC_codes()
    models_dict = train_model()

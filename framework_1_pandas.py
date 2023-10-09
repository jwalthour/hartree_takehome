import pandas as pd
import logging

logger = logging.getLogger(__name__)


def summarize_invoices(
    invoices: pd.DataFrame, tier_for_counterparty: pd.DataFrame
) -> pd.DataFrame:
    """Summarize invoices

    Args:
        invoices (pd.DataFrame): Columns expected:
            invoice_id: int,
            legal_entity: str,
            counter_party: str, index into tier_for_counterparty,
            rating: int,
            status: str,
            value: float
        tier_for_counterparty (pd.DataFrame): Columns expected:
            counter_party: str,
            tier: int

    Returns:
        pd.DataFrame: _description_
    """
    group_cols = ["legal_entity", "counter_party"]
    max_rating = (
        invoices.groupby(group_cols, as_index=False, sort=True)["rating"]
        .max()
        .rename(columns={"rating": "max_rating_by_counterparty"})
    )
    sum_status_arap = (
        invoices.where(invoices["status"] == "ARAP")
        .groupby(group_cols, as_index=False, sort=True)["value"]
        .sum()
        .rename(columns={"value": "sum_value_arap"})
    )
    sum_status_accr = (
        invoices.where(invoices["status"] == "ACCR")
        .groupby(group_cols, as_index=False, sort=True)["value"]
        .sum()
        .rename(columns={"value": "sum_value_accr"})
    )
    # logger.debug("max_rating = \n%r" % max_rating)
    # logger.debug("sum_status_arap = \n%r" % sum_status_arap)
    # logger.debug("sum_status_accr = \n%r" % sum_status_accr)
    summarized_invoices = (
        max_rating.merge(
            sum_status_arap,
            left_on=group_cols,
            right_on=group_cols,
            how="outer",
        )
        .merge(
            sum_status_accr,
            left_on=group_cols,
            right_on=group_cols,
            how="outer",
        )
        .join(tier_for_counterparty, on="counter_party", how="left")
    )

    # Compute cube-like aggregate
    grouping_cols_list = [
        ["legal_entity"],
        ["counter_party"],
        ["tier"],
        ["legal_entity", "counter_party"],
        ["legal_entity", "tier"],
        # ["counter_party", "tier"], # each counterparty is one tier so this would be redundant
    ]
    all_cube_grouping_cols = ["legal_entity", "counter_party", "tier"]

    def pad_keylike_columns(df: pd.DataFrame) -> None:
        """Add "Total" sentinel for any missing key-like columns"""
        sentinel_value = "Total"
        for col_name in all_cube_grouping_cols:
            if col_name not in df.columns:
                # Append column with same row count; column ordering will be addressed later
                df[col_name] = [sentinel_value] * len(df)

    for cube_row_group_cols in grouping_cols_list:
        # logger.debug("grouping by %r" % cube_row_group_cols)
        grouped = summarized_invoices.groupby(
            cube_row_group_cols, as_index=False, sort=True
        )
        # logger.debug("grouped: \n%r" % grouped)
        max_rating = grouped["max_rating_by_counterparty"].max()
        pad_keylike_columns(max_rating)
        # logger.debug("max_rating: \n%r" % max_rating)
        sum_status_arap = grouped["sum_value_arap"].sum()
        pad_keylike_columns(sum_status_arap)
        # logger.debug("sum_status_arap: \n%r" % sum_status_arap)
        sum_status_accr = grouped["sum_value_accr"].sum()
        pad_keylike_columns(sum_status_accr)
        # logger.debug("sum_status_accr: \n%r" % sum_status_accr)
        additional_rows = max_rating.merge(
            sum_status_arap,
            left_on=all_cube_grouping_cols,
            right_on=all_cube_grouping_cols,
            how="outer",
        ).merge(
            sum_status_accr,
            left_on=all_cube_grouping_cols,
            right_on=all_cube_grouping_cols,
            how="outer",
        )
        logger.debug("additional_rows: \n%r" % additional_rows)
        summarized_invoices = pd.concat([summarized_invoices, additional_rows])

    # Reorder columns per the assignment
    summarized_invoices = summarized_invoices[
        [
            "legal_entity",
            "counter_party",
            "tier",
            "max_rating_by_counterparty",
            "sum_value_arap",
            "sum_value_accr",
        ]
    ]
    return summarized_invoices


def main():
    invoices = pd.read_csv("data/dataset1.csv", index_col="invoice_id")
    # logger.debug("invoices = \n%r" % invoices)
    tier_for_counterparty = pd.read_csv("data/dataset2.csv", index_col="counter_party")
    # logger.debug("tier_for_counterparty = \n%r" % tier_for_counterparty)
    summarized_invoices = summarize_invoices(invoices, tier_for_counterparty)
    logger.debug("summarized_invoices = \n%r" % summarized_invoices)
    summarized_invoices.to_csv("output_pandas.csv", index=False)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()

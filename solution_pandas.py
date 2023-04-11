from itertools import combinations

import pandas as pd


COUNTER_PARTY_KEY_RAW = "counter_party"
COUNTER_PARTY_KEY = "counterparty"
LEGAL_ENTITY_KEY = "legal_entity"
TIER_KEY = "tier"
RATING_KEY = "rating"
STATUS_KEY = "status"
VALUE_KEY = "value"


class PandasSolution:
    def __init__(self):
        pass

    def run_calculations(self, x: pd.DataFrame) -> pd.Series:
        return pd.Series(
            {
                "max(rating by counterparty)": x[RATING_KEY].max(),
                "sum(value where status=ARAP)": x[x[STATUS_KEY] == "ARAP"][VALUE_KEY].sum(),
                "sum(value where status=ACCR)": x[x[STATUS_KEY] == "ACCR"][VALUE_KEY].sum(),
            }
        )

    def generate_output(self):
        df1 = pd.read_csv("./dataset1.csv")
        df2 = pd.read_csv("./dataset2.csv")

        df = pd.merge(df1, df2, on=[COUNTER_PARTY_KEY_RAW])
        df.rename(columns={COUNTER_PARTY_KEY_RAW: COUNTER_PARTY_KEY}, inplace=True)

        # We need to determine all unique ways we can group this dataframe based on the 3 non-aggregated fields in the
        # result file
        grouping_keys = [LEGAL_ENTITY_KEY, COUNTER_PARTY_KEY, TIER_KEY]
        grouped_keys = []
        for combination_length in range(0, len(grouping_keys) + 1):
            possible_groupings = list(combinations(grouping_keys, r=combination_length))
            grouped_keys.extend(possible_groupings)
        grouped_keys = [list(g) for g in grouped_keys]

        final_df = pd.DataFrame()
        for grouping in grouped_keys:
            if len(grouping) > 0:
                grouped_df = df.groupby(by=grouping, as_index=False).apply(self.run_calculations)
                missing_fields = set(grouping_keys).difference(set(grouping))
                for missing_field in missing_fields:
                    grouped_df[missing_field] = "Total"
            else:
                grouped_df = pd.DataFrame(self.run_calculations(df)).T
                grouped_df[LEGAL_ENTITY_KEY] = "Total"
                grouped_df[COUNTER_PARTY_KEY] = "Total"
                grouped_df[TIER_KEY] = "Total"

            final_df = pd.concat([final_df, grouped_df], axis="rows")

        # We re-order the final dataframe in preparation for writing
        final_df = final_df[
            grouping_keys + ["max(rating by counterparty)", "sum(value where status=ARAP)", "sum(value where status=ACCR)"]
        ]
        final_df.to_csv("result_pandas.csv", index=False)


if __name__ == "__main__":
    s = PandasSolution()
    s.generate_output()

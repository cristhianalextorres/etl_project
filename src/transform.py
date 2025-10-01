import pandas as pd

class Transform:
    def clean_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        return df

    def drop_nulls(self, df: pd.DataFrame, subset: list = None) -> pd.DataFrame:
        return df.dropna(subset=subset)

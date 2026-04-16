import pandas as pd
from typing import Optional

def _sample(df: pd.DataFrame, max_rows: int, stratify_col: Optional[str] = None) -> pd.DataFrame:
    if len(df) <= max_rows:
        return df
    if stratify_col and stratify_col in df.columns:
        try:
            sampled_idx = []
            for name, group in df.groupby(stratify_col):
                n_sample = min(len(group), max(1, int(max_rows * len(group) / len(df))))
                sampled_idx.extend(group.sample(n_sample, random_state=42).index)
            
            sampled_df = df.loc[sampled_idx]
            
            if len(sampled_df) > max_rows:
                sampled_df = sampled_df.sample(max_rows, random_state=42)
                
            return sampled_df.reset_index(drop=True)
        except Exception as e:
            print("Error in _sample:", e)
            pass
    return df.sample(max_rows, random_state=42).reset_index(drop=True)

df = pd.DataFrame({
    'notifications_per_day': [10, 20, 10, 20, 30, 40, 50, 60],
    'addicted_label': ['Yes', 'No', 'Yes', 'No', 'Yes', 'No', 'No', 'No']
})

print("Original columns:", df.columns.tolist())
sampled = _sample(df, max_rows=3, stratify_col='addicted_label')
print("Sampled columns:", sampled.columns.tolist())
print(sampled)

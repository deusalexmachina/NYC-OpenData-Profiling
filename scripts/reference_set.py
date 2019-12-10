"""
create reference set for task2 matching by grouping by true_label (semantic type) to top n frequent values based on their count

{true_label: [top_n frequent values]}
"""

from os import makedirs
from os.path import isdir
import pandas as pd

df = pd.read_csv('Task 2 - top10_frequent_vals.csv')

df['true_label'] = df['col_name'].map(lambda x: x.strip().strip(',').split(', '))

df_flat = df.explode('true_label')
df_flat['frequent_values->counts_(top_10)'] = df_flat['frequent_values->counts_(top_10)'].map(lambda x: [tuple(s.split('->')) for s in eval(x)])
df_flatter = df_flat.explode('frequent_values->counts_(top_10)')
df_flatter[['frequent_value', 'count']] = df_flatter['frequent_values->counts_(top_10)'].apply(lambda x: pd.Series([x[0], int(x[1])]))


def create_reference_set(df_flatter, kind, top_n=20):
    """
    groupby true_label and kind to get counts and then limit to the top_n counts
    """
    df_grouped = df_flatter.groupby(['true_label', kind]).agg({'count': sum})
    df_grouped = df_grouped.groupby(level=0).apply(
        lambda df: df.sort_index(by='count', ascending=False)[:top_n])
    df_grouped.index = df_grouped.index.droplevel(0)
    df_grouped.reset_index(inplace=True)

    return df_grouped


base_path = 'results_reference_sets'

if not isdir(base_path):
    makedirs(base_path)

df_grouped_values = create_reference_set(df_flatter, 'frequent_value')
df_grouped_values.to_csv(f'{base_path}/reference_set_values.csv', index=False)

df_grouped_cols = create_reference_set(df_flatter, 'col_name_ta')
df_grouped_cols.to_csv(f'{base_path}/reference_set_columns.csv', index=False)

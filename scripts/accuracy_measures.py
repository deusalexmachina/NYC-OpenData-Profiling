import pandas as pd

df = pd.read_csv('Task 2 - top10_frequent_vals.csv')
df.rename({'col_name': 'true_label'}, axis=1, inplace=True)
df['true_label'] = df['true_label'].map(lambda x: x.strip().replace(' ', '').split(','))
df['ds_name_ta'] = [ds_name[:ds_name.index('.')] + '.' + col_name +'.txt.gz' for ds_name, col_name in list(zip(df['ds_path'], df['col_name_ta']))]
df = df.explode('true_label')

df_results = pd.read_json('task2.json')
df_results = df_results.explode('semantic_types')
df_results['semantic_type'] = df_results['semantic_types'].apply(lambda x: pd.Series(x['semantic_type']))

df_work = df[['ds_name_ta', 'true_label']].set_index('ds_name_ta').join(df_results[['column_name', 'semantic_type']].set_index('column_name'), how='outer')
precision = (df_work[df_work['true_label'] == df_work['semantic_type']].groupby('semantic_type').count() / df_work.groupby('semantic_type').count()).fillna(0)
actual_cols = df_work['true_label'].reset_index().groupby('true_label').count().rename({'index':'true_label'}, axis=1)
actual_cols.index.rename('semantic_type', inplace=True)
recall = (df_work[df_work['true_label'] == df_work['semantic_type']].groupby('semantic_type').count() / actual_cols).fillna(0)

precision.to_csv('precision.csv')
recall.to_csv('recall.csv')

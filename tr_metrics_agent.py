from pandas import read_sql_query, pivot_table, concat
# from lib.lib_reporting import *
# from lib.engine_1stdibs import *
from sqlalchemy import text
import pandas as pd

# path_tr_metrics = join(path_reporting + '11_tr_metrics_agent/')
# sql_tr_metrics = join(path_tr_metrics, 'tr_metrics_agent.sql')
# sql_tr_metrics = prepare_file_sql(sql_tr_metrics)

# tr_metrics = read_sql_query(sql=text(sql_tr_metrics), con=engine_1stdibs.connect())
tr_metrics = pd.read_csv('tr_metrics_dataframe_raw.csv')

# tr_metrics['Year'] = tr_metrics['created_date'].apply(lambda x: x.strftime('%Y'))
# tr_metrics['Month'] = tr_metrics['created_date'].apply(lambda x: x.strftime('%m'))
# tr_metrics['Week'] = tr_metrics['created_date'].apply(lambda x: x.isocalendar()[1])
# tr_metrics['Day'] = tr_metrics['created_date'].apply(lambda x: x.strftime('%d'))

# # Updating OMT
# tr_metrics['OMT'] = '=HYPERLINK("https://adminv2.1stdibs.com/internal/omt/order/'
# tr_metrics['OMT'] = tr_metrics['OMT'] + tr_metrics['id'].astype(str) + '","OMT")'

# # Converting date format for JSON and Google API
# tr_metrics['created_date'] = tr_metrics['created_date'].dt.strftime('%m/%d/%Y')
# tr_metrics['created_date'] = tr_metrics['created_date'].astype('str')

# tr_metrics['latest_buyer_total'].fillna(0, inplace=True)

tr_metrics = tr_metrics[['Year', 'Month', 'dibs_U_id', 'status', 'id']].copy()

distinct_ids = list(tr_metrics['dibs_U_id'].unique())
distinct_ids.sort()

min_year = tr_metrics['Year'].min()
min_month = tr_metrics['Month'].min()

for id in distinct_ids:
    temp_df = pd.DataFrame()
    for status in ['Approved', 'Rejected', 'Canceled']:
        temp_df['Year'] = [min_year]
        temp_df['Month'] = [min_month]
        temp_df['dibs_U_id'] = [id]
        temp_df['status'] = [status]
        temp_df['id'] = [None]
        tr_metrics = concat([tr_metrics, temp_df], axis=0)

final = pd.DataFrame()

pivoted_grand_approved = tr_metrics[tr_metrics['status'] == 'Approved']
pivoted_grand_canceled = tr_metrics[tr_metrics['status'] == 'Canceled']
pivoted_grand_rejected = tr_metrics[tr_metrics['status'] == 'Rejected']

# Pivot the DataFrame to transform it into the desired output format
for ids in distinct_ids:
    df2 = tr_metrics[tr_metrics['dibs_U_id'] == ids]
    pivoted = df2.pivot_table(
        index=['dibs_U_id', 'status'],
        columns=['Year', 'Month'],
        values='id',
        aggfunc='count',
        fill_value=0,
        margins=True,
        margins_name='Total'
    )
    final = concat([final, pivoted], axis=0)

pivoted_grand = tr_metrics.pivot_table(
    index=['dibs_U_id', 'status'],
    columns=['Year', 'Month'],
    values='id',
    aggfunc='count',
    fill_value=0
)

pivoted_grand_approved = pivoted_grand_approved.pivot_table(
    index=['dibs_U_id', 'status'],
    columns=['Year', 'Month'],
    values='id',
    aggfunc='count',
    fill_value=0
    )

pivoted_grand_canceled = pivoted_grand_canceled.pivot_table(
    index=['dibs_U_id', 'status'],
    columns=['Year', 'Month'],
    values='id',
    aggfunc='count',
    fill_value=0
    )

pivoted_grand_rejected = pivoted_grand_rejected.pivot_table(
    index=['dibs_U_id', 'status'],
    columns=['Year', 'Month'],
    values='id',
    aggfunc='count',
    fill_value=0
    )

final = final._append(pivoted_grand_approved.sum().rename(('Grand Total', 'Approved')))
final = final._append(pivoted_grand_canceled.sum().rename(('Grand Total', 'Canceled')))
final = final._append(pivoted_grand_rejected.sum().rename(('Grand Total', 'Rejected')))
final = final._append(pivoted_grand.sum().rename(('Grand Total', '')))
final = final.fillna(0)
final.sort_index(axis=1)
final = final.drop(['Total'], axis=1, level=0)
final.sort_index(axis='columns',level= ['Year','Month'],inplace = True)

print(final)
final.to_excel('tr_metrics_dataframe_report.xlsx')
# update_worksheet(final, 'Transaction Review - Metrics', 'Manual Review - OMT - Agent', 'C3')

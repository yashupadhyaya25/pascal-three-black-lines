from pandas import read_sql_query, pivot_table, concat
from lib.lib_reporting import *
from lib.engine_1stdibs import *
from sqlalchemy import text
import pandas as pd

path_tr_metrics = join(path_reporting + '11_metrics_omt/')
sql_tr_metrics = join(path_tr_metrics, 'metrics_omt.sql')
sql_tr_metrics = prepare_file_sql(sql_tr_metrics)

tr_metrics = read_sql_query(sql=text(sql_tr_metrics), con=engine_1stdibs.connect())

tr_metrics['Year'] = tr_metrics['created_date'].apply(lambda x: x.strftime('%Y'))
tr_metrics['Month'] = tr_metrics['created_date'].apply(lambda x: x.strftime('%m'))
tr_metrics['Week'] = tr_metrics['created_date'].apply(lambda x: x.isocalendar()[1])
tr_metrics['Day'] = tr_metrics['created_date'].apply(lambda x: x.strftime('%d'))

# Updating OMT
tr_metrics['OMT'] = '=HYPERLINK("https://adminv2.1stdibs.com/internal/omt/order/'
tr_metrics['OMT'] = tr_metrics['OMT'] + tr_metrics['id'].astype(str) + '","OMT")'

# Converting date format for JSON and Google API
tr_metrics['created_date'] = tr_metrics['created_date'].dt.strftime('%m/%d/%Y')
tr_metrics['created_date'] = tr_metrics['created_date'].astype('str')

tr_metrics['latest_buyer_total'].fillna(0, inplace=True)

# tr_metrics = pd.read_csv("G:/.shortcut-targets-by-id/10c_wbYjXg65CblXAdT6kdB7EyX88MQSk/0PI_PROJECTS/Pascal/Development/raw.csv")

tr_metrics = tr_metrics[['Year','Month', 'status', 'id']].copy()

min_year = tr_metrics['Year'].min()
min_month = tr_metrics['Month'].min()

tr_metrics = tr_metrics.replace('Approved', 'Approved Total')
tr_metrics = tr_metrics.replace('Canceled', 'Canceled Total')
tr_metrics = tr_metrics.replace('Rejected', 'Rejected Total')

pivoted_grand_rejected = tr_metrics[tr_metrics['status'] == 'Rejected Total']
pivoted_grand_canceled = tr_metrics[tr_metrics['status'] == 'Canceled Total']
pivoted_grand_approved = tr_metrics[tr_metrics['status'] == 'Approved Total']
pivoted_grand_rejected_canceled = pd.concat([pivoted_grand_rejected, pivoted_grand_canceled], axis=0)

final = pd.DataFrame()

pivoted_grand_total = tr_metrics.pivot_table(
        index=[ 'status'],
        columns=['Year', 'Month'],
        values='id',
        aggfunc='count',
        fill_value=0
    )

pivoted_grand_approved = pivoted_grand_approved.pivot_table(
        index=[ 'status'],
        columns=['Year', 'Month'],
        values='id',
        aggfunc='count',
        fill_value=0
    )

pivoted_grand_canceled = pivoted_grand_canceled.pivot_table(
        index=[ 'status'],
        columns=['Year', 'Month'],
        values='id',
        aggfunc='count',
        fill_value=0
    )

pivoted_grand_rejected = pivoted_grand_rejected.pivot_table(
        index=[ 'status'],
        columns=['Year', 'Month'],
        values='id',
        aggfunc='count',
        fill_value=0
    )

pivoted_grand_rejected_canceled = pivoted_grand_rejected_canceled.pivot_table(
        index=[ 'status'],
        columns=['Year', 'Month'],
        values='id',
        aggfunc='count',
        fill_value=0
    )

final = final._append(pivoted_grand_approved.sum().rename(('Grand Total', 'Approved Total')))
final = final._append(pivoted_grand_canceled.sum().rename(('Grand Total', 'Canceled Total')))
final = final._append(pivoted_grand_rejected.sum().rename(('Grand Total', 'Rejected Total')))
final = final._append(pivoted_grand_total.sum().rename(('Grand Total', 'Grand Total')))
final = final._append((pivoted_grand_rejected_canceled.sum().rename(('Grand Total', 'Decline Ratio'))
                       / pivoted_grand_total.sum().rename(('Grand Total', 'Decline Ratio'))))
final = final.fillna(0)
final.sort_index(axis=1)

final.sort_index(axis='columns', level=['Year', 'Month'], inplace=True)

print(final)

update_worksheet(final, 'Transaction Metrics - OMT', 'Manual Review - OMT - Global', 'C3')

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

# tr_metrics = pd.read_csv("20_metrics_omt_agent/raw.csv")
# tr_metrics = pd.read_csv("raw.csv")

tr_metrics = tr_metrics[['Year', 'Month', 'dibs_U_firstname', 'status', 'id']].copy()

distinct_ids = list(tr_metrics['dibs_U_firstname'].unique())
distinct_ids.sort()

min_year = tr_metrics['Year'].min()
min_month = tr_metrics['Month'].min()

max_year = tr_metrics['Year'].max()
max_month = tr_metrics['Month'].max()

def create_date_list(min_year_month, max_year_month):
    # Extract minimum year and month values
    min_year = int(min_year_month[:4])
    min_month = int(min_year_month[4:])

    # Extract maximum year and month values
    max_year = int(max_year_month[:4])
    max_month = int(max_year_month[4:])

    # Create list of dates
    dates = []
    year = min_year
    month = min_month
    while year <= max_year:
        while month <= 12:
            dates.append(f"{year}{month:02}")
            if year == max_year and month == max_month:
                return dates
            month += 1
        month = 1
        year += 1
    return dates

date_list = create_date_list(str(min_year)+str(min_month),str(max_year)+str(max_month))

for id in distinct_ids:
    temp_df = pd.DataFrame()
    for status in ['Approved', 'Rejected', 'Canceled']:
        for year_month in date_list :
            temp_df['Year'] = [(str(year_month)[0:4])]
            temp_df['Month'] = [(str(year_month)[-2:])]
            # temp_df['Year'] = [int(str(year_month)[0:4])]
            # temp_df['Month'] = [int(str(year_month)[-2:])]
            temp_df['dibs_U_firstname'] = [id]
            temp_df['status'] = [status]
            temp_df['id'] = [None]
            tr_metrics = concat([tr_metrics, temp_df], axis=0)

final = pd.DataFrame()

pivoted_grand_approved = tr_metrics[tr_metrics['status'] == 'Approved']
pivoted_grand_canceled = tr_metrics[tr_metrics['status'] == 'Canceled']
pivoted_grand_rejected = tr_metrics[tr_metrics['status'] == 'Rejected']

# Pivot the DataFrame to transform it into the desired output format
for ids in distinct_ids:
    df2 = tr_metrics[tr_metrics['dibs_U_firstname'] == ids]
    pivoted = df2.pivot_table(
        index=['dibs_U_firstname', 'status'],
        columns=['Year', 'Month'],
        values='id',
        aggfunc='count',
        fill_value=0,
        # margins=True,
        # margins_name='Total'
    )

    pivoted_sub_grand_canceled = df2[df2['status'] == 'Canceled']
    pivoted_sub_grand_rejected = df2[df2['status'] == 'Rejected']

    pivoted_sub_grand_canceled = pivoted_sub_grand_canceled.pivot_table(
        index=["dibs_U_firstname","status"],
        columns=["Year", "Month"],
        values="id",
        aggfunc="count",
        fill_value=0
    )


    pivoted_sub_grand_rejected = pivoted_sub_grand_rejected.pivot_table(
        index=["dibs_U_firstname","status"],
        columns=["Year", "Month"],
        values="id",
        aggfunc="count",
        fill_value=0
    )

    pivoted_sub_grand_total = df2.pivot_table(
        index=["dibs_U_firstname","status"],
        columns=["Year", "Month"],
        values="id",
        aggfunc="count",
        fill_value=0
    )

    pivoted_sub_grand_rejected_canceled = pd.concat([pivoted_sub_grand_canceled,pivoted_sub_grand_rejected],axis=0)
    pivoted_sub_grand_rejected_canceled = pivoted_sub_grand_rejected_canceled.fillna(0)

    final = concat([final, pivoted], axis=0)
    final = final.fillna(0)
    final = final._append(pivoted_sub_grand_total.sum().rename((ids, 'Total')))
    
    final = final._append((pivoted_sub_grand_rejected_canceled.sum().rename((ids, 'Decline Ratio'))
                        /pivoted_sub_grand_total.sum().rename((ids, 'Decline Ratio'))*100).round(2).fillna(0).astype(str)+'%')

pivoted_grand = tr_metrics.pivot_table(
    index=['dibs_U_firstname', 'status'],
    columns=['Year', 'Month'],
    values='id',
    aggfunc='count',
    fill_value=0
)

pivoted_grand_approved = pivoted_grand_approved.pivot_table(
    index=['dibs_U_firstname', 'status'],
    columns=['Year', 'Month'],
    values='id',
    aggfunc='count',
    fill_value=0
    )

pivoted_grand_canceled = pivoted_grand_canceled.pivot_table(
    index=['dibs_U_firstname', 'status'],
    columns=['Year', 'Month'],
    values='id',
    aggfunc='count',
    fill_value=0
    )

pivoted_grand_rejected = pivoted_grand_rejected.pivot_table(
    index=['dibs_U_firstname', 'status'],
    columns=['Year', 'Month'],
    values='id',
    aggfunc='count',
    fill_value=0
    )

pivoted_grand_rejected_canceled = pd.concat([pivoted_grand_canceled,pivoted_grand_rejected],axis=0)

final = final._append(pivoted_grand_approved.sum().rename(('Grand Total', 'Approved')))
final = final._append(pivoted_grand_canceled.sum().rename(('Grand Total', 'Canceled')))
final = final._append(pivoted_grand_rejected.sum().rename(('Grand Total', 'Rejected')))
final = final._append(pivoted_grand.sum().rename(('Grand Total', 'Total')))
final = final._append((pivoted_grand_rejected_canceled.sum().rename(('Grand Total', 'Decline Ratio'))
                       /pivoted_grand.sum().rename(('Grand Total', 'Decline Ratio'))*100).round(2).fillna(0).astype(str)+'%')
final = final.fillna(0)
final.sort_index(axis=1)
final.sort_index(axis='columns', level=['Year', 'Month'], inplace=True)

print(final)
# final.to_excel('20_metrics_omt_agent/20_metrics_omt_agent.xlsx')

update_worksheet(final, 'Transaction Metrics - OMT - Team', 'Manual Review - OMT - Agent', 'C3')

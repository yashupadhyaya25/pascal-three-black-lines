# from lib.lib_braintree import *
import pandas as pd

# braintree = load_braintree_csv_all()

# braintree.loc[(braintree['Shipping Country'] == 'United States of America'), 'Country'] = 'US'
# braintree.loc[braintree['Shipping Country'].isna(), 'Country'] = 'Unknown'
# braintree['Country'].fillna('Intl', inplace=True)

# braintree.loc[(braintree['Processor Response Text'] == 'Approved'), 'Response'] = 'Approved'
# braintree['Response'].fillna('Declined', inplace=True)

# braintree['Year'] = braintree['Created Datetime'].apply(lambda x: x.strftime('%Y'))
# braintree['Month'] = braintree['Created Datetime'].apply(lambda x: x.strftime('%m'))

braintree = pd.read_csv('braintree.csv')

braintree = braintree[['Year', 'Month', 'Country', 'Response', 'Transaction ID']].copy()

distinct_ids = ['US', 'Intl', 'Unknown']

min_year = braintree['Year'].min()
min_month = braintree['Month'].min()

for id in distinct_ids:
    temp_df = pd.DataFrame()
    for status in ['US', 'Intl', 'Unknown']:
        temp_df['Year'] = [min_year]
        temp_df['Month'] = [min_month]
        temp_df['Country'] = [id]
        temp_df['Response'] = [status]
        temp_df['c'] = [None]
        tr_metrics = pd.concat([braintree, temp_df], axis=0)

final = pd.DataFrame()

pivoted_grand_approved = braintree[braintree['Response'] == 'Approved']
pivoted_grand_declined = braintree[braintree['Response'] == 'Declined']

# Pivot the DataFrame to transform it into the desired output format
for ids in distinct_ids:
    df2 = braintree[braintree['Country'] == ids]
    if not(df2.empty) :
        pivoted = df2.pivot_table(
            index=['Country', 'Response'],
            columns=['Year', 'Month'],
            values='Transaction ID',
            aggfunc='count',
            fill_value=0,
            # margins=True,
            # margins_name='Total'
        )
        
        pivoted_sub_declined = df2[df2['Response'] == 'Declined']
        
        pivoted_sub_declined = pivoted_sub_declined.pivot_table(
            index=["Country","Response"],
            columns=["Year", "Month"],
            values="Transaction ID",
            aggfunc="count",
            fill_value=0
        )
        
        pivoted_sub_declined = pivoted_sub_declined.fillna(0)
        
        pivoted_sub_grand_total = df2.pivot_table(
            index=["Country","Response"],
            columns=["Year", "Month"],
            values="Transaction ID",
            aggfunc="count",
            fill_value=0
        )
        
        pivoted_sub_grand_total = pivoted_sub_grand_total.fillna(0)
        
        final = pd.concat([final, pivoted], axis=0)
        final = final._append(pivoted_sub_grand_total.sum().rename((ids, 'Total')))
        final = final._append((pivoted_sub_declined.sum().rename((ids, 'Decline Ratio'))
                   /pivoted_sub_grand_total.sum().rename((ids, 'Decline Ratio'))*100).round(2).astype(str)+'%')
    
pivoted_grand = braintree.pivot_table(
    index=['Country', 'Response'],
    columns=['Year', 'Month'],
    values='Transaction ID',
    aggfunc='count',
    fill_value=0
)

pivoted_grand_approved = pivoted_grand_approved.pivot_table(
    index=['Country', 'Response'],
    columns=['Year', 'Month'],
    values='Transaction ID',
    aggfunc='count',
    fill_value=0
    )

pivoted_grand_declined = pivoted_grand_declined.pivot_table(
    index=['Country', 'Response'],
    columns=['Year', 'Month'],
    values='Transaction ID',
    aggfunc='count',
    fill_value=0
    )

final = final._append(pivoted_grand_approved.sum().rename(('Grand Total', 'Approved')))
final = final._append(pivoted_grand_declined.sum().rename(('Grand Total', 'Declined')))
final = final._append(pivoted_grand.sum().rename(('Grand Total', 'Grand Total')))
final = final._append((pivoted_grand_declined.sum().rename(('Grand Total', 'Decline Ratio'))
                       /pivoted_grand.sum().rename(('Grand Total', 'Decline Ratio'))*100).round(2).astype(str)+'%')

final = final.fillna(0)
final.sort_index(axis=1)
# final = final.drop(['Total'], axis=1, level=0)
final.sort_index(axis='columns', level=['Year', 'Month'], inplace=True)
final.to_excel('braintree_report.xlsx')
print(final)

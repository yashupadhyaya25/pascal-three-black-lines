from lib.lib_braintree import *
import pandas as pd

braintree = load_braintree_csv_all()

braintree.loc[(braintree['Shipping Country'] == 'United States of America'), 'Country'] = 'US'
braintree.loc[braintree['Shipping Country'].isna(), 'Country'] = 'Unknown'
braintree.loc[braintree['Card Type'].isna(), 'Card Type'] = braintree['Payment Instrument Type']
braintree['Country'].fillna('Intl', inplace=True)

braintree.loc[(braintree['Processor Response Text'] == 'Approved'), 'Response'] = 'Approved'
braintree['Response'].fillna('Declined', inplace=True)



braintree['Year'] = braintree['Created Datetime'].apply(lambda x: x.strftime('%Y'))
braintree['Month'] = braintree['Created Datetime'].apply(lambda x: x.strftime('%m'))


braintree = braintree[['Year', 'Month', 'Country', 'Response',
                       'Payment Instrument Type', 'Card Type', 'Transaction ID']].copy()

distinct_ids = list(braintree['Country'].unique())
distinct_PIT = list(braintree['Payment Instrument Type'].unique())
distinct_CT = list(braintree['Card Type'].unique())

min_year = braintree['Year'].min()
min_month = braintree['Month'].min()

main_df = pd.DataFrame()

for id in distinct_ids:    
    for status in ['Approved','Declined']:
        for PIT in distinct_PIT:
            for CT in distinct_CT:
                temp_df = pd.DataFrame()
                temp_df['Year'] = [min_year]
                temp_df['Month'] = [min_month]
                temp_df['Country'] = [id]
                temp_df['Response'] = [status]
                temp_df['Payment Instrument Type'] = [PIT]
                temp_df['Card Type'] = [CT]
                temp_df['Transaction ID'] = [None]
                main_df = pd.concat([main_df, temp_df], axis=0)

braintree = pd.concat([braintree,main_df],axis=0)

final = pd.DataFrame()
sub_final = pd.DataFrame()
# Pivot the DataFrame to transform it into the desired output format
for ids in distinct_ids:
    country_df = braintree[braintree['Country'] == ids]
    for PIT in distinct_PIT:
        PIT_df = country_df[country_df['Payment Instrument Type'] == PIT]
        for CT in distinct_CT:
            CT_df = PIT_df[PIT_df['Card Type'] == CT]
            pivoted = CT_df.pivot_table(
                index=['Country','Payment Instrument Type','Card Type','Response'],
                columns=['Year', 'Month'],
                values='Transaction ID',
                aggfunc='count',
                fill_value=0,
            )
            
            
            pivoted_sub_grand_total = CT_df.pivot_table(
                    index=['Country','Payment Instrument Type','Card Type','Response'],
                    columns=['Year', 'Month'],
                    values='Transaction ID',
                    aggfunc='count',
                    fill_value=0,
            )
            
            pivoted_sub_grand_decliend = CT_df[CT_df['Response'] == 'Declined']
            pivoted_sub_grand_decliend = pivoted_sub_grand_decliend.pivot_table(
                index=['Country','Payment Instrument Type','Card Type','Response'],
                columns=['Year', 'Month'],
                values='Transaction ID',
                aggfunc='count',
                fill_value=0,
            )

            sub_final = pd.concat([sub_final, pivoted], axis=0)
            sub_final = sub_final._append(pivoted_sub_grand_total.sum().rename((ids,PIT,CT,'Total')))

            sub_final = sub_final._append((pivoted_sub_grand_decliend.sum().rename((ids,PIT,CT, 'Decline Ratio'))
                       /pivoted_sub_grand_total.sum().rename((ids,PIT,CT,'Decline Ratio'))*100).round(2).astype(str)+'%')

            final = pd.concat([final, sub_final], axis=0)
            
final = final.fillna(0).replace('nan%',0)
print(final)

quit()



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
    if not (df2.empty):
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
            index=["Country", "Response"],
            columns=["Year", "Month"],
            values="Transaction ID",
            aggfunc="count",
            fill_value=0
        )

        pivoted_sub_declined = pivoted_sub_declined.fillna(0)

        pivoted_sub_grand_total = df2.pivot_table(
            index=["Country", "Response"],
            columns=["Year", "Month"],
            values="Transaction ID",
            aggfunc="count",
            fill_value=0
        )

        pivoted_sub_grand_total = pivoted_sub_grand_total.fillna(0)

        final = pd.concat([final, pivoted], axis=0)
        final = final._append(pivoted_sub_grand_total.sum().rename((ids, 'Total')))
        final = final._append((pivoted_sub_declined.sum().rename((ids, 'Decline Ratio'))
                               / pivoted_sub_grand_total.sum().rename((ids, 'Decline Ratio')) * 100).round(2).astype(
            str) + '%')

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
                       / pivoted_grand.sum().rename(('Grand Total', 'Decline Ratio')) * 100).round(2).astype(str) + '%')

final = final.fillna(0)
final.sort_index(axis=1)
# final = final.drop(['Total'], axis=1, level=0)
final.sort_index(axis='columns', level=['Year', 'Month'], inplace=True)

print(final)

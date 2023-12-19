from lib.lib_braintree import *
import pandas as pd

braintree = load_braintree_csv_all()

braintree.loc[braintree['Card Type'].isna(), 'Card Type'] = braintree['Payment Instrument Type']
braintree.loc[(braintree['Processor Response Text'] == 'Approved'), 'Response'] = 'Approved'
braintree['Response'].fillna('Declined', inplace=True)

braintree['Year'] = braintree['Created Datetime'].apply(lambda x: x.strftime('%Y'))
braintree['Month'] = braintree['Created Datetime'].apply(lambda x: x.strftime('%m'))

braintree = braintree[['Year', 'Month', 'Payment Instrument Type', 'Card Type', 'Response', 'Transaction ID']].copy()

distinct_PIT = list(braintree['Payment Instrument Type'].unique())
distinct_CT = list(braintree['Card Type'].unique())

min_year = braintree['Year'].min()
min_month = braintree['Month'].min()

max_year = braintree['Year'].max()
max_month = braintree['Month'].max()


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


date_list = create_date_list(str(min_year) + str(min_month), str(max_year) + str(max_month))

main_df = pd.DataFrame()

for status in ['Approved', 'Declined']:
    for PIT in distinct_PIT:
        PIT_distinct_CT = braintree[braintree['Payment Instrument Type'] == PIT]
        PIT_distinct_CT = list(PIT_distinct_CT['Card Type'].unique())
        for CT in PIT_distinct_CT:
            for year_month in date_list:
                temp_df = pd.DataFrame()
                temp_df['Year'] = [str(year_month)[0:4]]
                temp_df['Month'] = [str(year_month)[-2:]]
                temp_df['Country'] = [id]
                temp_df['Response'] = [status]
                temp_df['Payment Instrument Type'] = [PIT]
                temp_df['Card Type'] = [CT]
                temp_df['Transaction ID'] = [None]
                main_df = pd.concat([main_df, temp_df], axis=0)

braintree = pd.concat([braintree, main_df], axis=0)

final = pd.DataFrame()

for PIT in distinct_PIT:
    PIT_df = braintree[braintree['Payment Instrument Type'] == PIT]
    PIT_df_approved = PIT_df[PIT_df['Response'] == 'Approved']
    PIT_df_declined = PIT_df[PIT_df['Response'] == 'Declined']
    PIT_distinct_CT = braintree[braintree['Payment Instrument Type'] == PIT]
    PIT_distinct_CT = list(PIT_distinct_CT['Card Type'].unique())
    PIT_distinct_CT.sort()
    for CT in PIT_distinct_CT:
        sub_final = pd.DataFrame()

        CT_df = PIT_df[PIT_df['Card Type'] == CT]
        CT_df_declined = CT_df[CT_df['Response'] == 'Declined']

        pivoted = CT_df.pivot_table(
            index=['Payment Instrument Type', 'Card Type', 'Response'],
            columns=['Year', 'Month'],
            values='Transaction ID',
            aggfunc='count',
            fill_value=0,
        )

        sub_df_decline_pivoted = CT_df_declined.pivot_table(
            index=['Payment Instrument Type', 'Card Type', 'Response'],
            columns=['Year', 'Month'],
            values='Transaction ID',
            aggfunc='count',
            fill_value=0,
        )

        sub_final = pd.concat([sub_final, pivoted], axis=0)

        final = pd.concat([final, sub_final], axis=0)

        final = final._append(pivoted.sum().rename((PIT, CT, 'Total')))
        final = final._append((sub_df_decline_pivoted.sum().rename((PIT, CT, 'Decline Ratio'))
                               / pivoted.sum().rename((PIT, CT, 'Decline Ratio')) * 100).round(2).fillna(0).astype(
            str) + '%')

    if PIT == 'Credit Card' or PIT == "Apple Pay Card":
        sub_total_pivoted = PIT_df.pivot_table(
            index=['Payment Instrument Type', 'Card Type', 'Response'],
            columns=['Year', 'Month'],
            values='Transaction ID',
            aggfunc='count',
            fill_value=0,
        )

        PIT_total_df_approved = PIT_df_approved.pivot_table(
            index=['Payment Instrument Type', 'Card Type', 'Response'],
            columns=['Year', 'Month'],
            values='Transaction ID',
            aggfunc='count',
            fill_value=0,
        )

        PIT_total_df_declined = PIT_df_declined.pivot_table(
            index=['Payment Instrument Type', 'Card Type', 'Response'],
            columns=['Year', 'Month'],
            values='Transaction ID',
            aggfunc='count',
            fill_value=0,
        )

        final = final._append(PIT_total_df_approved.sum().rename((PIT + ' Subtotal', ' ', 'Approved')))
        final = final._append(PIT_total_df_declined.sum().rename((PIT + ' Subtotal', ' ', 'Declined')))
        final = final._append(sub_total_pivoted.sum().rename((PIT + ' Subtotal', ' ', 'Total')))
        final = final._append((PIT_total_df_declined.sum().rename((PIT + ' Subtotal', ' ', 'Decline Ratio'))
                               / sub_total_pivoted.sum().rename((PIT + ' Subtotal', ' ', 'Decline Ratio')) * 100).round(
            2).fillna(0).astype(str) + '%')

pivoted_grand_approved = braintree[braintree['Response'] == 'Approved']
pivoted_grand_declined = braintree[braintree['Response'] == 'Declined']

pivoted_grand = braintree.pivot_table(
    index=['Payment Instrument Type', 'Card Type', 'Response'],
    columns=['Year', 'Month'],
    values='Transaction ID',
    aggfunc='count',
    fill_value=0,
)

pivoted_grand_declined = pivoted_grand_declined.pivot_table(
    index=['Payment Instrument Type', 'Card Type', 'Response'],
    columns=['Year', 'Month'],
    values='Transaction ID',
    aggfunc='count',
    fill_value=0,
)

pivoted_grand_approved = pivoted_grand_approved.pivot_table(
    index=['Payment Instrument Type', 'Card Type', 'Response'],
    columns=['Year', 'Month'],
    values='Transaction ID',
    aggfunc='count',
    fill_value=0,
)

final = final._append(pivoted_grand_approved.sum().rename(('Grand Total', ' ', 'Approved')))
final = final._append(pivoted_grand_declined.sum().rename(('Grand Total', ' ', 'Declined')))
final = final._append(pivoted_grand.sum().rename(('Grand Total', ' ', 'Total')))
final = final._append((pivoted_grand_declined.sum().rename(('Grand Total', ' ', 'Decline Ratio'))
                       / pivoted_grand.sum().rename(('Grand Total', ' ', 'Decline Ratio')) * 100).round(
    2).fillna(0).astype(str) + '%')

update_worksheet(final, 'Transaction Metrics - Payments', 'Payment Method - Global', 'D3')

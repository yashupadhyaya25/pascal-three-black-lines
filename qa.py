from pandas import pivot_table, concat
# from lib.lib_qa import *
import pandas as pd

# qa = load_qa_csv()
# qa = qa[['canceled_year', 'canceled_month', 'hold_reason', 'confirmed_fraud', 'missing_escalation',
#          'payment_method', 'latest_buyer_price']]

# qa.to_csv('qa.csv', index=False)

# print(qa)
qa = pd.read_csv('qa.csv')
qa['fraud_count'] = 1
qa  = qa.drop(['missing_escalation','payment_method'],axis=1)

min_year = qa['canceled_year'].min()
min_month = qa['canceled_month'].min()

max_year = qa['canceled_year'].max()
max_month = qa['canceled_month'].max()

distinct_hold_reason = list(qa['hold_reason'].unique())
distinct_confirmed_fraud = list(qa['confirmed_fraud'].unique())

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

for hold_reason in distinct_hold_reason:
    for confirmed_fraud in distinct_confirmed_fraud :
            for year_month in date_list:
                temp_df = pd.DataFrame()
                temp_df['canceled_year'] = [int(str(year_month)[0:4])]
                temp_df['canceled_month'] = [int(str(year_month)[-2:])]
                temp_df['hold_reason'] = [hold_reason]
                temp_df['confirmed_fraud'] = [confirmed_fraud]
                # temp_df['missing_escalation'] = [None]
                # temp_df['payment_method'] = [None]
                temp_df['latest_buyer_price'] = [None]
                temp_df['fraud_count'] = [0]
                main_df = pd.concat([main_df, temp_df], axis=0)

qa = pd.concat([qa, main_df], axis=0)

# Create a pivot table
qa_pivoted = qa.pivot_table(
    index=['hold_reason', 'confirmed_fraud'],  # Index columns
    columns=['canceled_year', 'canceled_month'],  # Columns
    aggfunc={'fraud_count': 'sum', 'latest_buyer_price': 'sum'},  # Aggregation functions
    # margins = True,
    # margins_name = 'Total'
    
)


# Display the pivot table
qa_pivoted.to_excel('qa_report.xlsx')
print(qa_pivoted)


quit()

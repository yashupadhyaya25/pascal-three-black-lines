from pandas import pivot_table, concat
# from lib.lib_qa import *
import pandas as pd

# qa = load_qa_csv()
# qa = qa[['canceled_year', 'canceled_month', 'hold_reason', 'confirmed_fraud', 'missing_escalation',
#          'payment_method', 'latest_buyer_price']]

# qa.to_csv('qa.csv', index=False)

# print(qa)
df = pd.read_csv('qa.csv')

# Create a pivot table
df_pivoted = df.pivot_table(
    index=['hold_reason', 'confirmed_fraud'],  # Index columns
    columns=['canceled_year', 'canceled_month'],  # Columns
    aggfunc={'hold_reason': 'count', 'latest_buyer_price': 'sum'}  # Aggregation functions
)

# Display the pivot table
print(df_pivoted)


quit()

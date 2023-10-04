
'''
=================================================
Name: Edy Setiawan

This program is designed for Great Expectations
=================================================
'''

# BUILD CONTEXT

context = gx.get_context()

# BUILD DATA SOURCE

df = pd.read_csv(r'C:\Users\Edy Setiawan\Desktop\hacktiv8\Milestone\Milestone 3\P2M3_Edy_Setiawan_data_clean.csv')

datasource = context.sources.add_pandas(name="pandas_datasource")

name = "bankruptcy"
data_asset = datasource.add_dataframe_asset(name=name)

my_batch_request = data_asset.build_batch_request(dataframe=df)

######################################

# BUILD EXPECTATION SUITE AND VALIDATOR


# created an Expectation Suite
context.add_or_update_expectation_suite("my_expectation_suite")

# create a Validator using above Expectation Suite
validator = context.get_validator(
    batch_request=my_batch_request,
    expectation_suite_name="my_expectation_suite",
)

######################################

# Expectation 1: Dataset Should Not Be Empty
validator.expect_table_row_count_to_be_between(min_value=1, max_value=None)

# Expectation 2: 'bankrupt' column only contains the values 1 and 0
validator.expect_column_values_to_be_in_set(column='bankrupt', value_set=[0, 1])

# Expectation 3: 'current_liability_to_current_assets' Between 0 and 1
validator.expect_column_values_to_be_between(column='current_liability_to_current_assets', min_value=0, max_value=1)

# Expectation 4: 'liability-assets_flag' Should Contain Values from a Set

validator.expect_column_values_to_be_in_set(column='liability-assets_flag', value_set=[0, 1])

# Expectation 5: 'total_expense_or_assets' Should Not Contain NULL Values

validator.expect_column_values_to_not_be_null(column='total_expense_or_assets')

# Expectation 6: 'cash_or_current_liability' Should Have a Maximum Value Below a Certain Threshold

validator.expect_column_max_to_be_between(column='cash_or_current_liability', min_value=None, max_value=9650000000)


# Expectation 7: 'tax_rate_(a)' Should Have a Minimum Value Above a Certain Threshold

validator.expect_column_min_to_be_between(column='tax_rate_(a)', min_value=0, max_value=None)


# Save validator into expectation suite
validator.save_expectation_suite(discard_failed_expectations=False)

######################################

# BUILD CHECKPOINT

checkpoint = context.add_or_update_checkpoint(
    name="my_checkpoint",
    validations=[
        {
            "batch_request": my_batch_request,
            "expectation_suite_name": "my_expectation_suite",
        },
    ],
)

checkpoint_result = checkpoint.run()

context.build_data_docs()

# context.open_data_docs()
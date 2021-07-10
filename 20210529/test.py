import akshare as ak
import pandas as pd
#
bond_convert_jsl_df = ak.bond_cov_jsl()
print(bond_convert_jsl_df)
# bond_convert_jsl_df.to_csv('test1.csv')

# print(type(bond_convert_jsl_df))
#
# # print(bond_convert_jsl_df.columns)
#
# col_name = ['bond_id', 'bond_nm', 'price', 'full_price', 'premium_rt', 'rating_cd', 'issuer_rating_cd']
#
# # for col in bond_convert_jsl_df.columns:
# #     series = bond_convert_jsl_df[col]
# #     print(series[1])
# #     # break
#
# for col in col_name:
#     series = bond_convert_jsl_df[col]
#     print(series[1])
#     # break

# 主要股东

test=ak.stock_main_stock_holder(stock='600004')
print(test)
print(test['平均持股数'][5])
print(test['平均持股数'][15])

# test.to_csv('test.csv')





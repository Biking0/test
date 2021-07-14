import akshare as ak
stock_circulate_stock_holder_df = ak.stock_circulate_stock_holder(stock="600000")
# print(stock_circulate_stock_holder_df)
print(stock_circulate_stock_holder_df[0:3])
# print(stock_circulate_stock_holder_df[0:22]['占流通股比例(%)'])

new_sum=0
old_sum=0
for i in stock_circulate_stock_holder_df[0:10]['占流通股比例(%)']:
    # print(i)
    new_sum=new_sum+round(float(i),2)

for j in stock_circulate_stock_holder_df[10:20]['占流通股比例(%)']:
    # print(j)
    old_sum=old_sum+round(float(j),2)

# print('11',new_sum)

print(old_sum,new_sum,new_sum-old_sum)
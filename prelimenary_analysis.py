
from pyspark.sql.functions import sum, min, max, count, col, desc, to_date, explode, sequence, last, when, lit\
from pyspark.sql import Window

import pandas as pd
import matplotlib.pylab as plt
import seaborn as sns

sns.set(rc=\{'figure.figsize':(16,8)\})

nft_contract_address = spark3.contract(address='0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d')
ethereum_nft_nft_trades = spark.table("ethereum_nft.nft_trades")
ethereum_nft_nft_trades.columns

# comment\

date_sequence = spark.sql("SELECT sequence(to_date('2018-01-01'), to_date(now()), interval 1 day) as date")\
.withColumn("date", explode(col("date")))

date_sequence = date_sequence.select(to_date("date").alias("reference_date"))

# comment\

# floor price over time\

contract_eth_nft_trades = ethereum_nft_nft_trades.filter(col("nft_contract_address") == '0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d')\
.select("usd_amount", "currency_amount", "block_time")

contract_eth_nft_trades = contract_eth_nft_trades.select((to_date("block_time")).alias("date"), "usd_amount", "currency_amount")

contract_eth_nft_trades = contract_eth_nft_trades.groupBy("date")\
.agg(min("usd_amount").alias("floor_usd"), min("currency_amount").alias("floor_eth"))\
.sort("date", ascending=False).select("date", "floor_usd")

def replace(column, value):
    return when(column != value, column).otherwise(lit(None))

contract_eth_nft_trades = date_sequence\
.join(contract_eth_nft_trades, date_sequence.reference_date == contract_eth_nft_trades.date, "left")\

contract_eth_nft_trades = contract_eth_nft_trades.withColumn("floor_usd", replace(col("floor_usd"), 0.0))
contract_eth_nft_trades = contract_eth_nft_trades.withColumn("floor_usd", replace(col("floor_usd"), 0.0))

contract_eth_nft_trades_df = contract_eth_nft_trades.toPandas()

sns.lineplot(x = 'date'
           , y = 'floor_usd'
           , data=contract_eth_nft_trades_df)

#comment\

nft_holders = nft_contract_address.get_event_by_name("Transfer")\
    .select("event_parameter.inputs.*", "dt", "block_timestamp")\
    .select("to").distinct().collect()

def get_contracts(contract_spark_row_list):
# Given spark row list, return list of contract str
    contract_list = []
    for contract in contract_spark_row_list:
        contract_list.append(contract.asDict()['to'])
    return contract_list

nft_holders_list = get_contracts(nft_holders)

nft_buyer_nft_projects = ethereum_nft_nft_trades.filter(ethereum_nft_nft_trades.buyer.isin(nft_holders_list))

nft_buyer_nft_projects\
    .groupBy("nft_project_name")\
    .count()\
    .sort(desc("count")).limit(10)

#comment\

df_count_cost = nft_buyer_nft_projects.select("buyer"
                              ,"nft_contract_address"
                              ,"nft_project_name"
                              ,"usd_amount")\
    .groupBy("nft_project_name")\
    .agg(sum("usd_amount"), count("*"))

df_count_cost = df_count_cost.toDF("nft_project_name","sum_usd","count_total")

df_count_cost = df_count_cost.toPandas()
df_count_cost = df_count_cost[df_count_cost["nft_project_name"].notnull()]
df_count_cost = df_count_cost.sort_values(by='sum_usd', ascending=False).head(100)

df_count_cost.reset_index(inplace=True)

def get_scatterplot(df, exclude_top_number=0):
    
    exclude_projects = list(df['nft_project_name'].head(exclude_top_number))
    
    mask = df["nft_project_name"].isin(exclude_projects)
    
    updated_df = df[~mask]
    updated_df.reset_index(inplace=True)
    updated_df = updated_df[['nft_project_name','sum_usd','count_total']]

    #Create figure
    plt.figure(figsize = (8,8))

    # Create scatterplot
    ax = sns.scatterplot(data=updated_df,\
                         x="count_total",\
                         y="sum_usd",\
                         size="sum_usd",\
                         legend=False,\
                         sizes=(20, 2000))

    # For each point, we add a text inside the bubble\
    for line in range(0, updated_df.shape[0]):
        ax.text(updated_df["count_total"][line],\
             updated_df["sum_usd"][line],\
             updated_df["nft_project_name"][line],\
             horizontalalignment='center',\
             size='medium',\
             color='black',\
             weight='semibold')
        
    plt.show()\

get_scatterplot(df_count_cost)

#comment\

get_scatterplot(df_count_cost, exclude_top_number=15)

#comment\

top_five_nft_usd = list(df_count_cost.head(5)["nft_project_name"])

# boxplot\

df_box_data = nft_buyer_nft_projects.filter(nft_buyer_nft_projects.nft_project_name.isin(top_five_nft_usd))\
.toPandas()

import seaborn as sns
sns.set_theme(style="whitegrid")
ax = sns.boxplot(x=df_box_data["nft_project_name"], y=df_box_data["usd_amount"])}

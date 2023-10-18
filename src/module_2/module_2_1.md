```python
import numpy as np
import pandas as pd
import os


folder_path = "../../data" #Folder in main directory

# # Crea el directorio local si no existe
# if not os.path.exists(data_folder):
#     os.makedirs(data_folder)
```


```python
# Clean Users dataframe
# Importing dataframe from "dirty" folder
df = pd.read_parquet(f"{folder_path}/users.parquet")

# Drop information about users with no adults
drop_indexes = df[
    ((df["count_adults"] == 0) & (df["count_people"] > 0))
    | ((df["count_adults"] == 0) & (df["count_pets"] > 0))
].index

df_clean = df.copy()
df_clean.drop(index=drop_indexes, inplace=True)

df_clean["missing_values"] = np.where((df_clean["count_adults"].isna() | 
                                      df_clean["count_children"].isna() |
                                      df_clean["count_babies"].isna() |
                                      df_clean["count_pets"].isna()),
                                      1, 0)

missing_columns = [
    "count_adults",
    "count_children",
    "count_babies",
    "count_pets",
]

for column in missing_columns:
    df_clean[column].fillna(df_clean[column].mean(), inplace=True)

df_clean.to_parquet(f"{folder_path}/users_clean.parquet")

```

#### Dejo comentado lo que hice mal: Sustituir missing values con random values siguiendo la distribucion de cada variable 


```python


# 95% of rows with missing info about people
# Comparing df.isna() with df.notna(), similar user_segment and user_nuts1 distribution
# Fill every missing value following column distribution

# missing_columns = [
#     "count_adults",
#     "count_children",
#     "count_babies",
#     "count_pets",
#     "user_nuts1",
# ]

# for column in missing_columns:
#     missing_values = df_clean[column].isna()
#     s = df_clean[column].value_counts(normalize=True)
#     np.random.seed(99)
#     df_clean.loc[missing_values, column] = np.random.choice(
#         s.index, size=missing_values.sum(), p=s.values
#     )

# # Recalculating count_people column where np.nan
# missing_values = df_clean["count_people"].isna()
# df_clean.loc[missing_values, "count_people"] = (
#     df_clean.loc[missing_values, "count_adults"]
#     + df_clean.loc[missing_values, "count_children"]
#     + df_clean.loc[missing_values, "count_babies"]
# )

# # Exporting dataframe to clean folder
# df_clean.to_parquet(f"data-modified/users.parquet")
```


```python
# Clean Inventory dataframe
# Importing dataframe 
df = pd.read_parquet(f"{folder_path}/inventory.parquet")

#Adding np.nan to products with price equal to zero
df["price"].mask(df["price"] == 0, np.nan, inplace=True)
df["compare_at_price"].mask(df["compare_at_price"] == 0, np.nan, inplace=True)
df.to_parquet(f"{folder_path}/inventory_clean.parquet")

```

#### Añadiendo informacion de usuarios a cada pedido


```python
# Merging orders dataframe

df_orders = pd.read_parquet(f"{folder_path}/orders_clean.parquet")
df_users = pd.read_parquet(f"{folder_path}/users.parquet")
df_inventory = pd.read_parquet(f"{folder_path}/inventory_clean.parquet")

df_final = df_orders.copy()


# Left join to complete users information (count adults, count children, etc.)
df_final = pd.merge(df_final, df_users, how="left", on=["user_id"])


# Explode list of items whitin orders
df_explode = df_final.explode("ordered_items")
df_explode.rename(columns={"ordered_items": "variant_id"}, inplace=True)

# Merging with inventory dataframe
df_explode = pd.merge(df_explode, df_inventory, how="left", on=["variant_id"])
df_explode.dropna(inplace=True)

# Grouping information added
total_price = df_explode.groupby("id")["price"].agg("sum")
max_price_article = df_explode.groupby("id")["price"].agg("max")
total_compare_at_price = df_explode.groupby("id")["compare_at_price"].agg("sum")
vendors = df_explode.groupby("id")["vendor"].agg(list)
product_types = df_explode.groupby("id")["product_type"].agg(list)
tags = df_explode.groupby("id")["tags"].agg(list)

# Adding grouped information added to final dataframe
df_final = pd.merge(df_final, total_price, how="left", on=["id"])
df_final = pd.merge(df_final, total_compare_at_price, how="left", on=["id"])
df_final = pd.merge(df_final, max_price_article, how="left", on=["id"])

df_final = pd.merge(df_final, vendors, how="left", on=["id"])
df_final = pd.merge(df_final, product_types, how="left", on=["id"])
df_final = pd.merge(df_final, tags, how="left", on=["id"])


df_final.rename(columns={"price_x": "mean_price", "price_y": "max_article_price"}, inplace=True)

# If prices are zero, add np.nan with mask
df_final["mean_price"].mask(df_final["mean_price"] == 0, np.nan, inplace=True)
df_final["compare_at_price"].mask(
    df_final["compare_at_price"] == 0, np.nan, inplace=True
)

# Export to clean folder
df_final.to_parquet(f"{folder_path}/orders_modified.parquet")

```


```python
#Analyzing orders
df_orders = pd.read_parquet(f"{folder_path}/orders_modified.parquet")

monthly_sales = df_orders.groupby([df_orders.order_date.dt.month, df_orders.order_date.dt.year])[["mean_price", "count_people"]].agg(["sum", "mean"])
monthly_sales
#Starts selling in April 2020
#Growth during frist year
#Sales remained constant during March 2021 and September 2021
#Fast growth from September 2021 (x2 in October 2021)
#Best sales month: January 2022
#More people buying. Means relatively constants
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead tr th {
        text-align: left;
    }

    .dataframe thead tr:last-of-type th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr>
      <th></th>
      <th></th>
      <th colspan="2" halign="left">mean_price</th>
      <th colspan="2" halign="left">count_people</th>
    </tr>
    <tr>
      <th></th>
      <th></th>
      <th>sum</th>
      <th>mean</th>
      <th>sum</th>
      <th>mean</th>
    </tr>
    <tr>
      <th>order_date</th>
      <th>order_date</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="2" valign="top">1</th>
      <th>2021</th>
      <td>902.32</td>
      <td>60.154667</td>
      <td>53.0</td>
      <td>3.533333</td>
    </tr>
    <tr>
      <th>2022</th>
      <td>2732.88</td>
      <td>62.110909</td>
      <td>133.0</td>
      <td>3.022727</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">2</th>
      <th>2021</th>
      <td>1620.70</td>
      <td>73.668182</td>
      <td>76.0</td>
      <td>3.454545</td>
    </tr>
    <tr>
      <th>2022</th>
      <td>3145.90</td>
      <td>62.918000</td>
      <td>219.0</td>
      <td>3.650000</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">3</th>
      <th>2021</th>
      <td>1149.48</td>
      <td>71.842500</td>
      <td>52.0</td>
      <td>3.250000</td>
    </tr>
    <tr>
      <th>2022</th>
      <td>653.81</td>
      <td>54.484167</td>
      <td>39.0</td>
      <td>3.000000</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">4</th>
      <th>2020</th>
      <td>0.00</td>
      <td>NaN</td>
      <td>12.0</td>
      <td>4.000000</td>
    </tr>
    <tr>
      <th>2021</th>
      <td>2774.48</td>
      <td>56.622041</td>
      <td>141.0</td>
      <td>2.877551</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">5</th>
      <th>2020</th>
      <td>494.83</td>
      <td>26.043684</td>
      <td>60.0</td>
      <td>2.857143</td>
    </tr>
    <tr>
      <th>2021</th>
      <td>2481.10</td>
      <td>65.292105</td>
      <td>114.0</td>
      <td>3.000000</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">6</th>
      <th>2020</th>
      <td>1344.79</td>
      <td>35.389211</td>
      <td>114.0</td>
      <td>2.923077</td>
    </tr>
    <tr>
      <th>2021</th>
      <td>3019.78</td>
      <td>64.250638</td>
      <td>143.0</td>
      <td>3.042553</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">7</th>
      <th>2020</th>
      <td>2061.62</td>
      <td>38.898491</td>
      <td>176.0</td>
      <td>3.320755</td>
    </tr>
    <tr>
      <th>2021</th>
      <td>1269.30</td>
      <td>60.442857</td>
      <td>62.0</td>
      <td>2.952381</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">8</th>
      <th>2020</th>
      <td>2337.13</td>
      <td>46.742600</td>
      <td>148.0</td>
      <td>2.792453</td>
    </tr>
    <tr>
      <th>2021</th>
      <td>1896.96</td>
      <td>63.232000</td>
      <td>108.0</td>
      <td>3.600000</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">9</th>
      <th>2020</th>
      <td>1510.36</td>
      <td>47.198750</td>
      <td>118.0</td>
      <td>3.687500</td>
    </tr>
    <tr>
      <th>2021</th>
      <td>2278.97</td>
      <td>67.028529</td>
      <td>99.0</td>
      <td>2.911765</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">10</th>
      <th>2020</th>
      <td>902.91</td>
      <td>56.431875</td>
      <td>49.0</td>
      <td>3.062500</td>
    </tr>
    <tr>
      <th>2021</th>
      <td>2514.26</td>
      <td>69.840556</td>
      <td>98.0</td>
      <td>2.722222</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">11</th>
      <th>2020</th>
      <td>1422.66</td>
      <td>54.717692</td>
      <td>80.0</td>
      <td>3.076923</td>
    </tr>
    <tr>
      <th>2021</th>
      <td>2265.18</td>
      <td>58.081538</td>
      <td>97.0</td>
      <td>2.487179</td>
    </tr>
    <tr>
      <th rowspan="2" valign="top">12</th>
      <th>2020</th>
      <td>1388.21</td>
      <td>77.122778</td>
      <td>68.0</td>
      <td>3.777778</td>
    </tr>
    <tr>
      <th>2021</th>
      <td>2219.67</td>
      <td>61.657500</td>
      <td>98.0</td>
      <td>2.722222</td>
    </tr>
  </tbody>
</table>
</div>



import pandas as pd
import awswrangler as wr

header = ['age', 'height']
values = [[18,192.0]]


df = pd.DataFrame(values, columns = header)
wr.s3.to_csv(df, "s3://s-o-s3-bucket-prefix-20250520143017315000000001/project_test_with_wrangler.csv")


df_read = wr.s3.read_csv("s3://s-o-s3-bucket-prefix-20250520143017315000000001/project_test_with_wrangler.csv")
df_read_dropped = df_read.drop(['Unnamed: 0', 'age'], axis=1)
print(df_read_dropped)
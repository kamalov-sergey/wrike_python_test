import pandas as pd

df = pd.read_csv('//Users//sberbank//PycharmProjects//pandas_tst//sources//tst.csv')
print(df)
df1 = df.groupby('C1')['C1'].count()

print(df1)
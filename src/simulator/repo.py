import pandas as pd
pd.options.mode.chained_assignment = None
from matplotlib import pyplot as plt



def multiple_by_thousand(df, *column_names):
    for column_name in column_names:
        column = df[column_name]
        values = len(column.values)
        print(f'fixing column {column_name}')
        for i in range(values):
            column.iloc[i] = 1000 * column.iloc[i]
    print("done")

filename = '/home/eng/Hazelcast/simulator-tng/storage/runs/insert/1M/map_tiered/03-08-2022_09-04-10/A1_dstat.csv'
df = pd.read_csv(filename, skiprows=5)

#for col in df.columns:
#    print(col)

multiple_by_thousand(df, 'used', 'free', 'buf', 'cach')

print(df.to_string())

plt.figure()

plt.rcParams["figure.figsize"] = [10, 5]
plt.plot(df['epoch'], df['used'])
plt.plot(df['epoch'], df['free'])
plt.plot(df['epoch'], df['buf'])
plt.plot(df['epoch'], df['cach'])
plt.show()

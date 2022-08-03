import pandas as pd
pd.options.mode.chained_assignment = None
from matplotlib import pyplot as plt
import tempfile


def multiple_by_thousand(df, *column_names):
    for column_name in column_names:
        column = df[column_name]
        values = len(column.values)
        for i in range(values):
            column.iloc[i] = 1000 * column.iloc[i]
    print("done")

filename = '/home/eng/Hazelcast/simulator-tng/storage/runs/insert/10M/map_tiered/03-08-2022_10-52-46/A1_dstat.csv'
df = pd.read_csv(filename, skiprows=5)

for col in df.columns:
    print(col)

multiple_by_thousand(df, 'used', 'free', 'buf', 'cach')

print(df.to_string())

dirpath = tempfile.mkdtemp()
print(f"directory {dirpath}")
for c in range(2, len(df.columns)):
    column_name = df.columns[c]
    plt.figure()
    plt.rcParams["figure.figsize"] = [10, 5]

    df['epoch'] = pd.to_datetime(df['epoch'], unit='s')

    plt.plot(df['epoch'], df[column_name])
    filename = column_name.replace("/","_")

    plt.savefig(f'{dirpath}/{filename}.png')
    plt.close()


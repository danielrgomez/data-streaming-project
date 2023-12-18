import numpy as np
from numpy import add
import pandas as pd

#Reads the CSV into a pandas dataframe
df = pd.read_csv("data.csv",encoding ='unicode_escape')
#print(df)

# Splitlines splits the jston into multiple rows
df['json'] = df.to_json(orient='records', lines=True).splitlines()
#print(df['json'])

dfjson = df['json']
print(dfjson)

#Saves the output as a txt file. Contains json output.
np.savetxt(r'./output.txt', dfjson.values, fmt='%s')


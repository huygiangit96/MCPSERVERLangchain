import pandas as pd

file = "File.xlsm"

sheet_name = pd.ExcelFile(file).sheet_names
dict_result = dict(enumerate(sheet_name))
print(dict_result)
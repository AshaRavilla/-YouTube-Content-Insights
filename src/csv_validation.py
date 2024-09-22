import pandas as pd


import chardet

'''

def detect_encoding(file_path):
    with open(file_path, 'rb') as file:
        raw_data = file.read()
        result = chardet.detect(raw_data)
        encoding = result['encoding']
        confidence = result['confidence']
        print(f"Detected encoding: {encoding} (Confidence: {confidence})")
        return encoding

# Replace with your file path
file_path = 'CAvideos_kaggle.csv'
encoding = detect_encoding(file_path)
'''
'''
def detect_encoding(file_path):
    with open(file_path, 'rb') as file:
        result = chardet.detect(file.read())
        print(f"Detected encoding: {result['encoding']}")

print("Encoding of the csv files from API")
detect_encoding('CAvideos.csv')

print("-------------------------------------------------------------------------------------------")

print("Encoding of the csv files from API")
detect_encoding('CAvideos_kaggle.csv')

'''

df = pd.read_csv('CAvideos.csv')
print(len(df.columns))
print(df.columns)
print(df.info())
print("-------------------------------------------------------------------------------------------")

df_kaggle =pd.read_csv('CAvideos_kaggle.csv')
print(len(df_kaggle.columns))
print(df_kaggle.columns)
print(df_kaggle.info()) 
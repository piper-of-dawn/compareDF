import polars as pl
def convert_metric_prefix(string: str) -> int:
    try:
        return int(string)
    except:
        metric_prefixes = {"K": 1000, "M": 1000000, "B": 1000000000}
        number = float(string[:-1])
        metric = string[-1]
        if metric in metric_prefixes:
            return int(number * metric_prefixes[metric])
        else:
            return int(string)


import re

def extract_numbers(text):
    # regular expression to match one or more digits
    pattern = r"\d+\.?\d*"
    matches = re.findall(pattern, text)
    try:
        if len(matches) == 1:
            return float(matches[0])
        else:
            num_range = [float(match) for match in matches]
            return (num_range[1] + num_range[0])/2
    except:
        return 0


df = pl.read_csv('glassdoor.csv', try_parse_dates=True)

df = df.with_columns([
  pl.col("Company reviews").apply(lambda x: convert_metric_prefix(x)).alias('Jobs'),
  pl.col("Company salaries").apply(lambda x: convert_metric_prefix(x)).alias('Salaries'),
  pl.col("Company Jobs").apply(lambda x: convert_metric_prefix(x)).alias('Reviews'),
  pl.col('Company rating').alias('Rating'),
  pl.col('Company Name').alias('Company'),
  pl.col('Number of Employees').apply(lambda x: extract_numbers(x)).alias('Employees'),
  pl.col('Industry Type').alias('Industry')
  ])

df = df.drop(['Company reviews', 'Company salaries', 'Company Jobs', 'Company rating', 'Company Name', 'Number of Employees', 'Industry Type'])

df.select(pl.col('Company'), pl.col('Industry'), pl.col('Jobs'), pl.col('Salaries'), pl.col('Reviews'), pl.col('Rating'), pl.col('Employees'), pl.col('Location'), pl.col('Company Description')).write_parquet('glassdoor.parquet')
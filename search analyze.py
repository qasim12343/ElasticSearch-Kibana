from elasticsearch.helpers import bulk
from elasticsearch import Elasticsearch
import pandas as pd

# 1
df = pd.read_csv('organizations-10000.csv')


nuc = df['Country'].nunique()
print(nuc)

# 2
nucom = df['Organization Id'].nunique()
print(nucom)

# 3

num_companies_in_country = df[df['Country'] ==
                              'Country Name']['Organization Id'].nunique()
print(num_companies_in_country)

# 4
num_companies_over_100_employees = df[df['Number of employees']
                                      > 100]['Organization Id'].nunique()
print(num_companies_over_100_employees)

# 5
company_most_employees = df.loc[df['Number of employees'].idxmax()]
print(company_most_employees["Name"])

# 6
company_fewest_employees = df.loc[df['Number of employees'].idxmin()]
print(company_fewest_employees["Name"])


# 7
df = pd.read_csv('organizations-10000.csv')

es = Elasticsearch("http://localhost:9200")

e = es.info().body
print(e)


mappings = {
    "properties": {
        "Organization Id": {"type": "keyword"},
        "Name": {"type": "text"},
        "Website": {"type": "keyword"},
        "Country": {"type": "keyword"},
        "Description": {"type": "text"},
        "Founded": {"type": "integer"},
        "Industry": {"type": "keyword"},
        "Number of employees": {"type": "integer"}
    }
}


index_name = "organization"
es.indices.create(index=index_name, mappings=mappings)


def generate_actions(df, index_name):
    for i, row in df.iterrows():
        yield {
            "_index": index_name,
            "_id": i,
            "_source": {
                "Organization Id": row["Organization Id"],
                "Name": row["Name"],
                "Website": row["Website"],
                "Country": row["Country"],
                "Description": row["Description"],
                "Founded": row["Founded"],
                "Industry": row["Industry"],
                "Number of employees": row["Number of employees"]
            }
        }


bulk(es, generate_actions(df, index_name))

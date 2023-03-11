# sync-2lux

This is a prototype pipeline to ingest data from a csv file dataset and publish it in a MongoDB database

This aoo:

- Pulls raw data from Flipkart Products sample dataset (data/flipkart_com-ecommerce_sample.csv)
- Process the data to validate and normalize it
- Outputs the result to the database

## setup

1. Install the app `pip install -e .`

2. Verify and modify (if needed) the default config file (config.json)

```
{
  "connection_string": "mongodb://localhost:27017/",
  "database_name": "2lux"
}
```

3. Products specifics definitions are in a separate file: config_products.json. (there is no need to modify)

```
{
  "source_file": "data/flipkart_com-ecommerce_sample.csv",
  "headers": [
    "uniq_id",
    "crawl_timestamp",
    "product_url",
    "product_name",
    "product_category_tree",
    "pid",
    "retail_price",
    "discounted_price",
    "image",
    "is_fk_advantage_product",
    "description",
    "product_rating",
    "overall_rating",
    "brand",
    "product_specifications"
  ],
  "dtypes": {
    "uniq_id": "str",
    "crawl_timestamp": "str",
    "product_url": "str",
    "product_name": "str",
    "product_category_tree": "str",
    "pid": "str",
    "retail_price": "float64",
    "discounted_price": "float64",
    "image": "str",
    "is_FK_Advantage_product": "bool",
    "description": "str",
    "product_rating": "str",
    "overall_rating": "str",
    "brand": "str",
    "product_specifications": "str"
  },
  "parse_dates": ["crawl_timestamp"]
}
```

## Run

`tap-exactsales --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json >> state.json`

---

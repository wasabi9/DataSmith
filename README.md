# DataSmith

Here are the steps involved in Data Synthesis:

1. Domain - Specification of the domain in which data generation is to be done.
2. Getting Domain Knowledge - Either provided through documents or gathered by serving the web
3. Task Specification + Example Data Points - Specify Task Type, Description and Provide Data Examples
4. Confounding Variables and Diversity Dimensions - Either manual or to be determined automatically
5. Data Generation
6. Checks for Data Quality - Key Objective, Factual Correctness, Bias, Diversity

**Setup**

Python package requirements placed in requirements.txt

```
pip install -r requirements.txt
```

Add your OpenAI Key as environment variable - either in .zsh or .env file

**Entry Point**

main_sql.py

```
python main_sql.py generate_data --databases 1 --sql_folder sql/creates_test --metadata_folder sql/metadata_test
```

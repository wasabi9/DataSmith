import random
import os
import time
from typing import List
from tqdm import tqdm
import openai
import re
import ast
from retry import retry
from joblib import Parallel, delayed
from dotenv import load_dotenv

FOLDER = "datasets/created_v2/"
# FOLDER = "datasets/created_v3/"

class Prompts:
    def system_db_prompt(self):
        return "You are a database expert. Return the answer only in json"
    
    def user_db_prompt(self, domain, table: int=10):
        return """For the given domain {0} generate a name to the database in this domain and then give a list of {1} tables in that database. 
        e.g {{database: "sample_database", tables : [table_a, table_b, table_c]}}""".format(domain, table)
    
    def system_ddl_prompt(self):
        return "You are a database expert. Return the answer only in DDL code blocks"
    
    def user_ddl_prompt(self, database: str, table: str, columns: int=10):
        return """You are given a table name: {0}, the number of columns: {1} and the database name: {2}.
        Based on this information, create actual column names related to the domain and the table. 
        Output the result in DDL code block for Creating the table along with column name.
        Only provide the Create code block for the table. 
        Give the code block enclosed in ```""".format(table, columns, database)
    
@retry(tries=10, delay=4, max_delay=50, backoff=3, logger=True)
def call_openai(system_prompt: str, user_prompt: str):
    messages = []
    messages.append({
        "role": "system",
        "content": system_prompt,
    })

    messages.append({
        "role": "user",
        "content": user_prompt
    })

    result = openai.ChatCompletion.create(
        model="gpt-4",
        temperature=0.8,
        messages=messages
    )
    
    output = result['choices'][0]['message']['content']

    return output


class Dataset:
    def __init__(self) -> None:
        openai.api_key = os.environ["OPENAI_API_KEY"]
        self.prompts = Prompts()

    def extract_ddl(self, output: str):
        indices = [m.start() for m in re.finditer('```', output)]
        if len(indices) != 2:
            print("extractio failure")
        return output[indices[0]+3: indices[1]]

    def create(self, databases: int = 1) -> List[str]:
        # import ipdb; ipdb.set_trace()
        ddl_list = []
        print("Creating {0} databases".format(databases))

        domains = call_openai(
            "You need to generate a list of business database domains. Always return lists.",
            "Generate a list of {0} business domains that store data in databases. e.g ['healthcare_management', 'supply_chain', 'medicine', 'food processing']. Only return a list of domains".format(databases)
        )

        domains = domains[domains.find('['):domains.find(']')+1]
        domains = ast.literal_eval(domains)

        for i, domain in tqdm(enumerate(domains)):
            output = call_openai(
                self.prompts.system_db_prompt(),
                self.prompts.user_db_prompt(domain=domain, table=random.randint(5,15))
            )
            
            try:
                result_dict = ast.literal_eval(output)
                if isinstance(result_dict, dict):
                    print("Successfully converted the string to a dictionary:")
                    print(result_dict)
                else:
                    print("The provided string does not represent a valid dictionary.")
                    continue
            except (SyntaxError, ValueError) as e:
                print("Error:", e)
                continue
            
            def get_tables(table):
                # for table in tqdm(result_dict["tables"]):
                output = call_openai(
                    self.prompts.system_ddl_prompt(),
                    self.prompts.user_ddl_prompt(database=result_dict["database"], table=table, columns=random.randint(10,25))
                )
                try:
                    start_index = output.find("```", 0)
                    end_index = output.find("```", start_index+3)
                    ddl_block = output[start_index+3:end_index].strip()
                    if ddl_block[-1] != ";":
                        ddl_block+=";"
                    ddl_block = ddl_block[ddl_block.lower().find("create"):]
                except:
                    return ""

                ddl_block = ddl_block.replace(result_dict["database"]+".", "")

                return ddl_block
                # time.sleep(random.randint(5, 10))

            results = Parallel(n_jobs=-1)(delayed(get_tables)(table) for table in tqdm(result_dict["tables"]))

            ddl = ""
            for result in results:
                if result != "":
                    ddl += result + "\n\n"

            with open(FOLDER+result_dict["database"]+"_"+str(i)+".sql", "w") as file:
                file.write(ddl)
            ddl_list.append(ddl)
            time.sleep(random.randint(5, 10))

        return ddl_list
    

if __name__ == "__main__":
    load_dotenv()    
    dataset = Dataset()
    print(dataset.create(100))

            



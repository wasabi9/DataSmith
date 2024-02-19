import ast
import json
import os
import random
import re
from typing import List
from sql.generate_metadata import LLM
import openai
from joblib import Parallel, delayed
from tqdm import tqdm
from dotenv import load_dotenv

# def get_data_schema()

def get_train_prompt(query):
    messages = []
    messages.append(
        {
            "role": "system",
            "content": "You are a sql chatbot. Give the tables and column used in the given query"
        }
    )
    messages.append(
        {
            "role": "user",
            "content": """You are given an SQL query. Give the tables and columns used in the query.\n
            The input sql query: {0}\n
            Give the output in json format. The result should be enclosed in ```.\n
            Only output the output json with table and column names used in the query.\n
            Sample output: ```{{\"table_1\":[\"column_1\", \"column_2\"], \"table_2\":[\"column_3\", \"column_4\"]}}```\n
            Only give the tables and columns present in the SQL query.""".format(query)
        }
    )
    return messages

class PrepareData():
    def __init__(self, files: List[str], dialect: str="MySQL", master_folder: str="sql/metadata"):
        self.queries = {}
        self.master_folder = master_folder
        if dialect.lower() == "mysql":
            folder = os.path.join(self.master_folder, "question_mysql")
        elif dialect.lower() == "postgres":
            # folder = "datasets/metadata/questions_postgres/"
            folder = os.path.join(self.master_folder, "questions")
        else:
            raise Exception("Unsupported sql dialect type")
        self.dialect = dialect

        for file in files:
            self.queries[file.split('.')[0]] = json.load(open(os.path.join(folder, file), "r"))
        
        self.used_tables_columns = {}

        self._post_init()

        # import ipdb; ipdb.set_trace()
        # print(self.used_tables_columns)

    def _post_init(self):
        # import ipdb; ipdb.set_trace()
        llm = LLM(model="gpt-3.5-turbo-16k")
        self.used_tables_columns = {}

        if os.path.isfile(self.master_folder+"/train/"+"used_tables_columns.json"):
            with open(self.master_folder+"/train/"+"used_tables_columns.json", "r") as f:
                self.used_tables_columns = json.load(f)

        def get_query_tables_columns(file, question):
            query = self.queries[file][question]
            prompt = get_train_prompt(query)
            result = llm.create(prompt)
            result_ = re.findall(r"```([\s\S]*?)```", result.choices[0].message.content)
            description = lambda x: x[0] if len(x)>0 else result.choices[0].message.content

            table_column_used = result.choices[0].message.content
            try:
                table_column_used = ast.literal_eval(description(result_).strip('\n; '))
                
            except:
                try:
                    temp_ = description(result_).strip('\n; ')
                    if temp_[0] != '{':
                        temp_ = temp_[temp_.find('{'):]
                        table_column_used = json.loads(temp_)
                except Exception:
                    print("         Error in dict conversion for {0}".format(question))
            return (question, table_column_used)

        def get_tables_columns(file):
            print(" Getting tables and columns for {0}".format(file))
            if file not in self.used_tables_columns:
                question_dict = {}
                # for question in self.queries[file]:
                    # query = self.queries[file][question]
                    # prompt = get_train_prompt(query)
                    # result = llm.create(prompt)
                    # result_ = re.findall(r"```([\s\S]*?)```", result.choices[0].message.content)
                    # description = lambda x: x[0] if len(x)>0 else result.choices[0].message.content

                    # table_column_used = result.choices[0].message.content
                    # try:
                    #     table_column_used = ast.literal_eval(description(result_).strip('\n; '))
                        
                    # except:
                    #     try:
                    #         temp_ = description(result_).strip('\n; ')
                    #         if temp_[0] != '{':
                    #             temp_ = temp_[temp_.find('{'):]
                    #             table_column_used = json.loads(temp_)
                    #     except Exception:
                    #         print("         Skipping dict conversion for {0}".format(question))
                    # question_dict[question] = table_column_used
                result = Parallel(n_jobs=-1)(delayed(get_query_tables_columns)(file, question) for question in tqdm(self.queries[file]))
                # result = [get_query_tables_columns(file, question) for question in tqdm(self.queries[file])]
                for question, table_column_used in result:
                    question_dict[question] = table_column_used
                return file, question_dict
            return file, self.used_tables_columns[file]


        # # used_comps = Parallel(n_jobs=-1)(delayed(get_tables_columns)(file) for file in tqdm(self.queries.keys()))
        # used_comps = [get_tables_columns(file) for file in tqdm(self.queries.keys())]

        # for file, question_dict in used_comps:
        #     self.used_tables_columns[file] = question_dict
        
        # with open("datasets/metadata/train/"+"used_tables_columns.json", "w") as f:
        #     json.dump(self.used_tables_columns, f)

        for file in tqdm(self.queries.keys()):
            if file not in self.used_tables_columns:
                _, question_dict = get_tables_columns(file)
                self.used_tables_columns[file] = question_dict
                try:
                    with open(self.master_folder+"/train/"+"used_tables_columns.json", "w") as f:
                        json.dump(self.used_tables_columns, f)
                except TypeError:
                    import ipdb; ipdb.set_trace()
                    print(1)


    def get_train_data(self):
        self.training_data = {}
        self.extra_table_columns = {}

        for file in tqdm(self.used_tables_columns):
            # import ipdb; ipdb.set_trace()
            self.training_data[file] = {}
            all_table_dict = {}
            all_column_dict = {}
            all_join_dict = {}
            all_samples_dict = {}
            
            with open(self.master_folder+"/table/"+file+".json", "r") as f:
                all_table_dict = json.load(f)
            with open(self.master_folder+"/column/"+file+".json", "r") as f:
                all_column_dict = json.load(f)
            with open(self.master_folder+"/joins/"+file+".json", "r") as f:
                all_join_dict = json.load(f)
            with open(self.master_folder+"/samples/"+file+".json", "r") as f:
                all_samples_dict = json.load(f)

            for question in self.used_tables_columns[file]:
                self.training_data[file][question] = []
                try:
                    used_tables = list(self.used_tables_columns[file][question].keys())
                except AttributeError:
                    import ipdb; ipdb.set_trace()
                    print(question)
                all_tables = list(all_table_dict.keys())

                try:
                    extra_tables = random.sample([table for table in all_tables if table not in used_tables], random.randint(1,3))
                except ValueError:
                    try:
                        extra_tables = random.sample([table for table in all_tables if table not in used_tables], 1)
                    except ValueError:
                        extra_tables = []

                question_tables = used_tables+extra_tables

                question_tables = [i for i in question_tables if "*" not in i]

                for table in question_tables:
                    question_table_metadata = {}
                    question_table_metadata["table"] = table

                    question_table_metadata["table_description"] = all_table_dict.get(table, "")

                    if table in used_tables:
                        columns_list = self.used_tables_columns[file][question][table]
                        try:
                            columns_list = [i for i in columns_list if "*" not in i]
                        except TypeError:
                            raise Exception("Error in * removal for {0}".format(table))
                    else:
                        columns_list = random.sample(list(all_column_dict[table].keys()), random.randint(2,7))

                    try:    
                        columns_metadata = [(all_column_dict.get(table, {}).get(i, ""), all_samples_dict.get(table, {}).get(i, "")) for i in columns_list]
                    except AttributeError:
                        # import ipdb; ipdb.set_trace()
                        # print(table)
                        try:
                            if all_samples_dict[table] == []:
                                all_samples_dict[table] = {}
                            columns_metadata = [(all_column_dict.get(table, {}).get(i, ""), all_samples_dict.get(table, {}).get(i, "")) for i in columns_list]
                        except Exception as error:
                            print(error)
                    question_table_metadata["columns"] = {i:j for i, j in zip(columns_list, columns_metadata)}

                    question_table_metadata["joins"] = []
                    for join_ in all_join_dict["joins"]:
                        if len(join_) != 2:
                            continue
                        if table in join_:
                            question_table_metadata["joins"].append(join_)
                    self.training_data[file][question].append(question_table_metadata)
        with open(self.master_folder+"/train/all_dataset.json", "w") as f:
            json.dump(self.training_data, f)

    def get_train_data_v2(self):
        """Data points that are included in training data - 
        Table Name, Table Description, Column Name, Description, Type, Sample Values"""
        self.training_data = {}
        self.extra_table_columns = {}

        for file in tqdm(self.used_tables_columns):
            # import ipdb; ipdb.set_trace()
            self.training_data[file] = {}
            all_table_dict = {}
            all_column_dict = {}
            all_join_dict = {}
            all_samples_dict = {}
            all_datatypes_dict = {}
            
            with open(self.master_folder+"/table/"+file+".json", "r") as f:
                all_table_dict = json.load(f)
            with open(self.master_folder+"/column/"+file+".json", "r") as f:
                all_column_dict = json.load(f)
            with open(self.master_folder+"/joins/"+file+".json", "r") as f:
                all_join_dict = json.load(f)
            with open(self.master_folder+"/samples/"+file+".json", "r") as f:
                all_samples_dict = json.load(f)
            with open(self.master_folder+"/datatypes/"+file+".json", "r") as f:
                all_datatypes_dict = json.load(f)

            for question in self.used_tables_columns[file]:
                self.training_data[file][question] = []
                try:
                    used_tables = list(self.used_tables_columns[file][question].keys())
                except AttributeError:
                    raise Exception("Error in converting to list for used tables question keys {0}".format(question))
                all_tables = list(all_table_dict.keys())

                try:
                    extra_tables = random.sample([table for table in all_tables if table not in used_tables], random.randint(1,3))
                except ValueError:
                    try:
                        extra_tables = random.sample([table for table in all_tables if table not in used_tables], 1)
                    except ValueError:
                        extra_tables = []

                question_tables = used_tables+extra_tables

                question_tables = [i for i in question_tables if "*" not in i]

                for table in question_tables:
                    question_table_metadata = {}
                    question_table_metadata["table"] = table

                    question_table_metadata["table_description"] = all_table_dict.get(table, "")

                    if table in used_tables:
                        columns_list = self.used_tables_columns[file][question][table]
                        try:
                            columns_list = [i for i in columns_list if "*" not in i]
                        except TypeError:
                            raise Exception("Error in * removal from column list for {0}".format(table))
                    else:
                        columns_list = random.sample(list(all_column_dict[table].keys()), random.randint(2,7))

                    # import ipdb; ipdb.set_trace()
                    try:    
                        columns_metadata = [
                            {
                                "Description": all_column_dict.get(table, {}).get(i, ""), 
                                "Type": all_datatypes_dict.get(table, {}).get(i, ""), 
                                "Sample Values": all_samples_dict.get(table, {}).get(i, "")
                            } 
                            for i in columns_list
                        ]
                    except AttributeError:
                        # import ipdb; ipdb.set_trace()
                        # print(table)
                        try:
                            if all_samples_dict[table] == []:
                                all_samples_dict[table] = {}
                            columns_metadata = [
                                {
                                    "Description": all_column_dict.get(table, {}).get(i, ""), 
                                    "Type": all_datatypes_dict.get(table, {}).get(i, ""), 
                                    "Sample Values": all_samples_dict.get(table, {}).get(i, "")
                                } 
                                for i in columns_list
                            ]
                        except Exception as error:
                            print(error)
                            pass
                    question_table_metadata["columns"] = {i:j for i, j in zip(columns_list, columns_metadata)}

                    question_table_metadata["joins"] = []
                    for join_ in all_join_dict["joins"]:
                        if len(join_) != 2:
                            continue
                        if table in join_:
                            question_table_metadata["joins"].append(join_)
                    self.training_data[file][question].append(question_table_metadata)
        with open(self.master_folder+"/train/all_dataset.json", "w") as f:
            json.dump(self.training_data, f)


    def generate_query_explaination(self):
        pass

    def get_training_datasets(self):
        self.finetuning_dataset = []
        for file in tqdm(self.training_data):
            for question in self.training_data[file]:
                query = self.queries[file][question]
                metadata = self.training_data[file][question]
                system_prompt = """Generate syntactically correct {0} query to answer the question using the database metadata provided""".format(self.dialect)
                user_prompt = """Question : {0}\n DataBase Metadata: {1}""".format(question, metadata)
                assistant_prompt = "```sql\n{0}```".format(query)

                instance = {
                    "messages": [
                        {
                            "role": "system",
                            "content": system_prompt
                        },
                        {
                            "role": "user",
                            "content": user_prompt
                        },
                        {
                            "role": "assistant",
                            "content": assistant_prompt
                        }
                    ]
                }

                self.finetuning_dataset.append(instance)
        
        with open(self.master_folder+"/train/"+"finetune_dataset.jsonl", "w") as f:
            for instance in self.finetuning_dataset:
                f.write(json.dumps(instance)+"\n")

        # with open(self.master_folder+"/train/"+"finetune_dataset.jsonl", "w") as f:
        #     for instance in self.

        self.finetune_trainset = self.finetuning_dataset[:int(len(self.finetuning_dataset)*0.8)]
        self.finetune_testset = self.finetuning_dataset[int(len(self.finetuning_dataset)*0.8):]

        with open(self.master_folder+"/train/"+"finetune_train_dataset.jsonl", "w") as f:
            for instance in self.finetune_trainset:
                f.write(json.dumps(instance)+"\n")

        with open(self.master_folder+"/train/"+"finetune_test_dataset.jsonl", "w") as f:
            for instance in self.finetune_testset:
                f.write(json.dumps(instance)+"\n")

        self.small_finetune_trainset = self.finetune_trainset[:int(len(self.finetune_trainset)*0.5)]
        self.small_finetune_testset = self.finetune_testset[:int(len(self.finetune_testset)*0.5)]

        with open(self.master_folder+"/train/"+"small_finetune_train_dataset.jsonl", "w") as f:
            for instance in self.small_finetune_trainset:
                f.write(json.dumps(instance)+"\n")

        with open(self.master_folder+"/train/"+"small_finetune_test_dataset.jsonl", "w") as f:
            for instance in self.small_finetune_testset:
                f.write(json.dumps(instance)+"\n")


if __name__ == "__main__":
    load_dotenv()
    master_folder = "sql/metadata_v2"
    files = os.listdir(master_folder+"/questions")
    dialect = "Postgres"
    prep_data = PrepareData(files=files, dialect=dialect, master_folder=master_folder)
    prep_data.get_train_data_v2()
    prep_data.get_training_datasets()
        
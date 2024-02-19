import ast
import json
import os
import random
import re
import time
from typing import Dict, List
# import openai
from openai import OpenAI
from retry import retry
from tqdm import tqdm
# from joblib import Parallel, delayed
import concurrent.futures
from dotenv import load_dotenv

def get_tables(ddl: str)->List[str]:
    "Parse the ddl to return list of tables"
    statements = [i + ";" for i in ddl.split(";")]
    tables = [
        i[i.find("CREATE TABLE")+len("CREATE TABLE"):].strip().split(" ")[0] 
        for i in statements 
        if "CREATE TABLE" in i
    ]
    return tables

def get_columns(ddl: str, raw_columns: bool=False)->List[str]:
    "Parse ddl to return list of columns"
    columns = {}
    statements = [i + ";" for i in ddl.split(";")]
    for statement in statements:
        temp = ""
        if "CREATE TABLE IF NOT EXISTS" in statement:
            temp = "CREATE TABLE IF NOT EXISTS"
        elif "CREATE TABLE" in statement:
            temp = "CREATE TABLE"
        if temp != "":
            table = statement[statement.find(temp)+len(temp):].strip().split(" ")[0]
            columns_ = statement[statement.find(table)+len(table):].strip('();\n ').split('\n')
            if not raw_columns:
                column_list = [col.strip(' ,').split(' ')[0] for col in columns_]
            else:
                column_list = [col.strip(' ,') for col in columns_]
            column_list = [col for col in column_list if col != 'PRIMARY']
            table = table.strip('();\n ')
            columns[table] = column_list
    return columns

class LLM:
    def __init__(self, model: str="gpt-4"):
        # openai.api_key = os.environ["OPENAI_API_KEY"]
        self.model = model
        self.client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"),)

    @retry(tries=5, delay=5, max_delay=50, backoff=5, logger=True)
    def create(self, prompt: str, sleep: int=10, **kwargs):
        if "gpt" in self.model:
            result = self.client.chat.completions.create(
                messages=prompt,
                model=self.model
            )

        time.sleep(random.randint(5, sleep))
        return result

class PromptLibrary:
    def __init__(self) -> None:
        pass

    def database(self, database_name: str, table_list: List[str]):
        messages = []
        messages.append({
            "role": "system",
            "content": "You are a sql chatbot. Help with understanding database"
        })
        messages.append({
            "role": "user",
            "content": "You are given a database along with the list of tables in that database. \
                Using this describe the database in 2 lines. Give the domain and usecase of the database\
                      in the description provided.\n \
                        Database: {0}, \n Tables List: {1} \n\
                            Give the database description inside ```.".format(database_name, table_list)
        })
        return messages

    def table(self, database_name: str, table: str, column_list: str=List[str]):
        messages = []
        messages.append({
            "role": "system",
            "content": "You are a sql chatbot. Help with understanding table"
        })
        messages.append({
            "role": "user",
            "content": "You are given a table, the database along with the list of columns in that table. \
                Using this describe the table in <=2 lines.\n\
                        Database: {0}, \n Table name: {1}, Column list: {2}\n\
                            Give the table description inside ```.".format(database_name, table, column_list)
        })
        return messages

    def column(
            self, 
            database_name: str,
            database_description: str,
            table: str,
            table_description: str,
            column_list: List[str]
        ):
        messages = []
        messages.append({
            "role": "system",
            "content": "You are a sql chatbot. Help with understanding columns"
        })
        messages.append({
            "role": "user",
            "content": "You are given a table, it's description, the database, it's description along with the list of columns in that table. \
            Using this describe the columns in <=2 lines.\n\
            Database: {0}, Database Description: {1},\n Table: {2}, Table Description: {3}, \n Column list: {4}\n\
            Give the output in json format.\n\
            e.g Output: {{column_a: description_a, column_b: description_b}}\n\
            Each column should be a separate key in the json\n\
            Give the column description inside ```.".format(database_name, database_description, table, table_description,column_list)
        })
        return messages

    def samples(
            self,
            table: str,
            table_description: str,
            column_list: List[str],
            column_description: Dict[str, Dict[str, str]],
    ):
        messages = []
        messages.append({
            "role": "system",
            "content": "You are a sql chatbot. Help with generating samples for columns"
        })
        messages.append({
            "role": "user",
            "content": "You are given a table, it's description, column along with datatypes, and their descriptions. \
            Using this information, generate 5-10 samples for each column.\n\
            Table: {0}, Table Description: {1}, \n Column list: {2}, Column Descriptions: {3}\n\
            Give the output in json format.\n\
            e.g Output: {{column_a: [list of samples of column_a], column_b: [list of samples of column_b]}}\
            Only give the output samples inside ```".format(table, table_description, column_list, column_description)
        })
        return messages

    def joins(
            self,
            database_name: str,
            column_list: Dict[str, List[str]],
        ):
        messages = []
        messages.append({
            "role": "system",
            "content": " You are a sql chatbot. Find the list of joins given tables and columns"
        })
        messages.append({
            "role": "user",
            "content": """You are given a database name and dictionary of tables and column list. Generate the list of joins in the database such that each table is connected to every other directly or indirectly.\n
            You need to guess the joins based on database name and dictionary of table and column list and the principles of database design and management\n
            Keep a note of these points:\n
            1. Every table should be connected directly or indirectly to every other table.\n
            2. give at max one join for every combination of 2 tables\n
            3. Use the table and column only from the table-column dictionary\n
            4. Give the output enclosed in ```\n
            Input : Database is {0}, Table-Column list is {1}\n
            Give result only in this output json format: ```{{\"joins\":[{{\"table_1\":\"column_1\", \"table_2\":\"column_2\"}}, {{\"table_3\":\"column_3\", \"table_4\":\"column_4\"}}]}}```\n
            Only output the list of joins enclosed in ```\n""".format(database_name, column_list)
        })
        return messages

    def questions(
            self, 
            database_description: str, 
            table_description: str, 
            column_description: str, 
            joins: str,
            question_type: str="easy",
            dialect: str="MySQL"
        ):
        if question_type == "hard":
            conditions = """1. Greater than equal to 3 joins in query\n
            2. self joins\n
            3. cohort based\n
            4. have sub-queries and/or CTEs\n
            5. relevant business metric\n"""
            no_questions = 10
        elif question_type == "extra_hard":
            conditions = """1. Has SQL Components - WHERE, GROUP BY, ORDER BY, LIMIT, JOIN, OR, LIKE, HAVING\n
            2. Has SQL Components - EXCEPT, UNION, INTERSECT, NESTED\n
            3. Has Other components - number of agg > 1, number of select columns > 1, number of where conditions > 1, number of group by clauses > 1, number of group by clauses > 1 (no consider col1-col2 math equations etc.)"""
            
            no_questions = 20
            messages = []
            messages.append({
                "role": "system",
                "content": "You are a sql chatbot. Help with generating questions on the database based on provided info about the database"
            })
            messages.append({
                "role": "user",
                "content": """You are given the following information about the database:\n
            dictionary of database_name & database_description, dictionary of table_name & their descriptions, nested dictionary of column names and their descriptions, a list of common possible joins inside the database\n
            Based on this information generate {1} type of questions and their corresponding {7} queries as key-value pairs\n
            Strictly follow atleast 2 of the below questions:
            {2}
            The inputs provided are:\n
            database: {3}\n
            tables: {4}\n
            columns: {5}\n
            joins: {6}\n
            Generate {0} questions and there corresponding queries using the above information.\n
            Only use the table and columns provided in the list above.\n
            Give the result in json format.\n
            Use this output format, where questions are keys and sql queries are values: {{\"question_1\":\"query_1\", \"question_2\":\"query_2\"}}\n
            Be creative in generating the questions\n
            Avoid mentioninig the exact table and column names in question, prefer business metrics instead\n
            Give question as key and {7} query as value in json format, enclosed in ```\n""".format(
                no_questions,
                question_type,
                conditions,
                database_description,
                table_description,
                column_description,
                joins,
                dialect,
            )
            })
            return messages

        elif question_type == "medium":
            conditions = """1. 1-2 joins in query\n
            2. relevant business metric\n
            3. if possible, have date and timestamps manipulation\n"""
            no_questions = 10
        else:
            conditions = """1. 0 or 1 joins in query\n
            2. use filters\n
            3. simple relevant business metric\n"""
            no_questions = 20
        messages = []
        messages.append({
            "role": "system",
            "content": "You are a sql chatbot. Help with generating questions on the database based on provided info about the database"
        })
        messages.append({
            "role": "user",
            "content": """You are given the following information about the database:\n
            dictionary of database_name & database_description, dictionary of table_name & their descriptions, nested dictionary of column names and their descriptions, a list of common possible joins inside the database\n
            Based on this information generate {1} type of questions and their corresponding {7} queries as key-value pairs\n
            Strictly follow atleast 1 of the below questions:
            {2}
            The inputs provided are:\n
            database: {3}\n
            tables: {4}\n
            columns: {5}\n
            joins: {6}\n
            Generate {0} questions and there corresponding queries using the above information.\n
            Only use the table and columns provided in the list above.\n
            Give the result in json format.\n
            Use this output format, where questions are keys and sql queries are values: {{\"question_1\":\"query_1\", \"question_2\":\"query_2\"}}\n
            Be creative in generating the questions\n
            Avoid mentioninig the exact table and column names in question, prefer business metrics instead\n
            Give question as key and {7} query as value in json format, enclosed in ```\n
            """.format(
                no_questions,
                question_type,
                conditions,
                database_description,
                table_description,
                column_description,
                joins,
                dialect,
            )
        })
        return messages

class MetaData:
    """Order of metadata creation (also the order of dependecny) : 
    Database MetaData -> Table Metadata -> 
    Column Metadata -> Column Sample Values -> 
    Joins & Foreign Keys -> Business questions and SQL queries"""
    def __init__(self, sql_folder: str="sql/created", metadata_folder: str="sql/metadata", model: str="gpt-4"):
        self.sql_folder = sql_folder
        self.prompt_library = PromptLibrary() 
        self.metadata_folder = metadata_folder
        self.llm = LLM(model=model)
        self.post_init()

    def post_init(self):
        """Database metadata needs to be created first"""
        self.database_metadata = {}
        print("Creating database metadata")

        if os.path.isfile(self.metadata_folder + "/" + "database_metadata.json"):
            with open("" + self.metadata_folder + "/" + "database_metadata.json", "r") as f:
                self.database_metadata = json.load(f)

        def get_db_metadata(file):
            database_name = "_".join(file.split(".")[0].split("_")[:-1])
            database_id = file.split(".")[0]
                
            if database_id not in self.database_metadata:
                with open(self.sql_folder + "/" + file, "r") as f:
                    ddl = f.read()

                table_list = get_tables(ddl)
                prompt = self.prompt_library.database(database_name, table_list)
                result = self.llm.create(prompt, temperature=0.0)
                result_ = re.findall(r"```([\s\S]*?)```", result.choices[0].message.content)
                description = lambda x: x[0] if len(x)>0 else result.choices[0].message.content

                return (
                    database_id, 
                    database_name, 
                    description(result_))
            return (
                database_id, 
                self.database_metadata[database_id]["database"], 
                self.database_metadata[database_id]["description"])
        
        files = [file for file in os.listdir(self.sql_folder)]
        num_processes = None
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_processes) as executor:
            result = list(executor.map(get_db_metadata, files))
        for (database_id, database_name, database_description) in result:
            self.database_metadata[database_id] = {
                "database": database_name,
                "description": database_description
            }

        if os.path.exists(self.metadata_folder)==False:
            os.mkdir(self.metadata_folder)
        with open(self.metadata_folder+ "/" + "database_metadata.json", "w") as f:
            json.dump(self.database_metadata, f)

    def tables(self):
        """Table metadata"""
        self.table_metadata = {}

        if os.path.exists(os.path.join(self.metadata_folder,"table"))==False:
            os.mkdir(os.path.join(self.metadata_folder,"table"))

        def get_tables_metadata(file):
            print("     Creating table metadata for {0}".format(file))
            database_name = "_".join(file.split(".")[0].split("_")[:-1])
            # database_id = hash(file)
            database_id = file.split(".")[0]
            table_description = {}
            
            with open(self.sql_folder + "/" + file, "r") as f:
                ddl = f.read()
            column_lists = get_columns(ddl)

            if os.path.isfile(self.metadata_folder + "/table/" + str(database_id) + ".json"):
                with open("" + self.metadata_folder + "/table/" + str(database_id) + ".json", "r") as f:
                    table_description = json.load(f)
            
            if table_description == {}:
                for table in column_lists:
                    print("         Creating metadata for table {0}".format(table))
                    if table not in table_description:
                        prompt = self.prompt_library.table(database_name, table, column_lists[table])
                        result = self.llm.create(prompt, temperature=0.0)
                        result_ = re.findall(r"```([\s\S]*?)```", result.choices[0].message.content)
                        description = lambda x: x[0] if len(x)>0 else result.choices[0].message.content
                        table_description[table] = description(result_).strip('\n; ')
            
            with open(self.metadata_folder+'/table/'+str(database_id)+'.json', 'w') as file:
                json.dump(table_description, file)

            return (database_id, table_description)
            
        files = [file for file in os.listdir(self.sql_folder)]
        num_processes = None
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_processes) as executor:
            result = list(executor.map(get_tables_metadata, files))
        # result = Parallel(n_jobs=-1)(delayed(get_tables_metadata)(file) for file in tqdm(os.listdir(self.sql_folder)))
        for (database_id, table_description) in result:
            self.table_metadata[database_id] = table_description
        
    def columns(self):
        """Columns metadata"""
        self.column_metadata = {}

        if os.path.exists(os.path.join(self.metadata_folder,"column"))==False:
            os.mkdir(os.path.join(self.metadata_folder,"column"))

        print(" Creating column metadata--------------")
        def get_columns_metadata(file):
            print("     Creating column metadata for {0}".format(file))
            database_name = "_".join(file.split(".")[0].split("_")[:-1])
            # database_id = hash(file)
            database_id = file.split(".")[0]
            column_description = {}
            
            with open(self.sql_folder + "/" + file, "r") as f:
                ddl = f.read()
            column_lists = get_columns(ddl)

            if os.path.isfile(self.metadata_folder + "/column/" + str(database_id) + ".json"):
                with open("" + self.metadata_folder + "/column/" + str(database_id) + ".json", "r") as f:
                    column_description = json.load(f)
            
            if column_description == {}:
                for table in column_lists:
                    print("         Creating metadata for table {0}".format(table))
                    if table not in column_description:
                        prompt = self.prompt_library.column(
                            database_name, 
                            self.database_metadata[database_id], 
                            table, 
                            self.table_metadata[database_id], 
                            column_lists[table]
                        )
                        result = self.llm.create(prompt, temperature=0.0)
                        result_ = re.findall(r"```([\s\S]*?)```", result.choices[0].message.content)
                        description = lambda x: x[0] if len(x)>0 else result.choices[0].message.content
                        # column_description[table] = ast.literal_eval(description(result_).strip('\n; '))
                        try:
                            cleaned_str = description(result_).strip('\n; ')
                            if cleaned_str[:4]=='json':
                                cleaned_str = cleaned_str[4:].strip('\n; ')
                            column_description[table] = ast.literal_eval(cleaned_str)
                        except SyntaxError:
                            print(description(result_).strip('\n; '))
                            raise Exception("What's this weird bug!!!")
            
            with open(self.metadata_folder+'/column/'+str(database_id)+'.json', 'w') as file:
                json.dump(column_description, file)

            return (database_id, column_description)
        
        files = [file for file in os.listdir(self.sql_folder)]
        num_processes = None
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_processes) as executor:
            result = list(executor.map(get_columns_metadata, files))
        for (database_id, column_description) in result:
            self.column_metadata[database_id] = column_description       
        

    def samples(self):
        """Generating column samples"""
        self.generated_samples = {}

        if os.path.exists(os.path.join(self.metadata_folder,"samples"))==False:
            os.mkdir(os.path.join(self.metadata_folder,"samples"))

        print(" Generating column samples--------------")

        def get_samples(file):
            print("     Creating column samples for {0}".format(file))
            # database_name = "_".join(file.split(".")[0].split("_")[:-1])
            # database_id = hash(file)
            database_id = file.split(".")[0]
            sample_values = {}
            
            with open(self.sql_folder + "/" + file, "r") as f:
                ddl = f.read()
            column_lists = get_columns(ddl, raw_columns=True)

            if os.path.isfile(self.metadata_folder + "/samples/" + str(database_id) + ".json"):
                with open("" + self.metadata_folder + "/samples/" + str(database_id) + ".json", "r") as f:
                    sample_values = json.load(f)

            def get_table_samples(table):
                print("         Generating samples for table {0}".format(table))
                samples_ = []
                if table in sample_values:
                    samples_ = sample_values[table]
                if table not in sample_values:
                    prompt = self.prompt_library.samples(
                        table, 
                        self.table_metadata[database_id], 
                        column_lists[table],
                        self.column_metadata[database_id][table]
                    )
                    result = self.llm.create(prompt, temperature=0.0)
                    result_ = re.findall(r"```([\s\S]*?)```", result.choices[0].message.content)
                    description = lambda x: x[0] if len(x)>0 else result.choices[0].message.content
                    try:
                        samples_ = ast.literal_eval(description(result_).strip('\n; '))
                    except:
                        try:
                            samples_ = description(result_).strip('\n; ')
                            if samples_[0] != '{':
                                samples_ = samples_[samples_.find('{'):]
                                samples_ = json.loads(samples_)
                        except Exception as error:
                            print("         Skipping sample value creation for {0}".format(table))
                            print("         Error: {0}".format(error))
                            return (table, [])
                return (table, samples_)                     
            
            files = [file for file in os.listdir(self.sql_folder)]
            num_processes = None
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_processes) as executor:
                result = list(executor.map(get_table_samples, files))
            for table, sample_value in result:
                sample_values[table] = sample_value
            
            with open(self.metadata_folder+'/samples/'+str(database_id)+'.json', 'w') as file:
                json.dump(sample_values, file)

            return (database_id, sample_values)
        
        result = [get_samples(file) for file in tqdm(os.listdir(self.sql_folder))]
        for (database_id, sample_values) in result:
            self.generated_samples[database_id] = sample_values        

    def joins(self):
        """Generating Joins"""
        self.generated_joins = {}

        if os.path.exists(os.path.join(self.metadata_folder,"joins"))==False:
            os.mkdir(os.path.join(self.metadata_folder,"joins"))

        print(" Generating joins--------------")

        def get_joins_metadata(file):
            print("     Creating join metadata for database {0}".format(file))
            database_name = "_".join(file.split(".")[0].split("_")[:-1])
            # database_id = hash(file)
            database_id = file.split(".")[0]
            join_metadata = {}
            
            with open(self.sql_folder + "/" + file, "r") as f:
                ddl = f.read()
            column_lists = get_columns(ddl)

            if os.path.isfile(self.metadata_folder + "/joins/" + str(database_id) + ".json"):
                with open("" + self.metadata_folder + "/joins/" + str(database_id) + ".json", "r") as f:
                    join_metadata = json.load(f)
            
            if join_metadata == {}:
                prompt = self.prompt_library.joins(
                    database_name,
                    column_lists
                )
                result = self.llm.create(prompt, temperature=0.0)
                result_ = re.findall(r"```([\s\S]*?)```", result.choices[0].message.content)
                description = lambda x: x[0] if len(x)>0 else result.choices[0].message.content
                try:
                    join_metadata = ast.literal_eval(description(result_).strip('\n; '))
                except:
                    try:
                        join_metadata = description(result_).strip('\n; ')
                        if join_metadata[0] != '{':
                            join_metadata = join_metadata[join_metadata.find('{'):]
                            join_metadata = json.loads(join_metadata)
                    except Exception as error:
                        print("         Skipping join metadata creation for {0}".format(database_name))
                        print("         Error: {0}".format(error))
                        return (database_id, [])
            
            with open(self.metadata_folder+'/joins/'+str(database_id)+'.json', 'w') as file:
                json.dump(join_metadata, file)

            return (database_id, join_metadata)

        files = [file for file in os.listdir(self.sql_folder)]
        num_processes = None
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_processes) as executor:
            result = list(executor.map(get_joins_metadata, files))
        for (database_id, join_metadata) in result:
            self.generated_joins[database_id] = join_metadata


    def questions(self):
        """Generating Questions"""
        self.generated_questions = {}

        if os.path.exists(os.path.join(self.metadata_folder,"questions"))==False:
            os.mkdir(os.path.join(self.metadata_folder,"questions"))

        print(" Generating questions--------------")

        def wrapper_get_questions(file):
            _, extra_hard_ = get_questions(file, "extra_hard")
            database_id, easy_ = get_questions(file, "easy")
            _, medium_ = get_questions(file, "medium")
            _, hard_ = get_questions(file, "hard")

            def clean_output(question_dictionary):
                total_keys = question_dictionary.keys()
                updated_dictionary = {}
                for i in range(len(total_keys)):
                    if "question_"+str(i+1) in total_keys:
                        updated_dictionary[question_dictionary["question_"+str(i+1)]] = question_dictionary["query_"+str(i+1)]
                
                for key in total_keys:
                    if "question_" not in key and "query_" not in key:
                        updated_dictionary[key] = question_dictionary[key]
                return updated_dictionary

            combined_questions = {}

            try:
                easy_ = clean_output(easy_)
                combined_questions.update(easy_)
            except:
                pass

            try:
                medium_ = clean_output(medium_)
                combined_questions.update(medium_)
            except:
                pass

            try:
                hard_ = clean_output(hard_)
                combined_questions.update(hard_)
            except:
                pass

            try:
                extra_hard_ = clean_output(extra_hard_)
                combined_questions.update(extra_hard_)
            except:
                pass

            if combined_questions == {}:
                print("     0 questions generated for database {0}".format(file))

            with open(self.metadata_folder+'/questions/'+str(database_id)+'.json', 'w') as file:
                json.dump(combined_questions, file)

            return database_id, combined_questions
        
        def get_questions(file, question_type):
            print("     Creating {0} questions for database {1}".format(question_type, file))
            database_name = "_".join(file.split(".")[0].split("_")[:-1])
            # database_id = hash(file)
            database_id = file.split(".")[0]
            question_metadata = {}
            
            # with open(self.sql_folder + "/" + file, "r") as f:
            #     ddl = f.read()
            # column_lists = get_columns(ddl)

            if os.path.isfile(self.metadata_folder + "/questions/" + str(database_id) + ".json"):
                with open("" + self.metadata_folder + "/questions/" + str(database_id) + ".json", "r") as f:
                    question_metadata = json.load(f)
            
            if question_metadata == {}:
                prompt = self.prompt_library.questions(
                    self.database_metadata[database_id],
                    self.table_metadata[database_id],
                    self.column_metadata[database_id],
                    self.generated_joins[database_id],
                    question_type,
                    dialect="Postgres",
                )
                result = self.llm.create(prompt, temperature=0.0)
                print("     Total tokens used: {0}".format(result["usage"]["total_tokens"]))
                result_ = re.findall(r"```([\s\S]*?)```", result.choices[0].message.content)
                description = lambda x: x[0] if len(x)>0 else result.choices[0].message.content
                try:
                    question_metadata = ast.literal_eval(description(result_).strip('\n; '))
                except:
                    try:
                        question_metadata = description(result_).strip('\n; ')
                        if question_metadata[0] != '{':
                            question_metadata = question_metadata[question_metadata.find('{'):]
                            question_metadata = json.loads(question_metadata)
                    except Exception as error:
                        print("         Skipping join metadata creation for {0}".format(database_name))
                        print("         Error: {0}".format(error))
                        return (database_id, [])

            return (database_id, question_metadata)

        files = [file for file in os.listdir(self.sql_folder)]
        num_processes = None
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_processes) as executor:
            result = list(executor.map(wrapper_get_questions, files))
        for (database_id, questions) in result:
            self.generated_questions[database_id] = questions

    def datatypes(self):
        """Extracting the data types from the ddl file for databases"""
        files = os.listdir(self.sql_folder)
        # datatype_dict = {}
        for ddl_file in files:
            datatype_dict = {}
            with open(os.path.join(self.sql_folder, ddl_file), "r") as f:
                ddl = f.read()
            column_lists = get_columns(ddl, raw_columns=True)
            primary_column = ""
            for table in column_lists.keys():
                for i in range(len(column_lists[table])):
                    column = column_lists[table][i]
                    if len(column.split())>2 and column.split()[0] == "PRIMARY" and column.split()[1] == "KEY":
                        primary_column = " ".join(column.split()[2:]).strip(' ()')
                        del column_lists[table][i]
                        break
            for table in column_lists.keys():
                datatype_dict[table] = {}
                for i in range(len(column_lists[table])):
                    temp = column_lists[table][i].split()
                    try:
                        column, datatype = temp[0].strip(' ()'), " ".join(temp[1:]).strip(' (')
                    except IndexError:
                        pass
                    if primary_column == column:
                        if "PRIMARY KEY" not in datatype:
                            datatype += "PRIMARY KEY"
                    datatype_dict[table][column] = datatype
            with open(os.path.join(self.metadata_folder, 'datatypes', ddl_file.replace(".sql", ".json")), "w") as f:
                json.dump(datatype_dict, f)
            

    def clean_metadata(self):
        if os.path.exists(os.path.join(self.metadata_folder,"cleaned_table"))==False:
            os.mkdir(os.path.join(self.metadata_folder,"cleaned_table"))
        if os.path.exists(os.path.join(self.metadata_folder,"cleaned_column"))==False:
            os.mkdir(os.path.join(self.metadata_folder,"cleaned_column"))
        if os.path.exists(os.path.join(self.metadata_folder,"cleaned_joins"))==False:
            os.mkdir(os.path.join(self.metadata_folder,"cleaned_joins"))

        # cleaning database metadata
        for key in self.database_metadata:
            self.database_metadata[key]['description'] = self.database_metadata[key]['description'].replace("\n", "")
        with open(self.metadata_folder+'/database_metadata.json', 'w') as file:
            json.dump(self.database_metadata, file)

        # cleaning table metadata
        self.new_table_metadata = {}
        for database_id in self.table_metadata:
            self.new_table_metadata[database_id] = {}
            for table_key in self.table_metadata[database_id]:
                # cleaned_table_key = table_key.replace("\n", "").replace("(", "").replace(")", "")
                cleaned_table_key = table_key
                if cleaned_table_key.find("\n") != -1:
                    cleaned_table_key = cleaned_table_key[:cleaned_table_key.find("\n")]
                if cleaned_table_key.find("(") != -1:
                    cleaned_table_key = cleaned_table_key[:cleaned_table_key.find("(")]
                if cleaned_table_key.find(")") != -1:
                    cleaned_table_key = cleaned_table_key[:cleaned_table_key.find(")")]
                if cleaned_table_key.find(".") != -1:
                    cleaned_table_key = cleaned_table_key[cleaned_table_key.find(".")+1:]
                self.new_table_metadata[database_id][cleaned_table_key] = self.table_metadata[database_id][table_key]
                if cleaned_table_key != table_key:
                    print("Cleaned {0} table_key to {1}".format(table_key, cleaned_table_key))
            with open(self.metadata_folder+'/cleaned_table/'+str(database_id)+'.json', 'w') as file:
                json.dump(self.new_table_metadata[database_id], file)

        # cleaning columns metadata
        self.new_column_metadata = {}
        for database_id in self.column_metadata:
            self.new_column_metadata[database_id] = {}
            for table_key in self.column_metadata[database_id]:
                # cleaned_table_key = table_key.replace("\n", "").replace("(", "").replace(")", "")
                cleaned_table_key = table_key
                if cleaned_table_key.find("\n") != -1:
                    cleaned_table_key = cleaned_table_key[:cleaned_table_key.find("\n")]
                if cleaned_table_key.find("(") != -1:
                    cleaned_table_key = cleaned_table_key[:cleaned_table_key.find("(")]
                if cleaned_table_key.find(")") != -1:
                    cleaned_table_key = cleaned_table_key[:cleaned_table_key.find(")")]
                if cleaned_table_key.find(".") != -1:
                    cleaned_table_key = cleaned_table_key[cleaned_table_key.find(".")+1:]
                self.new_column_metadata[database_id][cleaned_table_key] = self.column_metadata[database_id][table_key]
                if cleaned_table_key != table_key:
                    print("Cleaned {0} table_key to {1}".format(table_key, cleaned_table_key))
            with open(self.metadata_folder+'/cleaned_column/'+str(database_id)+'.json', 'w') as file:
                json.dump(self.new_column_metadata[database_id], file)

        # cleaning joins metadata
        self.new_join_metadata = {}
        for database_id in self.generated_joins:
            self.new_join_metadata[database_id] = {}
            self.new_join_metadata[database_id]['joins'] = []
            try:
                for join_dict in self.generated_joins[database_id]['joins']:
                    new_join_dict = {}
                    for table_key in join_dict:
                        # cleaned_table_key = table_key.replace("\n", "").replace("(", "").replace(")", "")
                        cleaned_table_key = table_key
                        
                        if cleaned_table_key.find("\n") != -1:
                            cleaned_table_key = cleaned_table_key[:cleaned_table_key.find("\n")]
                        if cleaned_table_key.find("(") != -1:
                            cleaned_table_key = cleaned_table_key[:cleaned_table_key.find("(")]
                        if cleaned_table_key.find(")") != -1:
                            cleaned_table_key = cleaned_table_key[:cleaned_table_key.find(")")]
                        if cleaned_table_key.find(".") != -1:
                            cleaned_table_key = cleaned_table_key[cleaned_table_key.find(".")+1:]
                        new_join_dict[cleaned_table_key] = join_dict[table_key]
                        if cleaned_table_key != table_key:
                            print("Cleaned {0} table_key to {1}".format(table_key, cleaned_table_key))
                    self.new_join_metadata[database_id]['joins'].append(new_join_dict)
            except:
                continue
            with open(self.metadata_folder+'/cleaned_joins/'+str(database_id)+'.json', 'w') as file:
                json.dump(self.new_join_metadata[database_id], file)
        

if __name__ == "__main__":
    load_dotenv()
    # metadata = MetaData(model="gpt-4-1106-preview", sql_folder="datasets/created_v2", metadata_folder="datasets/metadata_v2")
    # metadata.tables()
    # metadata.columns()
    # metadata.samples()
    # metadata.joins()
    # metadata.questions()
    # metadata.datatypes()
    # metadata.clean_metadata()

    # metadata = MetaData(model="gpt-4-1106-preview", sql_folder="datasets/created_v3", metadata_folder="datasets/metadata_v3")
    metadata = MetaData(model="gpt-3.5-turbo-1106", sql_folder="sql/created_v3", metadata_folder="sql/metadata_v3")
    # metadata.tables()
    # metadata.columns()
    # metadata.samples()
    # metadata.joins()
    # metadata.questions()
    # metadata.clean_metadata()
    metadata.datatypes()
    print("Success")

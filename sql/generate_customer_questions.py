# based on metadata generate question
# generate corresponding athena queries
# use the queries document provided by Blotout as base
# 3 methods to generate questions:
# a. generate sub-queries, new queries from the queries provided by Blotout 
# b. generate new queries out of blue using business information and database metadata.
# c. Add handmade questions
# Additionally - Use the KPI names provided by Mandar. Search for publicly available questions on shopify dataset.
import os
import json
import random
from joblib import Parallel, delayed
from generate_metadata import LLM
from tqdm import tqdm
import pandas as pd
from dotenv import load_dotenv

def blotout_prompt(table_metadata,subset_columns):
    messages = []
    messages.append({
        "role": "system",
        "content": "You are given columns from an e-commerce database that stores shoppify-type data. Create questions and corresnponding Athena queries."
    })
    messages.append({
        "role": "user",
        "content": """For the given table and these columns and their metadata. Give a list of 30 business relevant questions with business terminologies related to shopify e-commerce space. And additionally give their corresponding Athena queries. Give the question and query in json format as markdown (enclosed in ```)\n
        example output : ```{{'what is the population?': 'select count(*) from table; 'what is population in 2022': 'select count(*) from table where year = 2022 }}```\n
        Input: table: {0}, columns: {1}\n
        Remember to give the output in markdown format.""".format(table_metadata, subset_columns)
    })

    return messages

def generate_questions(
        id: int,
        llm: LLM, 
        table: str, 
        all_tables,
        columns_metadata, 
        subset_column_id
    ):
    print("Running id: {0}".format(id))
    subset_columns = [columns_metadata[id] for id in subset_column_id]

    table_metadata = [
        metadata 
        for metadata in all_tables 
        if metadata["TableName"]==table.split('.')[0]
    ][0]

    prompt = blotout_prompt(
        table_metadata, 
        subset_columns
    )

    result = llm.create(prompt, temperature=0.0)
    print("Completed id: {0}".format(id))
    # import ipdb; ipdb.set_trace()

    return (subset_columns, result.choices[0].message.content)


class QuestionGeneration():
    def __init__(self, connection_folder: str, model: str="gpt-4", provided_questions_folder: str="provided_questions"):
        self.connection_folder = connection_folder
        self.llm = LLM(model=model)
        self.provided_questions_folder = provided_questions_folder

    def generate_question(self):
        all_tables, tables_metadata = '',{}
        files = os.listdir(self.connection_folder)
        # import ipdb; ipdb.set_trace()
        generated_questions = {}
        selected_columns = []
        
        for file in files:
            if ".json" in file:
                if "all_tables.json" in file:
                    with open(os.path.join(self.connection_folder, file)) as json_file:
                        all_tables = json.load(json_file)
                else:
                    with open(os.path.join(self.connection_folder, file)) as json_file:
                        tables_metadata[file] = json.load(json_file)

        # combinatorial generations inside each table
        for table in tables_metadata.keys():
            print("Generating for table: {0}".format(table))
            columns_metadata = tables_metadata[table]
            total_columns = len(columns_metadata)
            subset_column_ids = []
            for _ in range(10):
                column_ids = random.sample(range(total_columns), 5)
                subset_column_ids.append(column_ids)

            # import ipdb; ipdb.set_trace()
            # single threaded generation
            # result = [generate_questions(
            #     self.llm,
            #     table,
            #     all_tables,
            #     columns_metadata,
            #     subset_column_id
            # ) for subset_column_id in tqdm(subset_column_ids)]

            # parallel generation
            result = Parallel(n_jobs=-1)(delayed(generate_questions)(
                i,
                self.llm, 
                table, 
                all_tables,
                columns_metadata, 
                subset_column_id) 
                for i, subset_column_id in enumerate(subset_column_ids)
            )
            
            # import ipdb; ipdb.set_trace()
            for subset_columns, generation in result:
                try:
                    cleaned_generation = generation[generation.find("```json")+7:]
                    cleaned_generation = cleaned_generation.strip("```")
                    dict_cleaned_generation = json.loads(cleaned_generation)
                    
                    pre_additions = len(generated_questions)
                    generated_questions.update(dict_cleaned_generation)
                    post_additions = len(generated_questions)
                    selected_columns += [subset_columns for _ in range(post_additions-pre_additions)]
                    if len(selected_columns) != len(generated_questions):
                        raise Exception("Length mismatch for selected_columns and generated)_questions")
                except Exception as e:
                    print("Error: {0}".format(e))
                    continue
        
        # import ipdb; ipdb.set_trace()
        try:
            assert len(selected_columns) == len(generated_questions)
        except Exception as e:
            # import ipdb; ipdb.set_trace()
            # print(e)
            raise Exception(e)

        with open(os.path.join(self.connection_folder, "generated_questions.json"), "w") as generation_file:
            json.dump(generated_questions, generation_file)

        with open(os.path.join(self.connection_folder, "selected_columns.json"), "w") as selection_file:
            json.dump(selected_columns, selection_file)
            

    def derived_queries(self, filename: str):
        # question_sets = pd.read_csv(os.path.join(self.connection_folder, self.provided_questions_folder, filename))
        # questions_dict = {}

        pass

    def handmade_questions(self, filename: str):
        question_sets = pd.read_csv(os.path.join(self.connection_folder, self.provided_questions_folder, filename))

        with open(os.path.join(self.connection_folder, "generated_questions.json"), "r") as generation_file:
            generated_questions = json.load(generation_file)
        
        for index, row in question_sets.iterrows():
            question = row["Question"]
            query = row["Query"]
            generated_questions[question] = query

        # selected columns has not been updated here, need to do manually
        with open(os.path.join(self.connection_folder, "generated_questions.json"), "w") as generation_file:
            json.dump(generated_questions, generation_file)

class Dataset():
    def __init__(self, connection_folder, dialect: str="MySQL"):
        self.connection_folder = connection_folder
        self.dialect = dialect
        with open(os.path.join(connection_folder, "generated_questions.json"), "r") as generation_file:
            self.generated_questions = json.load(generation_file)
        with open(os.path.join(connection_folder, "selected_columns.json"), "r") as selection_file:
            self.selected_columns = json.load(selection_file)
    
    def create_training_data(self):
        training_data = []
        for question, selected_columns in zip(self.generated_questions, self.selected_columns):
            # Adding RAG randomness
            tables = set([i["Table"] for i in selected_columns])
            selected_columns_set = set([i["Field"] for i in selected_columns])
            rag_columns = []
            for table in tables:
                with open(os.path.join(self.connection_folder, table+".json"), "r") as table_file:
                    table_metadata = json.load(table_file)
                all_columns_set = set([i["Field"] for i in table_metadata])
                leftout_set = all_columns_set-selected_columns_set
                if len(leftout_set) <= 2:
                    additional_set = leftout_set
                else:
                    additional_set = random.sample(leftout_set, 2)
                additional_columns = [column_metadata for column_metadata in table_metadata if column_metadata["Field"] in additional_set]
                rag_columns += selected_columns+additional_columns

            # Preparing the prompt structure
            train_instance = {}
            train_instance["messages"] = []
            train_instance["messages"].append(
                {
                    "role": "system",
                    "content": "You are an {0} assistant. Generate syntactically correct queries based on the metadata and question asked.".format(self.dialect)
                }
            )
            train_instance["messages"].append(
                {
                    "role": "user",
                    "content": """You are given a question and the metadata for all the columns that are best suited to answer this question.
                    Use your query expertise to create a syntactically correct query in {0} to answer the question. 
                    Use the concepts of database management systems querying to write an efficient and easy to understand query.
                    ----------------
                    Question: {1}
                    Metadata: 
                    {2}
                    ----------------
                    Answer the above question in {0}
                    Return the query output as markdown.
                    e.g ```sql\n select * from table;```""".format(self.dialect, question, rag_columns)
                }
            )
            train_instance["messages"].append(
                {
                    "role": "assistant",
                    "content": "```sql\n"+self.generated_questions[question]+"```"
                }
            )
            training_data.append(train_instance)

        with open(os.path.join(self.connection_folder, "training", "training_data.jsonl"), "w") as f:
            for instance in training_data:
                f.write(json.dumps(instance)+"\n")


if __name__ == "__main__":
    load_dotenv()
    
    connection_folder = "/Users/abhinaykumar/Desktop/Work/featherflow/llama-tests/datasets/25dd7090-0b9f-4ffe-aafb-ad3266011955"
    question_generation = QuestionGeneration(connection_folder, model="gpt-4")
    # question_generation.generate_question()
    # question_generation.handmade_questions("blotout questions.csv")
    # with open(os.path.join(connection_folder, "generated_questions.json"), "r") as generation_file:
    #     generated_questions = json.load(generation_file)
    # with open(os.path.join(connection_folder, "selected_columns.json"), "r") as selection_file:
    #     selected_columns = json.load(selection_file)
    # import ipdb; ipdb.set_trace()
    # print(1)
    dataset = Dataset(connection_folder, dialect="Athena")
    dataset.create_training_data()


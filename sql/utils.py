from openai import OpenAI
import os
from joblib import Parallel, delayed
from retry import retry
from tqdm import tqdm
from data_quality import create_connection, execute_query
from dotenv import load_dotenv

@retry(tries=10, delay=4, max_delay=50, backoff=3, logger=True)
def convert_msql_to_pg(ddl_file, folder, dst_folder):

    if os.path.exists(os.path.join(dst_folder, ddl_file)):
        return

    with open(os.path.join(folder, ddl_file), "r") as file:
        ddl_content = file.read()

    system = "You are a MySQL and PostGres expert. Help me change MySQL DDL file to Postgres DDL file"
    user = "This is the equvalent DDL file in MYSQL:\n {0} \n Make relevant syntax changes to this and convert to PostGres Compatible DDL file.\n Keep the Column and Table names the same as before. Return the updates DDL in markdown enclosed in ```".format(ddl_content)


    messages = []
    messages.append({
        "role": "system",
        "content": system
    })

    messages.append({
        "role": "user",
        "content": user
    })

    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"),)
    result = client.chat.completions.create(
        messages=messages, 
        model="gpt-4-1106-preview"
    )

    output = result.choices[0].message.content

    updated_ddl = output.split("```sql")[-1].split("```")[0]

    with open(os.path.join(dst_folder, ddl_file), "w") as file:
        file.write(updated_ddl)

    return updated_ddl

def convert_files():
    folders = [
        ("/Users/abhinaykumar/Desktop/Work/featherflow/llama-tests/datasets/created", "/Users/abhinaykumar/Desktop/Work/featherflow/llama-tests/datasets/created_old/created"),
        ("/Users/abhinaykumar/Desktop/Work/featherflow/llama-tests/datasets/created_v2", "/Users/abhinaykumar/Desktop/Work/featherflow/llama-tests/datasets/created_old/created_v2"),
        ("/Users/abhinaykumar/Desktop/Work/featherflow/llama-tests/datasets/created_v3", "/Users/abhinaykumar/Desktop/Work/featherflow/llama-tests/datasets/created_old/created_v3")
    ]


    for dst_folder, folder in folders:
        results = Parallel(n_jobs=-1)(delayed(convert_msql_to_pg)(ddl_file, folder, dst_folder) for ddl_file in tqdm(os.listdir(folder)))
        # import ipdb; ipdb.set_trace()

def drop_databases(folder):
    db_user, db_password, db_host, db_port = "postgres", "postgres", "localhost", "5432"
    db_names = [file.split(".")[0].lower() for file in os.listdir(folder)]

    for db_name in db_names:
        connection = create_connection("postgres", db_user, db_password, db_host, db_port)
        if connection is not None:
            query = "DROP DATABASE IF EXISTS {0}".format(db_name)
            error = execute_query(connection, query)

            if error is not None:
                print("Couldnot delete db : {0} due to this error : \n {1}".format(db_name, error))

if __name__ == "__main__":
    load_dotenv()

    drop_databases("/Users/abhinaykumar/Desktop/Work/featherflow/llama-tests/datasets/created_v2")
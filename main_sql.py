import os
from base.datasynthesis import DataSynth
from sql.generate import Dataset
from sql.generate_metadata import MetaData
from sql.generate_train import PrepareData
from sql.data_quality import CheckQuery
from dotenv import load_dotenv

class DataSynthSQL(DataSynth):
    def __init__(
            self,
            sql_folder: str,
            metadata_folder: str,
            databases: str,
            model: str="gpt-3.5-turbo-1106",
            dialect: str="Postgres") -> None:
        load_dotenv(override=True)
        self.sql_folder = sql_folder
        self.metadata_folder = metadata_folder
        self.databases = databases
        self.model = model
        self.dialect = dialect

    def compile(self):
        # Generating Databases
        dataset = Dataset(folder=self.sql_folder)
        dataset.create(self.databases)

        # Create Metadata
        metadata = MetaData(
            model=self.model, 
            sql_folder=self.sql_folder, 
            metadata_folder=self.metadata_folder
        )
        metadata.tables()
        metadata.columns()
        metadata.samples()
        metadata.joins()
        metadata.questions()
        metadata.clean_metadata()
        metadata.datatypes()

    def process(self):
        # Generating Training Data
        try:
            files = os.listdir(self.metadata_folder+"/questions")
            if len(files)==0:
                raise Exception("No Files found")
        except:
            raise Exception("Check the folder - "+self.metadata_folder+"/questions")
        
        prep_data = PrepareData(files=files, dialect=self.dialect, master_folder=self.metadata_folder)
        prep_data.get_train_data_v2()
        prep_data.get_training_datasets()

    def clean(self):
        # Checking the generated queries
        question_folder = os.path.join(self.metadata_folder, "questions")
        check_query = CheckQuery(self.sql_folder, question_folder)
        check_query.setup_databases()
        check_query.check_questions()

if __name__=="__main__":
    databases, sql_folder, metadata_folder = 1, "sql/created_test", "sql/metadata_test"
    try:
        data_synthesis = DataSynthSQL(sql_folder, metadata_folder, databases)
        data_synthesis.compile()
        data_synthesis.process()
        data_synthesis.check()
    except:
        import shutil
        shutil.rmtree("sql/created_test")
        shutil.rmtree("sql/metadata_test")
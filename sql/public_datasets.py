import os
import json

class ProcessDataset():
    def __init__(self, dataset_name: str, folder: str):
        self.dataset_name = dataset_name
        self.dataset_folder = folder

        if self.dataset_name == "spider":
            self.process = self.get_spider
        elif self.dataset_name == "wikisql":
            self.process = self.get_wiki
        elif self.dataset_name == "bird":
            self.process = self.get_bird
        else:
            self.process = self.not_implemented

    def not_implemented(self):
        raise Exception("{0} public dataset is not implemented".format(self.dataset_name))

    def get_spider(self, mode='train'):
        if mode == 'train':
            primary_file = os.path.join(self.dataset_folder, 'train_spider.json')
            other_file = os.path.join(self.dataset_folder, 'train_others.json')

            with open(primary_file, "r") as file:
                primary_questions = json.load(file)

            with open(other_file, "r") as file:
                other_questions = json.load(file)

            combined_questions = primary_questions+other_questions
        
        elif mode == 'dev':
            dev_file = os.path.join(self.dataset_folder, 'dev.json')

            with open(dev_file, "r") as file:
                dev_questions = json.load(file)

            combined_questions = dev_questions
        
        else:
            raise Exception("Undefined Mode {0}".format(mode))

        self.dataset = {}

        for instance in combined_questions:
            if instance['db_id'] not in self.dataset.keys():
                self.dataset[instance['db_id']] = []
            
            self.dataset[instance['db_id']].append({
                "question": instance['question'],
                "query": instance["query"]
            })

        with open(os.path.join('/Users/abhinaykumar/Desktop/Work/featherflow/llama-tests/datasets/public/spider', 'question.json'), "w") as file:
            json.dump(self.dataset, file)

    def get_wiki(self, mode='train'):
        
        agg_ops = ['', 'MAX', 'MIN', 'COUNT', 'SUM', 'AVG']
        cond_ops = ['=', '>', '<', 'OP']

        if mode == 'train':
            dataset_file = os.path.join(self.dataset_folder, 'train.jsonl')
            table_file = os.path.join(self.dataset_folder, 'train.tables.jsonl')
        elif mode == 'dev':
            dataset_file = os.path.join(self.dataset_folder, 'dev.jsonl')
            table_file = os.path.join(self.dataset_folder, 'dev.tables.jsonl')
        else:
            raise Exception("Undefined Mode {0}".format(mode))
        raw_dataset, tables = [], []
        with open(dataset_file, "r") as file:
            for line in file:
                data = json.loads(line.strip())
                raw_dataset.append(data)
        with open(table_file, "r") as file:
            for line in file:
                data = json.loads(line.strip())
                tables.append(data)

        tables_dict = {}

        for table in tables:
            try:
                tables_dict[table['id']] = {'header': table['header'], 'types': table['types'], 'rows': table['rows']}
            except KeyError:
                print("Table ID : ", table['id'])
                continue

        self.dataset = {}

        for instance in raw_dataset:

            if instance['table_id'] not in self.dataset.keys():
                self.dataset[instance['table_id']] = []
            
            table = tables_dict[instance['table_id']]
            question = instance['question']
            sql = instance['sql']

            query = ''

            sel = table['header'][sql['sel']]
            table_name = "table_"+instance['table_id'].replace("-", "_")
            conds = sql['conds']
            agg = agg_ops[sql['agg']]

            temp = sel
            if agg != '':
                temp = agg+'('+temp+')'
            
            query += "SELECT "+temp+" FROM "+table_name

            if len(conds)>0:
                query += " WHERE "
            
            for i, cond in enumerate(conds):
                temp_cond = ""
                if i>0:
                    temp_cond += " AND "
                temp_cond += table['header'][cond[0]]+" "+cond_ops[cond[1]]+" "

                if type(cond[2]) == str:
                    temp_cond += "\'"+cond[2]+"\'"
                else:
                    temp_cond += str(cond[2])

                query += temp_cond

            self.dataset[instance['table_id']].append({
                "question": question,
                "query": query
            })

        with open(os.path.join('/Users/abhinaykumar/Desktop/Work/featherflow/llama-tests/datasets/public/wikisql', 'question.json'), "w") as file:
            json.dump(self.dataset, file)

    def get_bird(self):
        pass


        
if __name__ == "__main__":
    process_dataset = ProcessDataset("wikisql", "/Users/abhinaykumar/Desktop/Work/featherflow/llama-tests/wikisql")
    process_dataset.process()
import yaml

class Glan:
    def __init__(self, prompt_file="prompts/glan.yaml") -> None:
        self.prompt_file = prompt_file

        with open(prompt_file, 'r') as file:
            self.prompt_data = yaml.safe_load(file)
        
        self.prompt_taxonomy = self.prompt_data.taxonomy
        self.prompt_subject = self.prompt_data.subject
        self.prompt_syllabus = self.prompt_data.syllabus
        self.prompt_instruction = self.prompt_data.instruction

    def create_taxonomy(self, field):
        pass

    def generate_subject(self):
        pass

    def generate_syllabus(self):
        pass

    def generate_instruction(self):
        pass

    def generate_data(self, field="human knowledge"):
        self.create_taxonomy(field=field)
        self.generate_subject()
        self.generate_syllabus()
        self.generate_instruction()


if __name__ == "__main__":
    glan = Glan()

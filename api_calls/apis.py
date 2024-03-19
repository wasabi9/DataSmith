import os
from openai import OpenAI
from abc import ABC, abstractmethod

class CallAPI(ABC):

    @abstractmethod
    def reset(self):
        pass

    @abstractmethod
    def generate(self):
        pass


class CallOpenAI(CallAPI):
    def __init__(self, system_prompt: str, model: str="gpt-4-turbo-preview") -> None:
        self.system_prompt = system_prompt
        self.model = model
        self.post_init()
    
    def post_init(self) -> None:
        self.thread = []
        self.thread.append({
            "role": "system",
            "content": self.system_prompt,
        })

    def reset(self) -> None:
        self.post_init()
        print(self.thread)

    def generate(self, user_prompt: str) -> str:
        
        self.thread.append({
            "role": "user",
            "content": user_prompt
        })

        client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"),)
        result = client.chat.completions.create(
            messages=self.thread,
            model=self.model,
            ) 
        
        self.thread.append(result.choices[0].message)

        print(result.choices[0].message)

        return result.choices[0].message.content
    

# TODO: Add Claude Support as well
    
    
if __name__ == "__main__":
    openai_agent = CallOpenAI(system_prompt="You are a helpful chat assistant")

    openai_agent.generate(user_prompt="what is the meaning of love, life and everything?")

    openai_agent.generate(user_prompt="Who gave this answer first?")

    openai_agent.reset()

from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from ollama import ChatResponse, chat




# Define the DAG 
@dag(
    start_date=datetime(2025, 1, 1),  # The start date for the DAG,
    schedule=None,
    catchup=False,

)
def  answer_question():
    # Define the start task using the @task decorator
    @task
    def start():
        pass  # Placeholder for the start task logic
        return True
    
    # Define the end task using the @task decorator
    @task
    def end(*args):
        pass  # Placeholder for the end task logic
        return True

    # Define the task to wait
    @task
    def wait_for_question_file(*args):
        wait_for_file = FileSensor(
            task_id="wait_for_question_file",  # Task ID
            filepath="/tmp/question.txt",  # Path to the file to wait for
            poke_interval=10,  # Interval in seconds to check for the file
            timeout=600,  # Timeout in seconds before failing the task
        )
        return True

    # the task to question
    @task
    def ask_and_receive(*args):
        with open("/tmp/question.txt", "r") as file:
           for line in file.readlines():
                pregunta = line.strip()
                response: ChatResponse = chat(
                   model="llama3.2:1b",
                   messages=[
                      {
                         "role": "user",
                         "content": pregunta,
                      },
                   ],
                ) 
                print ("Riddle me this:")
                print (pregunta)
                print(response.message.content)      
        
        return True
    
    start_me = start()
    read_me = wait_for_question_file(start_me)
    pay_up = ask_and_receive(read_me)
    end(pay_up)

answer_question()

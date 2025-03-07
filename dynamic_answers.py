import os
from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from ollama import ChatResponse, chat

def ask_question(pregunta):
    response: ChatResponse = chat(
        model="llama3.2:1b",
        messages=[
            {
                "role": "user",
                "content": pregunta,
            },
        ],
    )
    return (idx, pregunta, response.message.content)

# Define the DAG 
@dag(
    start_date=datetime(2025, 1, 1),  # The start date for the DAG,
    schedule=None,
    catchup=False,
)
def dynamic_answers():
    # Define the start task using the @task decorator
    @task
    def start():
        # Remove existing question and answer Markdown files in /tmp directory
        for file_name in os.listdir("/tmp"):
            if file_name.startswith("questions_and_answers_") and file_name.endswith(".md"):
                os.remove(os.path.join("/tmp", file_name))
                print ("removed ", file_name)
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

    start_me = start()
    read_me = wait_for_question_file(start_me)

    @task
    def read_questions(*args):
        questions_and_numbers = []
        with open("/tmp/questions.txt", "r") as file:
            for idx, line in enumerate(file.readlines(), start=1):
                pregunta = line.strip()
                questions_and_numbers.append((idx, pregunta))
                print("question number: ", idx)
                print("Question: ", pregunta)
        return questions_and_numbers

    questions_and_numbers = read_questions(read_me)

    # Generate PythonOperator tasks for each question
    question_tasks = []
    for idx in questions_and_numbers:
        task_id = f"ask_question_{idx}"
        question_task = PythonOperator(
            task_id=task_id,
            python_callable=ask_question,
            op_args=questions_and_numbers[idx],
        )
        question_tasks.append(question_task)

    @task
    def write_questions_and_answers(questions_and_answers):
        for idx, pregunta, response in questions_and_answers:
            with open(f"/tmp/questions_and_answers_{idx}.md", "w") as file:
                file.write(f"# Question {idx}\n\n")
                file.write(f"**Question:** {pregunta}\n\n")
                file.write(f"**Answer:** {response}\n")
        return True
    
    write_me = write_questions_and_answers(question_tasks)
    end(write_me)

dynamic_answers()

from pipeline.PipelineController import run
from tasks.TasksController import create_tasks


def main(request) -> dict:
    data = request.get_json()
    print(data)

    if "table" in data:
        response = run(["table"])
    elif "task" in data:
        response = create_tasks()
    else:
        raise ValueError(data)

    print(response)
    return response

import time
from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner


# Each task sleeps to simulate work, then returns its input so tasks can be chained
@task(log_prints=True)
def task_one(number):
  time.sleep(2)
  print(f"Task 1 complete for Item {number}")
  return number


@task(log_prints=True)
def task_two(number):
  time.sleep(2)
  print(f"Task 2 complete for Item {number}")
  return number


@flow(log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=10), name="a_simple_flow")
def etl(numbers):
  print("Flow started ...")

  # SERIAL: each number runs through task_one then task_two, one at a time (~60s for 10 items)
  for number in numbers:
    result = task_one(number)
    task_two(result)

  # PARALLEL (mapped): .map() submits one task run per item to the task runner, which
  # executes them concurrently. Passing task_one's futures into task_two.map() makes each
  # task_two wait only on its own upstream task_one. (~6s for 10 items)
  # firsts = task_one.map(numbers)
  # seconds = task_two.map(firsts)
  # seconds.wait()

  print("Flow complete.")


if __name__ == "__main__":
  numbers = range(1, 6)
  etl(numbers)

from aiida_workgraph import WorkGraph, task
from aiida import load_profile, orm

load_profile()

@task()
def prepare_input(n, m):
    """I want to compute a 12x12 table.
    I will return therefore the following list of tuples: [(1, 1), (1, 2), ..., (12, 12)].
    """
    inputs = []
    for left_operand in range(0, n):
        for right_operand in range(0, m):
            inputs.append([left_operand, right_operand])
    print("inputs", inputs)
    return inputs

@task()
def compare(index, inputs):
    return index < len(inputs)

@task()
def update_index(index):
    return index + 1

@task.awaitable_builder()
def submit_job(index, inputs, code_label):
    """
    This function is responsible for submitting a task to the scheduler.
    """
    from aiida.calculations.arithmetic.add import ArithmeticAddCalculation
    from aiida_workgraph.engine.utils import submit
    from random import randint

    builder = ArithmeticAddCalculation.get_builder()
    builder.code = orm.load_code(code_label)
    builder.x = orm.Int(inputs[index][0])
    builder.y = orm.Int(inputs[index][1])
    builder.metadata.options.sleep = randint(1, 10)
    node = submit(builder)
    return {f"index_{index}": node}


wg = WorkGraph("test_submission_controller")
wg.context = {"index": 0,
              "should_run": True}
prepare_input1 = wg.add_task(prepare_input, name="prepare_input", n=8, m=8)
while1 = wg.add_task("While", name="While", conditions=["should_run"])
submit_job1 = wg.add_task(submit_job, name="submit_job", code_label="add@localhost",
                        inputs=prepare_input1.outputs["result"],
                        index="{{index}}")
update_index1 = wg.add_task(update_index, name="update_index", index="{{index}}")
update_index1.set_context({"result": "index"})
update_index1.waiting_on.add(submit_job1)
compare1 = wg.add_task(compare, name="compare", inputs=prepare_input1.outputs["result"],
                       index = update_index1.outputs["result"])
compare1.set_context({"result": "should_run"})
while1.children.add([submit_job1, update_index1, compare1])
wg.max_number_jobs = 30

# wg.run()
wg.submit()



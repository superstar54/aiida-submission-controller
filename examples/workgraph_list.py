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

@task(outputs=[{"name": "should_run"}, {"name": "index"}])
def find_next(index, inputs):
    index += 1
    should_run = index < len(inputs)
    return {"should_run": should_run, "index": index}


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
wg.context = {"index": -1}
prepare_input1 = wg.add_task(prepare_input, name="prepare_input", n=8, m=8)
find_next1 = wg.add_task(find_next, name="find_next", index="{{index}}",
                            inputs=prepare_input1.outputs["result"])
find_next1.set_context({"index": "index"})
while1 = wg.add_task("While", name="While", conditions=find_next1.outputs["should_run"])
submit_job1 = wg.add_task(submit_job, name="submit_job", code_label="add@localhost",
                        inputs=prepare_input1.outputs["result"],
                        index=find_next1.outputs["index"])
while1.children.add([submit_job1])
wg.max_number_jobs = 20
# wg.run()
wg.submit()



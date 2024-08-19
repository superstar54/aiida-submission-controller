from aiida_workgraph import WorkGraph, task
from aiida import load_profile, orm
from ase.build import bulk

load_profile()

@task(outputs=[{"name": "to_submit_group"}, {"name": "submitted_group"}])
def prepare_input(cell_sizes, to_submit_group_label, submitted_group_label):
    """A function that creates a group of structures to submit.
    """
    to_submit_group, created = orm.Group.collection.get_or_create(to_submit_group_label)
    submitted_group, _ = orm.Group.collection.get_or_create(submitted_group_label)
    if created:
        for cell_size in cell_sizes:
            structure = orm.StructureData(ase=bulk("Al", a=cell_size, cubic=True))
            structure.store()
            to_submit_group.add_nodes(structure)
    return {"to_submit_group": to_submit_group, "submitted_group": submitted_group}

@task(outputs=[{"name": "should_run"}, {"name": "structure"}])
def find_next(to_submit_group, submitted_group):
    """A function that checks if there are any structures that need to be submitted.
    """
    #find the difference between the two groups
    pks = [node.pk for node in to_submit_group.nodes]
    pks2 = [node.pk for node in submitted_group.nodes]
    extras_to_run = list(set(pks).difference(pks2))

    if len(extras_to_run) == 0:
        return {"should_run": False, "structure": None}

    structure = orm.load_node(extras_to_run[0])
    
    return {"should_run": True, "structure": structure}

@task.awaitable_builder()
def submit_job(structure, code, protocol, submitted_group):
    """
    This function is responsible for submitting a task to the scheduler.
    """
    from aiida_quantumespresso.workflows.pw.base import PwBaseWorkChain
    from aiida_workgraph.engine.utils import submit

    builder = PwBaseWorkChain.get_builder_from_protocol(code,
            structure=structure,
            protocol=protocol)
    node = submit(builder)
    submitted_group.add_nodes(structure)
    return {f"structure_{structure.pk}": node}

pw_code = orm.load_code("qe-7.2-pw@localhost")

wg = WorkGraph("test_submission_controller")
prepare_input1 = wg.add_task(prepare_input, name="prepare_input",
                             to_submit_group_label="structures",
                             submitted_group_label="submitted_structures",
                             cell_sizes=(3.9, 4.0, 4.1, 4.2, 4.3, 4.4),
)
find_next1 = wg.add_task(find_next, name="find_next1", to_submit_group=prepare_input1.outputs["to_submit_group"],
                        submitted_group=prepare_input1.outputs["submitted_group"])
while1 = wg.add_task("While", name="While", conditions=find_next1.outputs["should_run"])
submit_job1 = wg.add_task(submit_job, name="submit_job", code=pw_code,
                        structure=find_next1.outputs["structure"],
                        protocol="fast",
                        submitted_group=prepare_input1.outputs["submitted_group"])
while1.children.add([submit_job1])
wg.max_number_jobs = 2

wg.submit()
# wg.run()
# wg.save()




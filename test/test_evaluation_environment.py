from openalea.workflow.evaluation_environment import EvaluationEnvironment


def test_env_created_with_proper_id():
    env = EvaluationEnvironment()
    assert env.current_execution() is not None

    env = EvaluationEnvironment(1)
    assert env.current_execution() == 1


def test_env_new_execution():
    env = EvaluationEnvironment()
    eid0 = env.current_execution()

    env.new_execution()
    assert env.current_execution() != eid0


def test_env_clear_free_ids():
    env = EvaluationEnvironment()
    eid0 = env.current_execution()
    env.new_execution()

    env.clear()
    assert env.current_execution() == eid0

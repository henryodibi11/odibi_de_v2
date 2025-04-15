import time
import pytest
from odibi_de_v2.utils.decorators.benchmark import benchmark


@benchmark(module="TEST", component="timing")
def simulated_task(seconds: float):
    time.sleep(seconds)
    return "done"


def test_benchmark_logs_duration():
        result = simulated_task(0.1)
        assert result == "done"

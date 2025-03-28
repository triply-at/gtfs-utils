import os
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def data_dir(request):
    file = request.module.__file__
    test_dir = Path(os.path.dirname(file))
    return test_dir / "data"


@pytest.fixture(
    scope="module", params=[pytest.param(True, marks=pytest.mark.slow()), False]
)
def lazy(request):
    return request.param

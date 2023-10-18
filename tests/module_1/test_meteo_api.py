""" This is a dummy example to show how to import code from src/ for testing"""


import unittest
import pandas as pd

from src.module_1.module_1_meteo_api import get_yearly_mean_std, VARIABLES

# flake8: noqa E501


def test_main():
    variable = VARIABLES.split(",")[-1]
    data_dict = {
        "time": ["1950-01-01", "1950-02-01", "1975-02-01", "1975-02-03"],
        f"{variable}_CMCC_CM2_VHR4": [10, 12, 10, 10],
    }

    variable_dict = {variable: "°C"}

    result = get_yearly_mean_std(data_dict, variable_dict)

    expected_result = {
        (variable, "mean"): {1950: 11.0, 1975: 10.0},
        (variable, "std"): {1950: 1.4142135623730951, 1975: 0.0},
    }

    assert result.to_dict() == expected_result


if __name__ == "__main__":
    test_main()

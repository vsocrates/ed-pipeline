#!/usr/bin/env python3

import colorama
from dags import *
from modules import demos
from pyspark import SparkConf
from src.ed_pipeline.qc import quality_checks
from src.ed_pipeline.utils import helper_variables, helpful_functions
import typer
from typer import Argument


def main(n: int = Argument(..., min=0, help="The input n of fact(n)")) -> None:
    """Compute factorial of a given input."""
    colorama.init(autoreset=True, strip=False)

    spark = helpful_functions.get_spark_session("test", SparkConf())


def entry_point() -> None:
    typer.run(main)


# Allow the script to be run standalone (useful during development).
if __name__ == "__main__":
    entry_point()

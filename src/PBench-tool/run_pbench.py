import argparse
import os
import yaml

from simulatedannealing import *
from replay_ta import *
from linearprogram_option import *
from random_send import *

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run PBench for all configs in a directory")
    parser.add_argument(
        "--config-dir",
        default="configs/snowset",
        help="Config directory to scan (relative to src/PBench-tool or absolute). Default: configs/snowset",
    )
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    directory = args.config_dir
    if not os.path.isabs(directory):
        directory = os.path.join(script_dir, directory)

    for root, dirs, files in os.walk(directory):
        dirs.sort()
        for file in files:
            if file.endswith('.yml'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                    print(f"processing {file}")
                    print(config)
                    ILP_work(config)
                    gen_ta(config)
                    replay_ta(config)

                    # gen_random(config)
                    # replay_random(config)

"""
This is a hacky POC and not a production-level solution, it should be removed or heavily improved
before going into develop/main
"""

import os
import sys

from cstar.orchestration.orchestration import build_and_run

if __name__ == "__main__":
    os.environ["CSTAR_INTERACTIVE"] = "0"
    os.environ["CSTAR_ACCOUNT_KEY"] = "ees250129"
    os.environ["CSTAR_QUEUE_NAME"] = "wholenode"
    os.environ["CSTAR_ORCHESTRATED"] = "1"

    # wp_path = "/Users/eilerman/git/C-Star/personal_testing/workplan_local.yaml"
    wp_path = "/home/x-seilerman/wp_testing/workplan.yaml"

    my_run_name = sys.argv[1]
    os.environ["CSTAR_RUNID"] = my_run_name

    build_and_run(wp_path)

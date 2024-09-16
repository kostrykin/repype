import sys

import repype.cli

if __name__ == '__main__':
    if repype.cli.run_cli():
        sys.exit(0)
    else:
        sys.exit(1)

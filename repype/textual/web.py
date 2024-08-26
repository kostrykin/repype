import os
import sys

from textual_serve.server import Server


args = ' '.join(sys.argv[1:])

server = Server(
    command = f'{sys.executable} -m repype.textual {args}',
)
server.serve(
    debug = bool(os.environ.get('REPYPE_TEXTUAL_DEBUG', False)),
)
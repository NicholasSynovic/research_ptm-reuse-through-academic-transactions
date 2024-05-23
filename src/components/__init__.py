from os.path import expanduser
from pathlib import Path

from pyfs import resolvePath

USER_HOME: Path = resolvePath(path=expanduser(path="~"))

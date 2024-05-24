from pathlib import Path
from tkinter import Tk, filedialog
from typing import Literal

from pyfs import resolvePath

from src.components import COMPONENT_TITLE, USER_HOME


def tk_FilePicker() -> Path | Literal[False]:
    root: Tk = Tk()
    root.withdraw()

    root.geometry("1920x1080")

    fp: str = filedialog.askopenfilename(
        initialdir=USER_HOME,
        title=COMPONENT_TITLE,
    )
    if fp:
        root.destroy()
        return resolvePath(path=Path(fp))
    else:
        root.destroy()
        return False


if __name__ == "__main__":
    tk_FilePicker()

import tkinter as tk
from pathlib import Path
from tkinter import filedialog
from typing import Literal

from pyfs import resolvePath


def tk_FilePicker() -> Path | Literal[False]:
    root = tk.Tk()
    root.withdraw()

    fp: str = filedialog.askopenfilename()
    if fp:
        root.destroy()
        return resolvePath(path=Path(fp))
    else:
        root.destroy()
        return False


if __name__ == "__main__":
    tk_FilePicker()

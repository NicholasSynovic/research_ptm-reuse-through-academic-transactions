import tkinter as tk
from tkinter import filedialog


def select_file():
    root = tk.Tk()
    root.withdraw()  # Hide the root window

    file_path = filedialog.askopenfilename()
    if file_path:
        print("Selected file path:", file_path)
    else:
        print("No file selected")

    root.destroy()


if __name__ == "__main__":
    select_file()

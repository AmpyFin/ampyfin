import os

EXCLUDE_DIRS = {".git"}  # Add more names here if needed

def print_directory_tree(start_path, indent=""):
    try:
        entries = sorted(os.listdir(start_path))
    except PermissionError:
        print(f"{indent}⛔ [Permission Denied] {start_path}")
        return

    for index, entry in enumerate(entries):
        if entry in EXCLUDE_DIRS:
            continue

        full_path = os.path.join(start_path, entry)
        is_last = index == len(entries) - 1
        connector = "└── " if is_last else "├── "
        print(indent + connector + entry)

        if os.path.isdir(full_path):
            new_indent = indent + ("    " if is_last else "│   ")
            print_directory_tree(full_path, new_indent)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Print directory structure.")
    parser.add_argument("path", nargs="?", default=".", help="Root directory path (default: current directory)")
    args = parser.parse_args()

    print(f"📁 Directory tree for: {os.path.abspath(args.path)}")
    print_directory_tree(args.path)

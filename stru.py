import os

# Folders to ignore
EXCLUDE_DIRS = {"logs", "config", ".git"}

def print_tree(start_path, prefix=""):
    try:
        items = sorted(os.listdir(start_path))
    except PermissionError:
        print(prefix + "└── [Permission Denied]")
        return

    # Filter out excluded directories
    items = [item for item in items if item not in EXCLUDE_DIRS]

    for index, item in enumerate(items):
        path = os.path.join(start_path, item)
        is_last = index == len(items) - 1

        connector = "└── " if is_last else "├── "
        print(prefix + connector + item)

        if os.path.isdir(path):
            extension = "    " if is_last else "│   "
            print_tree(path, prefix + extension)


if __name__ == "__main__":
    root_dir = input("Enter directory path (leave empty for current): ").strip()
    if not root_dir:
        root_dir = "."

    print(f"\nFile structure of: {os.path.abspath(root_dir)}\n")
    print_tree(root_dir)
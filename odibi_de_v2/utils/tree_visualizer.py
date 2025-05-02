import os
from typing import Optional


def print_tree(
    start_path: str,
    prefix: str = "",
    max_depth: Optional[int] = None,
    current_depth: int = 0,
    collect_lines: bool = False,
    output_file: Optional[str] = None,
    start_label: Optional[str] = None
) -> Optional[list[str]]:
    """
    Prints a tree structure of the given directory, with an optional maximum depth.
    Parameters:
        start_path (str): The path to the directory to start the tree from.
        prefix (str, optional)
            The prefix to use for each line in the tree. Defaults to an empty string.
        max_depth (int, optional)
            The maximum depth of the tree to print. Defaults to None.
            If None, the tree will be printed to the maximum depth.
        current_depth (int, optional)
            The current depth of the tree. Defaults to 0.
        collect_lines (bool, optional)
            Whether to collect the lines of the tree into a list. Defaults to False.
        output_file (str, optional)
            The file to write the lines of the tree to. Defaults to None.
            If None, the lines will be printed to the console.
        start_label (str, optional)
            The label to use for the starting directory. Defaults to None.
            If None, the starting directory will be labeled as "📁".
            If not None, the starting directory will be labeled with the given string.
            If the starting directory is a file, the label will be "📄".
    Returns:
        Optional[list[str]]: A list of strings representing the lines of the tree, if collect_lines is True.

    Example:
        print_tree(
            start_path="./odibi_de_v2",
            start_label="📁 odibi_de_v2/",
            collect_lines=True,
            output_file="../odibi_de_structure.txt"
        )

    """
    lines = []
    entries = sorted([f for f in os.listdir(start_path) if not f.startswith(".")])

    if start_label:
        header = f"{start_label}"
        print(header)
        if collect_lines:
            lines.append(header)

    for index, name in enumerate(entries):
        path = os.path.join(start_path, name)
        is_last = index == len(entries) - 1
        connector = "└── " if is_last else "├── "
        icon = "📁" if os.path.isdir(path) else "📄"
        line = f"{prefix}{connector}{icon} {name}"
        print(line)
        if collect_lines:
            lines.append(line)

        if os.path.isdir(path) and (max_depth is None or current_depth < max_depth):
            extension = "    " if is_last else "│   "
            child_lines = print_tree(
                path,
                prefix + extension,
                max_depth=max_depth,
                current_depth=current_depth + 1,
                collect_lines=collect_lines
            )
            if collect_lines and child_lines:
                lines.extend(child_lines)

    if collect_lines and output_file:
        with open(output_file, "w") as f:
            f.write("\n".join(lines))

    return lines if collect_lines else None
import os
import re

def generate_recursive_inits(top_folder: str, init_filename: str = "__init__.py"):
   """
   Recursively generate __init__.py files.

   - Leaf folders: import directly from .py files.
   - Non-leaf folders: import everything from child subpackages.
   """

   def scan_functions(file_path):
       """Return function/class names from a file."""
       with open(file_path, "r") as f:
           code = f.read()
       objects = re.findall(
           r"^\s*(?:async\s+)?def\s+([a-zA-Z_]\w*)|^\s*class\s+([a-zA-Z_]\w*)",
           code, flags=re.MULTILINE
       )
       return [name for tup in objects for name in tup if name]

   for root, dirs, files in os.walk(top_folder, topdown=False):
       py_files = [f for f in files if f.endswith(".py") and f != init_filename]

       init_lines = []
       all_exports = []

       # Case 1: Leaf folder (has .py files)
       for file in py_files:
           module_name = file[:-3]
           file_path = os.path.join(root, file)
           exports = scan_functions(file_path)
           if exports:
               import_line = f"from .{module_name} import {', '.join(exports)}"
               init_lines.append(import_line)
               all_exports.extend(exports)

       # Case 2: Non-leaf folder (has subfolders with __init__.py)
       for d in dirs:
           sub_init = os.path.join(root, d, init_filename)
           if os.path.exists(sub_init):
               # import all symbols from subpackage
               import_line = f"from .{d} import *"
               init_lines.append(import_line)
               # read its __all__ to extend our own
               with open(sub_init, "r") as f:
                   match = re.search(r"__all__\s*=\s*\[(.*?)\]", f.read(), re.S)
                   if match:
                       sub_exports = [s.strip("'\" ") for s in match.group(1).split(",") if s.strip()]
                       all_exports.extend(sub_exports)

       if not init_lines:
           continue

       # Build __all__
       all_line = "__all__ = [" + ", ".join(f"'{name}'" for name in all_exports) + "]"

       init_path = os.path.join(root, init_filename)
       with open(init_path, "w") as f:
           f.write("\n".join(init_lines))
           f.write("\n\n")
           f.write(all_line)

       rel_init = os.path.relpath(init_path, top_folder)
       print(f"Generated {rel_init} with {len(all_exports)} exports.")

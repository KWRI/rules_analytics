import os
import hashlib
import shutil
import ast
import time
import stat
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


class SkeletonMapper(ast.NodeTransformer):

    def visit_Name(self, node):
        return ast.copy_location(ast.Name(id='v', ctx=node.ctx), node)

    def visit_Constant(self, node):
        return ast.copy_location(ast.Constant(value='c'), node)

    def visit_FunctionDef(self, node):
        node.name = 'f'
        self.generic_visit(node)
        return node


def get_structural_hash(source):
    try:
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.ClassDef, ast.Module)):
                if (node.body and isinstance(node.body[0], ast.Expr) and
                        isinstance(node.body[0].value, ast.Constant)):
                    node.body.pop(0)

        transformed_tree = SkeletonMapper().visit(tree)
        clean_code = ast.unparse(transformed_tree)
        normalized = "".join(clean_code.split())
        return hashlib.md5(normalized.encode('utf-8')).hexdigest()
    except Exception:
        return None


def safely_delete_dir(dir_path):
    if not dir_path.exists():
        return

    def remove_readonly(func, path, excinfo):
        try:
            os.chmod(path, stat.S_IWRITE)
            func(path)
        except Exception:
            pass

    try:
        shutil.rmtree(dir_path, onerror=remove_readonly)
        time.sleep(0.3)
    except PermissionError:
        print(f"Directory {dir_path.name} locked by OS. Retrying in 2 seconds...")
        time.sleep(2)
        shutil.rmtree(dir_path, ignore_errors=True)


def run_audit():
    repo_path = os.getenv("REPO_PATH")
    if not repo_path:
        print("Error: REPO_PATH not found in ..env")
        return

    source_active = Path(repo_path) / "ui-rules" / "active"
    lab_dir = Path(repo_path) / "logic-comparison-lab"

    if not source_active.exists():
        print(f"Error: Active folder source path not found at {source_active}")
        return

    print("Cleaning up old lab directory...")
    safely_delete_dir(lab_dir)

    lab_dir.mkdir(parents=True, exist_ok=True)

    groups = {}
    total_scanned_active = 0
    print(f"Starting audit in {source_active}...")

    for file_path in source_active.iterdir():
        if file_path.is_file() and file_path.suffix == ".py":
            total_scanned_active += 1
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    fingerprint = get_structural_hash(content)

                    if fingerprint:
                        if fingerprint not in groups:
                            groups[fingerprint] = []
                        groups[fingerprint].append(file_path)
            except Exception as e:
                print(f"Error reading {file_path.name}: {e}")

    print(f"Total active rule files analyzed: {total_scanned_active}")

    match_count = 0
    for fingerprint, paths in groups.items():
        if len(paths) > 1:
            match_count += 1
            group_folder = lab_dir / f"review_group_{match_count}_{paths[0].stem}"
            group_folder.mkdir(parents=True, exist_ok=True)

            for p in paths:
                shutil.copy2(p, group_folder / p.name)

            print(f"Created group {match_count}: {len(paths)} structurally identical files.")

    print("Audit complete.")
    if match_count > 0:
        print(f"Logic clusters found: {match_count}")
        print(f"Location: {lab_dir}")
    else:
        print("No structural duplicates found.")


if __name__ == "__main__":
    run_audit()

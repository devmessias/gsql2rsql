#!/usr/bin/env python3
"""Generate all golden SQL files for transpile tests.

This script imports each test class and uses its exact schema and query
to generate the expected SQL files.
"""

import importlib.util
import re
import sys
from pathlib import Path
from types import ModuleType

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))


def extract_test_info(
    test_file: Path,
) -> tuple[str, str, str | None] | None:
    """Extract TEST_ID, TEST_NAME, and class name from a test file."""
    content = test_file.read_text()

    # Match TEST_ID = "XX"
    id_match = re.search(r'TEST_ID\s*=\s*["\'](\d+)["\']', content)
    # Match TEST_NAME = "name"
    name_match = re.search(r'TEST_NAME\s*=\s*["\']([a-z_]+)["\']', content)
    # Match class TestXxx:
    class_match = re.search(r'class\s+(Test\w+)\s*[:(]', content)

    if not (id_match and name_match):
        return None

    class_name = class_match.group(1) if class_match else None
    return id_match.group(1), name_match.group(1), class_name


def import_test_module(test_file: Path) -> ModuleType | None:
    """Import a test module dynamically."""
    try:
        spec = importlib.util.spec_from_file_location(
            test_file.stem, test_file
        )
        if spec is None or spec.loader is None:
            return None

        module = importlib.util.module_from_spec(spec)
        sys.modules[test_file.stem] = module
        spec.loader.exec_module(module)
        return module
    except (ImportError, AttributeError, ValueError) as e:
        print(f"Error importing {test_file}: {e}")
        return None


def extract_golden_query(
    test_module: ModuleType, class_name: str
) -> str | None:
    """Extract Cypher query from test_golden_file_match method."""
    try:
        test_class = getattr(test_module, class_name)

        # Read the test file source to extract the query
        import inspect

        source = inspect.getsource(test_class.test_golden_file_match)

        # Extract Cypher query from source
        # Look for patterns: cypher = """...""" or cypher = "..."
        pattern = (
            r'cypher\s*=\s*(?:'
            r'"""(.*?)"""|'
            r"'''(.*?)'''|"
            r'"([^"]+)|'
            r"'([^']+))"
        )
        cypher_match = re.search(pattern, source, re.DOTALL)

        if not cypher_match:
            return None

        # Get the first non-None group
        query = next(
            (g for g in cypher_match.groups() if g is not None), None
        )
        if query:
            return query.strip()

        return None
    except (AttributeError, TypeError, OSError) as e:
        print(f"Error extracting query: {e}")
        return None


def generate_golden_file(
    test_file: Path,
    test_id: str,
    test_name: str,
    class_name: str | None,
) -> bool:
    """Generate golden file by importing and running the test."""
    print(f"Generating {test_id}_{test_name}.sql...", end=" ", flush=True)

    if not class_name:
        print("⚠️  No class name found")
        return False

    try:
        # Import the test module
        module = import_test_module(test_file)
        if module is None:
            print("❌ Failed to import module")
            return False

        # Get the test class
        test_class = getattr(module, class_name, None)
        if test_class is None:
            print(f"❌ Class {class_name} not found")
            return False

        # Create instance and run setup
        instance = test_class()
        instance.setup_method()

        # Extract the query
        query = extract_golden_query(module, class_name)
        if query is None:
            print("⚠️  Could not extract query")
            return False

        # Transpile using the test's own method
        sql = instance._transpile(query)

        # Save the golden file
        from tests.utils.sql_test_utils import (
            EXPECTED_DIR,
            ensure_output_dirs,
            normalize_sql,
        )

        ensure_output_dirs()

        expected_path = EXPECTED_DIR / f"{test_id}_{test_name}.sql"
        expected_path.write_text(normalize_sql(sql))

        print("✅")
        return True

    except (AttributeError, TypeError, ValueError, OSError) as e:
        print(f"❌ Error: {str(e)[:50]}")
        return False


def main() -> int:
    """Main entry point."""
    tests_dir = Path("tests/transpile_tests")

    if not tests_dir.exists():
        print(f"Error: {tests_dir} not found", file=sys.stderr)
        return 1

    # Find all test files
    test_files = sorted(tests_dir.glob("test_*.py"))

    if not test_files:
        print(f"Error: No test files found in {tests_dir}", file=sys.stderr)
        return 1

    print(f"Found {len(test_files)} test files")
    print("-" * 60)

    success_count = 0
    failed_count = 0
    skipped_count = 0

    for test_file in test_files:
        test_info = extract_test_info(test_file)

        if test_info is None:
            msg = f"⚠️  Skipping {test_file.name}: No TEST_ID/TEST_NAME"
            print(msg)
            skipped_count += 1
            continue

        test_id, test_name, class_name = test_info

        if generate_golden_file(test_file, test_id, test_name, class_name):
            success_count += 1
        else:
            failed_count += 1

    print("-" * 60)
    print(f"✅ Success: {success_count}")
    print(f"❌ Failed: {failed_count}")
    print(f"⚠️  Skipped: {skipped_count}")

    # Return success if at least 25% of tests passed
    # Lenient since some tests may be for unimplemented features
    total_attempted = success_count + failed_count
    if total_attempted == 0:
        return 1

    success_rate = success_count / total_attempted
    if success_rate >= 0.25:  # At least 25% success
        msg = f"\n✅ Golden files generated ({success_rate:.1%})"
        print(msg)
        return 0

    msg = f"\n❌ Too many failures ({success_rate:.1%})"
    print(msg)
    return 1


if __name__ == "__main__":
    sys.exit(main())

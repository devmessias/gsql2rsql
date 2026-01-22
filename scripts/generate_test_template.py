#!/usr/bin/env python3
"""
Script to generate test templates for OpenCypher transpiler tests.

Usage:
    python scripts/generate_test_template.py 04 "Pagination with SKIP and LIMIT"
"""

import sys
from pathlib import Path


def generate_spec_template(test_id: str, test_name: str) -> str:
    """Generate markdown specification template."""
    return f"""# {test_id} – {test_name}

## Cypher

```cypher
-- TODO: Add OpenCypher query here
MATCH (n:Node)
RETURN n
```

## What this query does

TODO: Explain in plain English what this query does:
- What nodes/relationships are matched
- What filters/conditions are applied
- What is returned
- Expected cardinality (one row, many rows, etc.)

## Expected Databricks SQL

```sql
-- TODO: Add expected SQL output
SELECT
  n.id
FROM `schema`.`Node` AS n
```

## Notes

- **Recursion required**: No/Yes
- **Ordering semantics**: (describe ordering behavior)
- **Null handling**: (describe how NULLs are handled)
- **Ambiguity resolved**: (explain any design decisions)
- **Known limitations**: (document any workarounds needed)
"""


def generate_test_template(test_id: str, test_name: str) -> str:
    """Generate pytest test template."""
    class_name = "".join(word.capitalize() for word in test_name.split())
    class_name = class_name.replace(" ", "").replace("-", "")

    return f'''"""Test {test_id}: {test_name}."""

from gsql2rsql import OpenCypherParser, LogicalPlan, SQLRenderer
from gsql2rsql.common.schema import NodeSchema, EdgeSchema, EntityProperty
from gsql2rsql.renderer.schema_provider import SimpleSQLSchemaProvider, SQLTableDescriptor


class Test{class_name}:
    """Test {test_name.lower()}."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        # Schema (SimpleSQLSchemaProvider)
        self.schema = SimpleSQLSchemaProvider()

        # TODO: Add nodes
        self.schema.add_node(
            NodeSchema(
                name="Person",
                properties=[
                    EntityProperty("id", int),
                    EntityProperty("name", str),
                ],
                node_id_property=EntityProperty("id", int),
            ),
            SQLTableDescriptor(
                table_name="dbo.Person",
                node_id_columns=["id"],
            ),
        )

        # TODO: Add edges if needed
        # self.schema.add_edge(
        #     EdgeSchema(
        #         name="KNOWS",
        #         source_node_id="Person",
        #         sink_node_id="Person",
        #     ),
        #     SQLTableDescriptor(
        #         entity_id="Person@KNOWS@Person",
        #         table_name="dbo.Knows",
        #     ),
        # )

    def test_{test_name.lower().replace(" ", "_").replace("-", "_")}(
        self,
    ) -> None:
        """Test TODO: describe what this tests."""
        # TODO: Replace with actual Cypher query
        cypher = "MATCH (p:Person) RETURN p"

        parser = OpenCypherParser()
        ast = parser.parse(cypher)
        plan = LogicalPlan.process_query_tree(ast, self.schema)
        renderer = SQLRenderer(db_schema_provider=self.schema)
        sql = renderer.render_plan(plan)

        # TODO: Add semantic validations
        # Validate structure, not exact string matching

        # Should have SELECT
        assert "SELECT" in sql

        # Should reference expected tables
        # assert "Person" in sql

        # Should have/not have specific clauses
        # assert "WHERE" in sql.upper()
        # assert "JOIN" not in sql.upper()
        # assert "ORDER BY" in sql
        # etc.
'''


def main() -> None:
    """Generate test templates."""
    if len(sys.argv) != 3:
        print("Usage: python scripts/generate_test_template.py <id> <name>")
        print('Example: python scripts/generate_test_template.py 04 '
              '"Pagination with SKIP and LIMIT"')
        sys.exit(1)

    test_id = sys.argv[1]
    test_name = sys.argv[2]

    # Generate file names
    safe_name = test_name.lower().replace(" ", "_").replace("-", "_")
    spec_file = Path(f"tests/docs/{test_id}_{safe_name}.md")
    test_file = Path(f"tests/transpile_tests/test_{test_id}_{safe_name}.py")

    # Generate spec
    spec_content = generate_spec_template(test_id, test_name)
    spec_file.write_text(spec_content)
    print(f"✅ Created spec: {spec_file}")

    # Generate test
    test_content = generate_test_template(test_id, test_name)
    test_file.write_text(test_content)
    print(f"✅ Created test: {test_file}")

    print(f"\nNext steps:")
    print(f"1. Edit {spec_file} - Add Cypher query and expected SQL")
    print(f"2. Edit {test_file} - Implement test assertions")
    print(f"3. Run: uv run pytest {test_file} -v")
    print(f"4. Fix any failing tests by updating the transpiler")


if __name__ == "__main__":
    main()

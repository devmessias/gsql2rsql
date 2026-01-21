## Inspiration and Design Differences

gsql2rsql was inspired by the [Microsoft openCypherTranspiler](https://github.com/microsoft/openCypherTranspiler), a C# project for transpiling OpenCypher to T-SQL (now discontinued). While the core idea is similar—translating Cypher graph queries to SQL—gsql2rsql introduces several key architectural differences:

- **Stricter Phase Separation:**
  gsql2rsql enforces a much stronger separation between the phases of the transpiler pipeline (Parser, Planner, Resolver, Renderer). Each phase has a single responsibility, and the renderer is intentionally kept as "dumb" as possible, only emitting SQL from fully-resolved logical plans. This separation makes the codebase easier to maintain, test, and extend.

- **Human-Friendly Debugging:**
  The architecture is designed for transparency and developer experience. For example, error messages during development are rich and actionable, showing available variables, suggestions, and hints. See the example below:

    ```
    Makefile:55: warning: ignoring old recipe for target 'test-pyspark-quick'
    Testing recursive query transpilation...
    ╔══════════════════════════════════════════════════════════════════════════════╗
    ║ ColumnResolutionError: Variable 'rels' is not defined                        ║
    ╚══════════════════════════════════════════════════════════════════════════════╝

    ━━━ Query ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        1 │ MATCH path = (root:Vertex)-[rels:REL*1..5]-(n:Vertex) WHERE root.node_id = '1234_algo' AND n.node_type = 'node_type' AND NONE(r IN rels WHERE r.relationship_type IN ['a', 'b']) RETURN rels AS edges, n AS vertex_info
          │                             ▲
          │                             └── ERROR: Variable 'rels' is not defined
        2 │

    ━━━ Available Variables (Scope Level 0) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

      Name         Type      Data Type   Defined At              Properties
      ───────────────────────────────────────────────────────────────────────────
      root         entity   Vertex       MATCH (root:Vertex)      node_type, metadata, node_id
      path         path     PATH         MATCH path = ...         -
      n            entity   Vertex       MATCH (n:Vertex)         node_type, metadata, node_id
      edges        value    unknown      RETURN/WITH AS edges     -
      vertex_info  value    Vertex       RETURN/WITH AS vertex_info -

    ━━━ Suggestions ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

      • Did you mean 'root'? (3 characters difference)

    ━━━ Hints ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

      💡 Make sure 'rels' is defined in a MATCH clause before use.
         Variables must be defined before they can be referenced in WHERE, WITH, or RETURN clauses.

    ━━━ Debug Information ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

      Operator:         ProjectionOperator (id=6)
      Resolution Phase: expression_resolution
      Symbol Table:
        Symbol Table Dump:
          Scope 0 (global):
            root: entity(Vertex) @ scope 0
            path: path(PATH) @ scope 0
            n: entity(Vertex) @ scope 0
            edges: value(unknown) @ scope 0
            vertex_info: value(Vertex) @ scope 0
    ```

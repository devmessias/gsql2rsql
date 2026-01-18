"""Sample data generator for PySpark testing.

This module generates realistic sample data from YAML schema definitions,
creating PySpark DataFrames that can be used to test transpiled SQL queries.
"""

from __future__ import annotations

import random
import string
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


# Sample data pools for generating realistic values
FIRST_NAMES = [
    "Alice",
    "Bob",
    "Charlie",
    "Diana",
    "Eve",
    "Frank",
    "Grace",
    "Henry",
    "Ivy",
    "Jack",
    "Kate",
    "Leo",
    "Mia",
    "Noah",
    "Olivia",
    "Peter",
]

LAST_NAMES = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Anderson",
    "Taylor",
    "Thomas",
    "Moore",
]

CITIES = [
    "New York",
    "Los Angeles",
    "Chicago",
    "Houston",
    "Phoenix",
    "Philadelphia",
    "San Antonio",
    "San Diego",
    "Dallas",
    "San Jose",
    "Austin",
    "Seattle",
    "Denver",
    "Boston",
    "Portland",
    "Miami",
]

COUNTRIES = ["USA", "UK", "Canada", "Germany", "France", "Japan", "Brazil", "Australia"]

COMPANY_NAMES = [
    "TechCorp",
    "DataSystems",
    "CloudInc",
    "AI Solutions",
    "FinanceHub",
    "MediaGroup",
    "RetailMax",
    "HealthPlus",
    "EnergyFirst",
    "TransGlobal",
]

INDUSTRIES = [
    "Technology",
    "Finance",
    "Healthcare",
    "Retail",
    "Manufacturing",
    "Energy",
    "Media",
    "Transportation",
    "Education",
    "Real Estate",
]

MOVIE_TITLES = [
    "The Matrix",
    "Inception",
    "Interstellar",
    "The Dark Knight",
    "Pulp Fiction",
    "Fight Club",
    "Forrest Gump",
    "The Godfather",
    "Gladiator",
    "Avatar",
]

GENRES = ["Action", "Drama", "Comedy", "Sci-Fi", "Thriller", "Romance", "Horror"]

MERCHANT_CATEGORIES = ["Retail", "Restaurant", "Gas Station", "Online", "Travel", "Entertainment"]

STATUSES = ["active", "inactive", "suspended", "pending", "verified"]

KYC_STATUSES = ["verified", "pending", "rejected", "in_review"]


def random_string(length: int = 8) -> str:
    """Generate a random alphanumeric string."""
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def random_name() -> str:
    """Generate a random full name."""
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def random_date(start_year: int = 2020, end_year: int = 2024) -> str:
    """Generate a random date string."""
    year = random.randint(start_year, end_year)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return f"{year}-{month:02d}-{day:02d}"


def random_timestamp(start_year: int = 2023, end_year: int = 2024) -> str:
    """Generate a random timestamp string."""
    date = random_date(start_year, end_year)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return f"{date} {hour:02d}:{minute:02d}:{second:02d}"


@dataclass
class PropertyGenerator:
    """Generator configuration for a specific property."""

    name: str
    data_type: str
    generator: Any = None  # Callable or list of choices

    def generate(self) -> Any:
        """Generate a value for this property."""
        if self.generator is not None:
            if callable(self.generator):
                return self.generator()
            elif isinstance(self.generator, list):
                return random.choice(self.generator)
            return self.generator

        # Default generators based on type and name hints
        name_lower = self.name.lower()

        if self.data_type in ("int", "integer", "long"):
            if "age" in name_lower:
                return random.randint(18, 80)
            elif "score" in name_lower:
                return random.randint(0, 100)
            elif "year" in name_lower:
                return random.randint(1990, 2024)
            elif "days" in name_lower:
                return random.randint(1, 365)
            elif "population" in name_lower:
                return random.randint(10000, 10000000)
            elif "since" in name_lower:
                return random.randint(2000, 2024)
            return random.randint(1, 1000)

        elif self.data_type in ("float", "double"):
            if "amount" in name_lower or "salary" in name_lower or "price" in name_lower:
                return round(random.uniform(10.0, 10000.0), 2)
            elif "rating" in name_lower or "strength" in name_lower:
                return round(random.uniform(0.0, 5.0), 2)
            return round(random.uniform(0.0, 100.0), 2)

        elif self.data_type in ("bool", "boolean"):
            return random.choice([True, False])

        else:  # string
            if "name" in name_lower and "holder" not in name_lower or "holder_name" in name_lower:
                return random_name()
            elif "city" in name_lower:
                return random.choice(CITIES)
            elif "country" in name_lower:
                return random.choice(COUNTRIES)
            elif "category" in name_lower:
                return random.choice(MERCHANT_CATEGORIES)
            elif "industry" in name_lower:
                return random.choice(INDUSTRIES)
            elif "title" in name_lower:
                return random.choice(MOVIE_TITLES)
            elif "genre" in name_lower:
                return random.choice(GENRES)
            elif "status" in name_lower:
                return random.choice(STATUSES)
            elif "kyc" in name_lower:
                return random.choice(KYC_STATUSES)
            elif "date" in name_lower:
                return random_date()
            elif "timestamp" in name_lower or "time" in name_lower:
                return random_timestamp()
            elif "street" in name_lower:
                return f"{random.randint(1, 999)} {random.choice(LAST_NAMES)} Street"
            elif "location" in name_lower:
                return random.choice(CITIES)
            elif "role" in name_lower or "position" in name_lower:
                return random.choice(["Manager", "Engineer", "Director", "Analyst", "Lead"])
            elif "number" in name_lower:
                return "".join(random.choices(string.digits, k=16))
            elif "code" in name_lower:
                return "".join(random.choices(string.ascii_uppercase, k=2))
            elif "type" in name_lower:
                return random.choice(["deposit", "withdrawal", "transfer", "payment"])
            elif "nickname" in name_lower:
                return random.choice(FIRST_NAMES).lower() + str(random.randint(1, 99))
            return random_string()


@dataclass
class TableDataGenerator:
    """Generator for a complete table's data."""

    table_name: str
    short_name: str  # Name without catalog.schema prefix
    id_property: str
    id_type: str
    properties: list[PropertyGenerator] = field(default_factory=list)
    _current_id: int = 0

    def generate_row(self) -> dict[str, Any]:
        """Generate a single row of data."""
        self._current_id += 1
        row = {self.id_property: self._current_id}
        for prop in self.properties:
            row[prop.name] = prop.generate()
        return row

    def generate_rows(self, count: int) -> list[dict[str, Any]]:
        """Generate multiple rows of data."""
        return [self.generate_row() for _ in range(count)]

    def reset(self) -> None:
        """Reset the ID counter."""
        self._current_id = 0


@dataclass
class EdgeDataGenerator:
    """Generator for edge/relationship table data."""

    table_name: str
    short_name: str
    source_id_property: str
    sink_id_property: str
    source_id_type: str
    sink_id_type: str
    properties: list[PropertyGenerator] = field(default_factory=list)

    def generate_edges(
        self,
        source_ids: list[int],
        sink_ids: list[int],
        *,
        density: float = 0.3,
        allow_self_loops: bool = False,
    ) -> list[dict[str, Any]]:
        """Generate edge data between source and sink nodes.

        Args:
            source_ids: List of source node IDs.
            sink_ids: List of sink node IDs.
            density: Probability of edge creation (0.0-1.0).
            allow_self_loops: Whether to allow edges from a node to itself.

        Returns:
            List of edge dictionaries.
        """
        edges = []
        for source_id in source_ids:
            for sink_id in sink_ids:
                if not allow_self_loops and source_id == sink_id:
                    continue
                if random.random() < density:
                    edge = {
                        self.source_id_property: source_id,
                        self.sink_id_property: sink_id,
                    }
                    for prop in self.properties:
                        edge[prop.name] = prop.generate()
                    edges.append(edge)
        return edges


class SchemaDataGenerator:
    """Generate sample data for an entire schema."""

    def __init__(
        self,
        spark: SparkSession,
        yaml_schema: dict[str, Any],
        *,
        seed: int | None = 42,
    ) -> None:
        """Initialize the generator.

        Args:
            spark: SparkSession for creating DataFrames.
            yaml_schema: Schema definition from YAML.
            seed: Random seed for reproducibility.
        """
        self.spark = spark
        self.yaml_schema = yaml_schema
        self.node_generators: dict[str, TableDataGenerator] = {}
        self.edge_generators: dict[str, EdgeDataGenerator] = {}
        self.node_ids: dict[str, list[int]] = {}
        self.generated_dfs: dict[str, DataFrame] = {}

        if seed is not None:
            random.seed(seed)

        self._parse_schema()

    def _extract_short_name(self, full_name: str) -> str:
        """Extract table name without catalog.schema prefix."""
        parts = full_name.split(".")
        return parts[-1]

    def _parse_schema(self) -> None:
        """Parse YAML schema and create generators."""
        schema = self.yaml_schema.get("schema", {})

        # Parse nodes
        for node in schema.get("nodes", []):
            id_prop = node.get("idProperty", {})
            props = [
                PropertyGenerator(
                    name=p["name"],
                    data_type=p.get("type", "string"),
                )
                for p in node.get("properties", [])
            ]

            generator = TableDataGenerator(
                table_name=node["tableName"],
                short_name=self._extract_short_name(node["tableName"]),
                id_property=id_prop.get("name", "id"),
                id_type=id_prop.get("type", "int"),
                properties=props,
            )
            self.node_generators[node["name"]] = generator

        # Parse edges
        for edge in schema.get("edges", []):
            source_id = edge.get("sourceIdProperty", {})
            sink_id = edge.get("sinkIdProperty", {})
            props = [
                PropertyGenerator(
                    name=p["name"],
                    data_type=p.get("type", "string"),
                )
                for p in edge.get("properties", [])
            ]

            generator = EdgeDataGenerator(
                table_name=edge["tableName"],
                short_name=self._extract_short_name(edge["tableName"]),
                source_id_property=source_id.get("name", "source_id"),
                sink_id_property=sink_id.get("name", "sink_id"),
                source_id_type=source_id.get("type", "int"),
                sink_id_type=sink_id.get("type", "int"),
                properties=props,
            )
            self.edge_generators[edge["name"]] = generator

    def generate_node_data(
        self,
        node_name: str,
        count: int = 10,
    ) -> DataFrame:
        """Generate data for a node type.

        Args:
            node_name: Name of the node type.
            count: Number of rows to generate.

        Returns:
            PySpark DataFrame with generated data.
        """
        generator = self.node_generators[node_name]
        generator.reset()
        rows = generator.generate_rows(count)
        self.node_ids[node_name] = [r[generator.id_property] for r in rows]

        df = self.spark.createDataFrame(rows)
        self.generated_dfs[generator.short_name] = df
        return df

    def generate_edge_data(
        self,
        edge_name: str,
        *,
        density: float = 0.3,
        allow_self_loops: bool = False,
    ) -> DataFrame:
        """Generate data for an edge type.

        Requires that source and sink node data has already been generated.

        Args:
            edge_name: Name of the edge type.
            density: Probability of edge creation.
            allow_self_loops: Whether to allow self-loops.

        Returns:
            PySpark DataFrame with generated edge data.
        """
        schema = self.yaml_schema.get("schema", {})

        # Find edge definition to get source/sink node types
        edge_def = None
        for e in schema.get("edges", []):
            if e["name"] == edge_name:
                edge_def = e
                break

        if edge_def is None:
            raise ValueError(f"Edge type '{edge_name}' not found in schema")

        source_node = edge_def["sourceNode"]
        sink_node = edge_def["sinkNode"]

        if source_node not in self.node_ids:
            raise ValueError(
                f"Source node '{source_node}' data must be generated before edge '{edge_name}'"
            )
        if sink_node not in self.node_ids:
            raise ValueError(
                f"Sink node '{sink_node}' data must be generated before edge '{edge_name}'"
            )

        generator = self.edge_generators[edge_name]
        rows = generator.generate_edges(
            self.node_ids[source_node],
            self.node_ids[sink_node],
            density=density,
            allow_self_loops=allow_self_loops,
        )

        # Handle case with no edges
        if not rows:
            # Create at least one edge to avoid empty DataFrame issues
            source_id = self.node_ids[source_node][0]
            sink_ids = self.node_ids[sink_node]
            # If same node type and no self-loops, try to find different sink
            if source_node == sink_node and not allow_self_loops and len(sink_ids) > 1:
                sink_id = sink_ids[1]
            else:
                sink_id = sink_ids[0]

            edge = {
                generator.source_id_property: source_id,
                generator.sink_id_property: sink_id,
            }
            for prop in generator.properties:
                edge[prop.name] = prop.generate()
            rows = [edge]

        df = self.spark.createDataFrame(rows)
        self.generated_dfs[generator.short_name] = df
        return df

    def generate_all(
        self,
        node_counts: dict[str, int] | None = None,
        edge_density: float = 0.3,
        default_node_count: int = 10,
    ) -> dict[str, DataFrame]:
        """Generate data for all nodes and edges in the schema.

        Args:
            node_counts: Optional dict mapping node names to row counts.
            edge_density: Probability of edge creation.
            default_node_count: Default number of rows per node type.

        Returns:
            Dict mapping short table names to DataFrames.
        """
        node_counts = node_counts or {}

        # Generate all nodes first
        for node_name in self.node_generators:
            count = node_counts.get(node_name, default_node_count)
            self.generate_node_data(node_name, count)

        # Then generate all edges
        for edge_name in self.edge_generators:
            self.generate_edge_data(edge_name, density=edge_density)

        return self.generated_dfs

    def register_all_views(self) -> None:
        """Register all generated DataFrames as temporary views."""
        for name, df in self.generated_dfs.items():
            df.createOrReplaceTempView(name)

    def get_dataframe(self, short_name: str) -> DataFrame | None:
        """Get a generated DataFrame by short table name."""
        return self.generated_dfs.get(short_name)


def generate_sample_data_for_yaml(
    spark: SparkSession,
    yaml_data: dict[str, Any],
    *,
    node_counts: dict[str, int] | None = None,
    edge_density: float = 0.3,
    default_node_count: int = 10,
    seed: int | None = 42,
) -> SchemaDataGenerator:
    """Convenience function to generate sample data from a YAML schema.

    Args:
        spark: SparkSession for creating DataFrames.
        yaml_data: YAML data with schema definition.
        node_counts: Optional dict mapping node names to row counts.
        edge_density: Probability of edge creation.
        default_node_count: Default number of rows per node type.
        seed: Random seed for reproducibility.

    Returns:
        SchemaDataGenerator with generated data and registered views.
    """
    generator = SchemaDataGenerator(spark, yaml_data, seed=seed)
    generator.generate_all(
        node_counts=node_counts,
        edge_density=edge_density,
        default_node_count=default_node_count,
    )
    generator.register_all_views()
    return generator

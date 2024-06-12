"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from abc import ABC, abstractmethod
from typing import List, Dict

from gravitino.api.schema import Schema
from gravitino.api.schema_change import SchemaChange


class NoSuchSchemaException(Exception):
    """Exception raised if the schema does not exist."""

    pass


class SupportsSchemas(ABC):
    """
    The Catalog interface to support schema operations. If the implemented catalog has schema
    semantics, it should implement this interface.
    """

    @abstractmethod
    def list_schemas(self) -> List[str]:
        """List schemas under the catalog.

        If an entity such as a table, view exists, its parent schemas must also exist and must be
        returned by this discovery method. For example, if table a.b.t exists, this method invoked as
        list_schemas(a) must return [a.b] in the result array.

        Raises:
            NoSuchCatalogException: If the catalog does not exist.

        Returns:
            A list of schema names (as strings) under the catalog.
        """
        pass

    def schema_exists(self, schema_name: str) -> bool:
        """Check if a schema exists.

        If an entity such as a table, view exists, its parent namespaces must also exist. For
        example, if table a.b.t exists, this method invoked as schema_exists(a.b) must return true.

        Args:
            schema_name: The name of the schema.

        Returns:
            True if the schema exists, false otherwise.
        """
        try:
            self.load_schema(schema_name)
            return True
        except NoSuchSchemaException:
            return False

    @abstractmethod
    def create_schema(
        self, schema_name: str, comment: str, properties: Dict[str, str]
    ) -> Schema:
        """Create a schema in the catalog.

        Args:
            schema_name: The name of the schema.
            comment: The comment of the schema.
            properties: The properties of the schema.

        Raises:
            NoSuchCatalogException: If the catalog does not exist.
            SchemaAlreadyExistsException: If the schema already exists.

        Returns:
            The created schema.
        """
        pass

    @abstractmethod
    def load_schema(self, schema_name: str) -> Schema:
        """Load metadata properties for a schema.

        Args:
            schema_name: The name of the schema.

        Raises:
            NoSuchSchemaException: If the schema does not exist.

        Returns:
            A schema.
        """
        pass

    @abstractmethod
    def alter_schema(self, schema_name: str, *changes: SchemaChange) -> Schema:
        """Apply the metadata change to a schema in the catalog.

        Args:
            schema_name: The name of the schema.
            changes: The metadata changes to apply.

        Raises:
            NoSuchSchemaException: If the schema does not exist.

        Returns:
            The altered schema.
        """
        pass

    @abstractmethod
    def drop_schema(self, schema_name: str, cascade: bool) -> bool:
        """Drop a schema from the catalog. If cascade option is true, recursively
        drop all objects within the schema.

        Args:
            schema_name: The name of the schema.
            cascade: If true, recursively drop all objects within the schema.

        Returns:
            True if the schema exists and is dropped successfully, false otherwise.

        Raises:
            NonEmptySchemaException: If the schema is not empty and cascade is false.
        """
        pass

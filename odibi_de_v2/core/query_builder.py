from abc import ABC, abstractmethod
from typing import List, Optional
class BaseQueryBuilder(ABC):
    """
    Abstract base class for all SQL query builders.

    All query builders must implement methods to build the query
    string and retrieve any query parameters.
    """

    @abstractmethod
    def build(self) -> str:
        """
        Build and return the SQL query as a string.

        Returns:
            str: The generated SQL query.
        """
        pass

    @abstractmethod
    def get_parameters(self) -> Optional[List]:
        """
        Retrieve the list of parameters for the query, if any.

        Returns:
            Optional[List]: The parameters for parameterized queries,
            or None if no parameters exist.
        """
        pass
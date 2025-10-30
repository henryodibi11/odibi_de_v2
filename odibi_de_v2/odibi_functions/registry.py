from typing import Dict, Optional, Callable, Tuple, List
from threading import Lock


class FunctionRegistry:
    """Singleton registry for engine-specific and universal functions.
    
    The FunctionRegistry manages reusable functions that can be registered for
    specific engines (spark, pandas) or universally (any). It follows a singleton
    pattern to ensure a single global registry across the application.
    
    Architecture:
        - Singleton pattern ensures one global registry
        - Thread-safe registration and resolution
        - Engine-specific overrides (spark/pandas) take priority over universal (any)
        - Functions stored with (name, engine) as composite key
    
    Examples:
        >>> from odibi_de_v2.odibi_functions import REGISTRY
        >>> 
        >>> # Register a spark-specific function
        >>> def spark_transform(df):
        ...     return df.filter("value > 0")
        >>> REGISTRY.register("positive_filter", "spark", spark_transform)
        >>> 
        >>> # Register a pandas fallback
        >>> def pandas_transform(df):
        ...     return df[df['value'] > 0]
        >>> REGISTRY.register("positive_filter", "pandas", pandas_transform)
        >>> 
        >>> # Resolve for specific engine
        >>> fn = REGISTRY.resolve("mymodule", "positive_filter", "spark")
        >>> result = fn(data)
        >>> 
        >>> # List all registered functions
        >>> all_funcs = REGISTRY.get_all()
        >>> print(all_funcs)
        [('positive_filter', 'spark'), ('positive_filter', 'pandas')]
    """
    
    _instance: Optional['FunctionRegistry'] = None
    _lock: Lock = Lock()
    
    def __new__(cls) -> 'FunctionRegistry':
        """Ensure singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize()
        return cls._instance
    
    def _initialize(self) -> None:
        """Initialize registry storage."""
        self._functions: Dict[Tuple[str, str], Callable] = {}
        self._metadata: Dict[Tuple[str, str], Dict] = {}
    
    def register(
        self,
        name: str,
        engine: str,
        fn: Callable,
        module: Optional[str] = None,
        description: Optional[str] = None,
        **metadata
    ) -> None:
        """Register a function for a specific engine or universally.
        
        Args:
            name: Unique function name (e.g., "clean_nulls", "deduplicate")
            engine: Target engine ("spark", "pandas", "any")
            fn: Callable function to register
            module: Optional module/namespace for organization
            description: Optional function description
            **metadata: Additional metadata (author, version, tags, etc.)
        
        Raises:
            ValueError: If name or engine is empty
            TypeError: If fn is not callable
        
        Examples:
            >>> def clean_data(df):
            ...     return df.dropna()
            >>> REGISTRY.register(
            ...     "clean_nulls",
            ...     "pandas",
            ...     clean_data,
            ...     module="cleaning",
            ...     description="Remove null rows",
            ...     author="data_team"
            ... )
        """
        if not name or not isinstance(name, str):
            raise ValueError("Function name must be a non-empty string")
        if not engine or not isinstance(engine, str):
            raise ValueError("Engine must be a non-empty string")
        if not callable(fn):
            raise TypeError(f"Expected callable, got {type(fn).__name__}")
        
        engine = engine.lower()
        key = (name, engine)
        
        with self._lock:
            self._functions[key] = fn
            self._metadata[key] = {
                "module": module,
                "description": description or fn.__doc__,
                **metadata
            }
    
    def resolve(
        self,
        module: Optional[str],
        func: str,
        engine: str
    ) -> Optional[Callable]:
        """Resolve a function by name and engine with fallback logic.
        
        Resolution order:
            1. Exact match: (func, engine)
            2. Universal fallback: (func, "any")
            3. None if not found
        
        Args:
            module: Optional module namespace (reserved for future use)
            func: Function name to resolve
            engine: Target engine ("spark", "pandas")
        
        Returns:
            Registered callable or None if not found
        
        Examples:
            >>> # Register universal function
            >>> REGISTRY.register("count_rows", "any", lambda df: len(df))
            >>> 
            >>> # Resolves to universal implementation
            >>> fn = REGISTRY.resolve(None, "count_rows", "pandas")
            >>> fn is not None
            True
            >>> 
            >>> # Register spark-specific override
            >>> REGISTRY.register("count_rows", "spark", lambda df: df.count())
            >>> 
            >>> # Resolves to spark-specific implementation
            >>> spark_fn = REGISTRY.resolve(None, "count_rows", "spark")
            >>> pandas_fn = REGISTRY.resolve(None, "count_rows", "pandas")
            >>> spark_fn is not pandas_fn
            True
        """
        engine = engine.lower()
        
        engine_key = (func, engine)
        if engine_key in self._functions:
            return self._functions[engine_key]
        
        any_key = (func, "any")
        if any_key in self._functions:
            return self._functions[any_key]
        
        return None
    
    def get_all(self, engine: Optional[str] = None) -> List[Tuple[str, str]]:
        """Get all registered functions, optionally filtered by engine.
        
        Args:
            engine: Optional engine filter ("spark", "pandas", "any")
        
        Returns:
            List of (name, engine) tuples
        
        Examples:
            >>> REGISTRY.register("fn1", "spark", lambda x: x)
            >>> REGISTRY.register("fn2", "pandas", lambda x: x)
            >>> REGISTRY.register("fn3", "any", lambda x: x)
            >>> 
            >>> # Get all functions
            >>> len(REGISTRY.get_all()) >= 3
            True
            >>> 
            >>> # Filter by engine
            >>> spark_funcs = REGISTRY.get_all(engine="spark")
            >>> ('fn1', 'spark') in spark_funcs
            True
        """
        if engine:
            engine = engine.lower()
            return [
                (name, eng)
                for (name, eng) in self._functions.keys()
                if eng == engine
            ]
        return list(self._functions.keys())
    
    def get_metadata(self, name: str, engine: str) -> Optional[Dict]:
        """Get metadata for a registered function.
        
        Args:
            name: Function name
            engine: Engine identifier
        
        Returns:
            Metadata dictionary or None if not found
        
        Examples:
            >>> REGISTRY.register(
            ...     "test_fn",
            ...     "spark",
            ...     lambda x: x,
            ...     module="test",
            ...     version="1.0"
            ... )
            >>> meta = REGISTRY.get_metadata("test_fn", "spark")
            >>> meta['module']
            'test'
            >>> meta['version']
            '1.0'
        """
        engine = engine.lower()
        key = (name, engine)
        return self._metadata.get(key)
    
    def unregister(self, name: str, engine: str) -> bool:
        """Unregister a function.
        
        Args:
            name: Function name
            engine: Engine identifier
        
        Returns:
            True if function was unregistered, False if not found
        
        Examples:
            >>> REGISTRY.register("temp_fn", "any", lambda x: x)
            >>> REGISTRY.unregister("temp_fn", "any")
            True
            >>> REGISTRY.unregister("temp_fn", "any")
            False
        """
        engine = engine.lower()
        key = (name, engine)
        
        with self._lock:
            if key in self._functions:
                del self._functions[key]
                self._metadata.pop(key, None)
                return True
            return False
    
    def clear(self) -> None:
        """Clear all registered functions (useful for testing).
        
        Examples:
            >>> REGISTRY.register("test", "any", lambda x: x)
            >>> len(REGISTRY.get_all()) > 0
            True
            >>> REGISTRY.clear()
            >>> len(REGISTRY.get_all())
            0
        """
        with self._lock:
            self._functions.clear()
            self._metadata.clear()


REGISTRY = FunctionRegistry()

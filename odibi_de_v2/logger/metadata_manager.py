

class MetadataManager:
    """
    Manages dynamic metadata for logging or contextual tracking.

    This class provides a simple interfact for storing and updating
    metadata key-value pairs that can be injected into log records or used
    throughout a processing pipeline

    Attributes:
        metadata (dict): Dictionary containing current metadata values.

    Example:
        >>> manager = MetadataManager()
        >>> manager.update_metadata(project="OEE", step="validate")
        >>> manager.get_metadata()
            # {'project': 'OEE', 'step': 'validate'}
        >>> manager.update_metadata(clear_existing=True, phase="ingest")
        >>> manager.get_metadata()
            {'phase': 'ingest'}
    """

    def __init__(self):
        """
        Initialize a MetadataManager instance with an empty
        metadata dictionary.
        """
        self.metadata = {}

    def update_metadata(self, clear_existing: bool = False, **kwargs):
        """
        Update or replace metadata key-value pairs.
        Args:
            clear_existing (bool): If True, clears all existing metadata
                before applying new values. Defaults to False.
            **kwargs: Arbitrary key-value metadata entries to add or update.
        Raises:
            ValueError: If `clear_existing` is not a boolean.
        """
        if clear_existing:
            error_message = (
                "'clear_existing' must be a boolean."
            )
            if not isinstance(clear_existing, bool):
                print(error_message)
                raise ValueError(error_message)
            # Remove all existing keys
            self.metadata.clear()
        self.metadata.update(kwargs)

    def get_metadata(self):
        """
        Retrieve the current metadata dictionary.

        Returns:
            dict: The current metadata values.
        """
        return self.metadata



class LoadSaveStrategy:
    """Interface for determining which load strategy to use for loading attributes from a database."""
    def load(self, *args, **kwargs):
        return NotImplementedError
    
    def save(self, *args, **kwargs):
        return NotImplementedError
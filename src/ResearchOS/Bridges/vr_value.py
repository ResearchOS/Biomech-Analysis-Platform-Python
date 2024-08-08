from typing import Any, TYPE_CHECKING


if TYPE_CHECKING:
    pass

# Exit codes:
# 0: Success

class VRValue():

    def __init__(self, value: Any = None, exit_code: int = None) -> None:
        self.value = value
        self.exit_code = exit_code

        


        

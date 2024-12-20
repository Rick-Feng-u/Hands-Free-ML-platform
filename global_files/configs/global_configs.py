from abc import ABC
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import datetime

class GlobalConfigs(ABC):
    def __init__(self):
        self.gcp_region = 'us-east-1'
        self.today = datetime.datetime.now()
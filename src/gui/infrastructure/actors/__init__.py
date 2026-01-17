# GUI Actors Package
"""
Actor-based components for GUI state management and control.
Uses the existing BaseActor framework from the scraper system.
"""
from .gui_state import GuiStateActor
from .gui_controller import GuiControllerActor
from .gui_bridge import GuiBridgeActor
from .scjn_bridge import (
    SCJNGuiBridgeActor,
    SCJNSearchConfig,
    SCJNProgress,
    SCJNJobStarted,
    SCJNJobProgress,
    SCJNJobCompleted,
    SCJNJobFailed,
)
from .bjv_bridge import (
    BJVGuiBridgeActor,
    BJVSearchConfig,
    BJVProgress,
    BJVJobStarted,
    BJVJobProgress,
    BJVJobCompleted,
    BJVJobFailed,
)
from .cas_gui_bridge import (
    CASGuiBridgeActor,
    CASGuiConfig,
    CASJobStarted,
    CASJobProgress,
    CASJobCompleted,
    CASJobError,
)
from .dof_gui_bridge import (
    DOFGuiBridgeActor,
    DOFGuiConfig,
    DOFJobStarted,
    DOFJobProgress,
    DOFJobCompleted,
    DOFJobError,
)

__all__ = [
    'GuiStateActor',
    'GuiControllerActor',
    'GuiBridgeActor',
    'SCJNGuiBridgeActor',
    'SCJNSearchConfig',
    'SCJNProgress',
    'SCJNJobStarted',
    'SCJNJobProgress',
    'SCJNJobCompleted',
    'SCJNJobFailed',
    'BJVGuiBridgeActor',
    'BJVSearchConfig',
    'BJVProgress',
    'BJVJobStarted',
    'BJVJobProgress',
    'BJVJobCompleted',
    'BJVJobFailed',
    'CASGuiBridgeActor',
    'CASGuiConfig',
    'CASJobStarted',
    'CASJobProgress',
    'CASJobCompleted',
    'CASJobError',
    'DOFGuiBridgeActor',
    'DOFGuiConfig',
    'DOFJobStarted',
    'DOFJobProgress',
    'DOFJobCompleted',
    'DOFJobError',
]

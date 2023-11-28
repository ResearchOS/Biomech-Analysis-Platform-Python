"""The set of states for all widgets."""

from widget_objects.widget_object import WidgetObject

class WState():    
    def __init__(self, widgets: list[WidgetObject]):
        self.widgets = {}
        for widget in widgets:
            id = widget['id']
            del widget['id']
            self.widgets[id] = widget
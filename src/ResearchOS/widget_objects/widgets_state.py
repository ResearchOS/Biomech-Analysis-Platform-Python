from widget_objects.widget_object import WidgetObject

class WState():    
    """The set of states for all widgets. One widget state contains a dictionary of widget objects."""
    def __init__(self, action_id, widgets: list[WidgetObject]):
        self.action_id = action_id
        if not isinstance(widgets, list):
            widgets = [widgets]
        self.widgets = {}
        for widget in widgets:
            id = widget.id            
            self.widgets[id] = widget

    def set_state(self):
        """Set the state of each widget."""
        for widget_id in self.widgets.keys():
            self.widgets[widget_id].draw()

if __name__ == "__main__":
    wobj = WidgetObject
    wobj.id = 1
    wstate = WState(wobj)
    wstate.set_state()
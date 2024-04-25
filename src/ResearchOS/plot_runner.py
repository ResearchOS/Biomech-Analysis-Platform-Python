from typing import TYPE_CHECKING
import os

if TYPE_CHECKING:
    from ResearchOS.PipelineObjects.plot import Plot

from ResearchOS.code_runner import CodeRunner

class PlotRunner(CodeRunner):

    def __init__(self) -> None:
        pass

    def get_save_path(self, pl: "Plot"):
        """Return the save path for the plot."""
        lineage = self.node.get_node_lineage(action=self.action)
        if self.node in lineage:
            lineage.remove(self.node)
        dataset_parent_folder = os.path.dirname(self.dataset.dataset_path)
        plots_folder = os.path.join(dataset_parent_folder, "plots")
        save_path = os.path.join(plots_folder, pl.id + " " + pl.name)
        for l in lineage[::-1]:
            save_path = os.path.join(save_path, l.name)
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        if self.node.id != self.dataset.id:
            save_path = os.path.join(save_path, self.node.name)
        return save_path
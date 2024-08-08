from typing import TYPE_CHECKING
import os

import networkx as nx

if TYPE_CHECKING:
    from ResearchOS.PipelineObjects.plot import Plot

from ResearchOS.code_runner import CodeRunner

class PlotRunner(CodeRunner):

    def __init__(self) -> None:
        pass

    def get_save_path(self, pl: "Plot"):
        """Return the save path for the plot."""
        lineage = self.node.get_node_lineage(action=self.action)[::-1]
        ds_schema = self.dataset.schema
        schema_G = nx.MultiDiGraph()
        schema_G.add_edges_from(ds_schema)
        ds_schema_nodes = list(nx.topological_sort(schema_G))
        if self.pl_obj.batch:
            batch = self.pl_obj.batch
            if self.dataset.__class__ in batch:
                batch = batch[1:]
        else:
            batch = [self.pl_obj.level]

        # 1. Get all schema nodes that are not in the batch, that are lower than the lowest node in the batch.
        lowest_batch_idx_in_schema = min([ds_schema_nodes[1:].index(b) for b in batch]) # Omit the dataset.
        folder_names = lineage[0:lowest_batch_idx_in_schema]
        folder_names = [f.name for f in folder_names]
        file_name_idx = [idx for idx in range(len(lineage)) if lineage[idx].__class__ == batch[0]]
        if len(file_name_idx) == 0:
            file_name = self.dataset.name # For Dataset, just use the top level node name
        else:
            file_name_idx = file_name_idx[0]
            file_name = lineage[file_name_idx].name

        dataset_parent_folder = os.path.dirname(self.dataset.dataset_path)
        plots_folder = os.path.join(dataset_parent_folder, "plots")
        save_path = os.path.join(plots_folder, pl.id + " " + pl.name)
        for folder_name in folder_names:
            save_path = os.path.join(save_path, folder_name)
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        save_path = os.path.join(save_path, file_name)
        return save_path
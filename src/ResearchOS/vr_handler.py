from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ResearchOS.variable import Variable
    from ResearchOS.research_object import ResearchObject

class VRHandler():

    @staticmethod
    def add_slice_to_input_vrs(input_dict: dict):
        new_dict = {}
        for key, vr in input_dict.items():
            new_dict[key] = {}
            new_dict[key]["VR"] = vr
            slice = getattr(vr, "slice", None)
            new_dict[key]["slice"] = slice
            try:
                del vr.slice
            except:
                pass
        return new_dict

    def set_output_vrs(self, robj: "ResearchObject", output_vrs):
        self.output_vrs = output_vrs

    def set_vrs_source_pr(self, robj: "ResearchObject", vrs_source_pr):
        self.vrs_source_pr = vrs_source_pr
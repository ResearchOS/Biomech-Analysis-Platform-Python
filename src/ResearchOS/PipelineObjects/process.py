from src.ResearchOS.PipelineObjects import PipelineObject

class Process(PipelineObject):

    def run(self):
        pass


    def square(x):
        return x^2


if __name__=="__main__":
    pr = Process()
    pr.add_input_variable(var = "id1")
    pr.add_output_variable(var = "id2")
    pr.assign_code(Process.square)
    pr.subset_data(gender == 'm')
    pr.run()


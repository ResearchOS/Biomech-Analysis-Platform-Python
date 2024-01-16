from ResearchOS.PipelineObjects import Project, Analysis, Logsheet, Subset, Process
from DataObjects import Dataset, Subject, Visit, Trial
# from ..ResearchOS import Variable, User

class Quickstart():
    "Testing"
    def __init__():
        "Testing"
        pass

    def method1():
        "Testing"

# The way I want the imports to work:
# import ResearchOS as ros
# pj = ros.Project

# 1. Create a new ResearchOS project, and make it the current project.
# This will also create a new Dataset, Analysis, and Logsheet object.
pj = Project.new_current(name = 'quickstart')

# Access the current analysis, dataset, and logsheet in one of two ways.
# a. Call the parent instance's method.
an = pj.current_analysis()
lg = an.current_logsheet_id()
ds = pj.current_dataset()

# b. Use the static method of that class with the parent as an argument.
an = Analysis.current_analysis(pj_id = pj.id) # For the current project.
lg = Logsheet.current_logsheet(an_id = an.id) # For the current analysis.
ds = Dataset.current_dataset(pj_id = pj.id) # For the current project.

# 2. Provide the metadata for the dataset.
ds.root_path = '/home/username/data'

# 3. Provide the metadata for the logsheet.
# a. The path to the logsheet.
lg.path = '/home/username/data/logsheet.csv'
# b. The number of rows in the header. Starts reading data after this row.
lg.num_header_rows = 3
# c. The column header names for the columns that map to data objects.
lg.col_header[Subject] = 'Subject Codename' # Key is the class itself, value is the column header.
lg.col_header[Visit] = 'Visit Number'
# d. The column header names for the rest of the columns, which map to variables
lg.col_header['Age'] = ('Age (years)', int) # Key is the class name, value is the column header.

# 4. Specify the trials that you want the logsheet to operate over.
# a. Create a Subset object.
sb = Subset(name = "Test")
# b. Add logic to the subset.
sb.incl = [
    ('Age', '>', 18),
    ('Age', '<', 30)
]
sb.excl = [
    ('Visit Number', '==', 2)
]

# 5. Apply the subset to the logsheet.
lg.set_subset(sb.id)

# 6. Run the logsheet data import. This will import all of the data from the specified logsheet to the SQL database.
lg.import_metadata()

# 7. Create a Process object, add it to the current analysis, and run it.
# a. Create the process.
pr = Process(name = "Test")

# b. Set the level of the process. Must be a data object.
pr.set_level(Trial)

# c. Add the process to the current analysis.
an.add_pr(pr)

# d. Add input and output arguments to the process.
vr0 = Variable(id = "VR000000_000")
vr1 = Variable(id = "VR000000_001")
pr.set_inputs(vr0)
pr.set_outputs(vr1)

# e. Set the subset of data for this pr to operate over.
pr.set_subset(sb.id)

# f. Run the process. Outputs are saved to the SQL database.
pr.run()

# 8. Access the data that was just created
vr = Variable(id = "VR000000_001")
data = vr.get_data() # Get all values of this variable from all levels of all data objects.
data = vr.get_data(subset = sb.id) # Get all values of this variable from all levels of the data objects in the specified subset.


# 9. Plot the data using standard tools.
import matplotlib.pyplot as plt
plt.plot(data) 

# 10. Create another process with using the output of the first process as input.
# a. Create the process.
pr = Process(name = "Test2")

# b. Set the level of the process. Must be a data object.
pr.set_level(Trial)

# c. Add the process to the current analysis.
an.add_pr(pr)

# d. Add input and output arguments to the process.
pr.set_inputs(vr1)
vr2 = Variable(id = "VR000000_002")
pr.set_outputs(vr2)

# e. Set the subset of data for this pr to operate over.
pr.set_subset(sb.id)

# f. Run the process. Outputs are saved to the SQL database.
pr.run()

# Repeat any of the data access or plotting steps above to access the data from this process.
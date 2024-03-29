import ResearchOS as ros

# Example of a dynamic variable. The "name" is optional.
vr1 = ros.Variable(id = "VR1", name = "Variable 1")

# Example of a hard-coded variable.
hard_coded_vr = ros.Variable(id = "VR2", hard_coded_value=5)

# Note that for more complex hard-coded variables, you can load from a file.
# with(open("path/to/file")) as f:
#     hard_coded_value = f.read()
# vr = ros.Variable(id = "VR3", hard_coded_value=hard_coded_value)
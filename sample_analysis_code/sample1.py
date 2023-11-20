"""Trying to get a feel for what a "sample analysis code" might look like."""

from objects.dataset import Dataset
from objects.subject import Subject
from objects.visit import Visit
from objects.trial import Trial
from objects.phase import Phase
from objects.variable import Variable
from objects.subvariable import Subvariable

# Create a dataset (which is a collection of subjects)
dataset = Dataset.new()

# Create new subjects
subject1 = Subject.new(dataset, name = "Subject1")
subject2 = Subject.new(dataset, name = "Subject2")

# Create new visits
visit1 = Visit.new(subject1, name = "Visit1")

# Create new trials
trial1 = Trial.new(visit1, name = "Trial1")

# Create new phases
phase1 = Phase.new(name = "Motive Frame Range")

# Add the phase to the trial
trial1.add_phase(phase1)

# Create new variables
variable1 = Variable.new(name = "TBCMPosition")

# Create new subvariables
subvariable1 = Subvariable.new(name = "Column 2")

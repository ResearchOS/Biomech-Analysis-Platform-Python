from pathlib import Path

PACKAGES_PREFIX = 'ros-'

TMP_PACKAGES_PATH = Path("/Users/mitchelltillman/Desktop/Work/Stevens_PhD/Non_Research_Projects/ResearchOS_Python/tests/fixtures/tmp_packages")

# Simplest package where everything is in the same folder, and are each one file, and it is NOT a project.
PACKAGE1_PATH = TMP_PACKAGES_PATH / (PACKAGES_PREFIX + "package1")
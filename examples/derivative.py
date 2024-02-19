# import numpy as np

def derivative(data, fs):
    # data: Trial level variable
    # fs: Dataset level? Visit level?

    deriv = np.diff(data) / fs

    return deriv
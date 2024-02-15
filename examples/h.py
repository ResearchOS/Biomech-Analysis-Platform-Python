import numpy as np

def r_seg_com_rel_tbcm(r_seg_com: dict, r_tbcm: list) -> dict:
    """Compute the segment center of mass position relative to TBCM position."""

    # Compute the relative position of the segment center of mass to the TBCM
    rel_pos = {}
    for seg in r_seg_com:
        rel_pos[seg] = r_seg_com[seg] - r_tbcm

    return rel_pos

def v_seg_com_rel_tbcm(v_seg_com: dict, v_tbcm: list) -> dict:
    """Compute the segment center of mass velocity relative to TBCM velocity."""

    # Compute the relative velocity of the segment center of mass to the TBCM
    rel_vel = {}
    for seg in v_seg_com:
        rel_vel[seg] = v_seg_com[seg] - v_tbcm

    return rel_vel

def w_segs(seg_anat_ax: dict, fs: int) -> dict:
    """Compute the angular velocity of the segments."""
    delta_t = 1/fs
    w = {}
    for seg in seg_anat_ax:
        # seg: N x 1 array
        w[seg] = []
        for frame_num in range(0,len(seg)-1):
            # frame: 3 x 1 array of 1x3.

            # Calculate the relative rotation matrix
            R1 = seg[frame_num]
            R2 = seg[frame_num + 1]
            Delta_R = R2 @ np.linalg.inv(R1)

            # Approximate the skew-symmetric part of the angular velocity
            skew_symmetric = (Delta_R - np.eye(3)) / delta_t

            # Extract the angular velocity vector from the skew-symmetric matrix
            omega = np.array([skew_symmetric[2, 1], skew_symmetric[0, 2], skew_symmetric[1, 0]])

            w[seg].append(omega)

    return w

def I_segs(seg_mass: dict, seg_com: dict, seg_anat_ax: dict) -> dict:
    """Compute the inertia of the segments in (global? local?) coordinates."""
    I = {}
    for seg in seg_mass:
        I[seg] = seg_mass[seg] * np.dot(seg_com[seg], seg_com[seg]) * np.eye(3)
    return I

def H(w: dict, I: dict, r: dict, v: dict, m: dict) -> list:
    """Compute the angular momentum of the segments."""
    H = {}
    for seg in w:
        H[seg] = I[seg] @ w[seg]
    return H
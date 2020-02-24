import argparse
from time import time, sleep
import os
from glob import glob
from boutiques.descriptor2func import function

import pydra
import typing as ty

import nibabel as nib
import numpy as np


def group_analysis(brain_files):
    brain_sizes = []
    for brain_file in brain_files:
        data = nib.load(brain_file.output.out).get_fdata()
        brain_sizes.append((data != 0).sum())

    return np.array(brain_sizes).mean()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Example BIDS app using Pydra and Boutiques (performs participant and group analysis)"
    )
    # From Tristan (TG): to make this more generic, we could parse the inputs of
    # the Boutiques descriptor, including their description, and add them to the
    # parser and pydra workflow inputs. We could also parse the Boutiques outputs
    # and add them to the workflow.
    parser.add_argument(
        "bids_dir",
        help="The directory with the input dataset "
        "formatted according to the BIDS standard.",
    )
    parser.add_argument(
        "output_dir",
        help="The directory where the output files "
        "should be stored. If you are running group level analysis "
        "this folder should be prepopulated with the results of the"
        "participant level analysis.",
    )
    args = parser.parse_args()

    subject_dirs = glob(os.path.join(args.bids_dir, "sub-*"))
    subjects_to_analyze = [subject_dir.split("-")[-1] for subject_dir in subject_dirs]

    wf = pydra.Workflow(
        name="BIDS App Example with Boutiques",
        input_spec=["T1_file", "output_dir"],
        output_dir=args.output_dir,
    )

    # TG: for the pydra integration, we should create a function that returns
    # a function similar to the one below. That function should be generic to any
    # descriptor. See comments below.
    @pydra.mark.task
    # TG: the input arguments of the function should be parsed from the descriptor.
    # This is what boutiques.descriptor2func.function does
    def fsl_bet_boutiques(T1_file, output_dir):
        # TG: the output file paths can be obtained 
        # using bosh.evaluate, passing an invocation.
        # This logic duplicates what bosh does.
        maskfile = os.path.join(
            output_dir,
            (
                os.path.split(T1_file)[-1]
                .replace("_T1w", "_brain")
                .replace(".gz", "")
                .replace(".nii", "")
            ),
        )
        fsl_bet = function("zenodo.3267250")
        # TG: if the inputs are under cwd, no need to mount
        # anything
        ret = fsl_bet(
            "-v{0}:{0}".format(T1_file.split('sub-')[0]),
            "-v{0}:{0}".format(output_dir),
            infile=T1_file,
            maskfile=maskfile,
        )

        if ret.exit_code != 0:
            raise Exception(ret.stdout, ret.stderr)

        return ret.output_files[0].file_name

    T1_files = [
        T1_file
        for subject_label in subjects_to_analyze
        for T1_file in glob(
            os.path.join(args.bids_dir, "sub-%s" % subject_label, "anat", "*_T1w.nii*")
        )
        + glob(
            os.path.join(
                args.bids_dir, "sub-%s" % subject_label, "ses-*", "anat", "*_T1w.nii*"
            )
        )
    ]

    wf.split("T1_file", T1_file=T1_files)

    wf.add(
        fsl_bet_boutiques(
            name="fsl_bet", T1_file=wf.lzin.T1_file, output_dir=wf.lzin.output_dir
        )
    )

    wf.combine("T1_file")
    wf.set_output([("out", wf.fsl_bet.lzout.out)])

    with pydra.Submitter(plugin="cf") as sub:
        sub(wf)
    print("Group analysis result:", group_analysis(wf.result()[0]))

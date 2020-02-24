import argparse
from time import time, sleep
import os

import pydra
import typing as ty

from utils import crawl_dir
import nibabel as nib
import numpy as np

from string import digits

from utils import benchmark


def increment(filename, start, args, it):
    """Increment the data of a Nifti image by 1.

    :param filename: str -- representation of the path for the input file.
    :param start: float -- start time of application.
    :param args: argparser -- Argparse object.
    :param it: int -- iteration number
    :return: str -- output path.
    """
    start_time = time() - start

    img = nib.load(filename)
    data = np.asanyarray(img.dataobj)

    data = data + 1
    sleep(args.delay)

    out_basename = os.path.basename(filename)

    if it > 0:
        out_basename = "{0}{1}".format(it, out_basename.lstrip(digits))
    else:
        out_basename = "{0}inc-{1}".format(it, out_basename)

    out_path = os.path.join(args.output_dir, out_basename)

    out_img = nib.Nifti1Image(data, img.affine, img.header)
    nib.save(out_img, out_path)

    end_time = time() - start

    if args.benchmark:
        benchmark(
            start_time,
            end_time,
            filename,
            args.output_dir,
            args.experiment,
            increment.__name__,
        )

    return out_path

if __name__ == "__main__":

    start = time() # Start time of the pipeline  
  
    parser = argparse.ArgumentParser(description="BigBrain incrementation")
    parser.add_argument(
        "bb_dir",
        type=str,
        help=("The folder containing BigBrain NIfTI images" "(local fs only)"),
    )
    parser.add_argument(
        "output_dir",
        type=str,
        help=("the folder to save incremented images to" "(local fs only)"),
    )
    parser.add_argument(
        "experiment", type=str, help="Name of the experiment being performed"
    )
    parser.add_argument("iterations", type=int, help="number of iterations")
    parser.add_argument(
        "delay", type=float, help="sleep delay during " "incrementation"
    )
    parser.add_argument("--benchmark", action="store_true", help="benchmark results")

    args_ = parser.parse_args()
    paths = crawl_dir(os.path.abspath(args_.bb_dir))

    wf = pydra.Workflow(name="pydra-incrementation", input_spec=["f", "start", "args", "it"],
                             start=start, args=args_, cache_dir=args_.output_dir, it=0)

    print("Output directory", wf.output_dir)

    increment = pydra.mark.task(increment)

    wf.split("f", f=paths)

    func_name = "increment{}".format(0)
    wf.add(increment(name=func_name, filename=wf.lzin.f, start=wf.lzin.start, args=wf.lzin.args, it=wf.lzin.it))

    for i in range(1, args_.iterations):
        prev_func_name = func_name
        func_name = "increment{}".format(i)

        wf.add(increment(name=func_name, filename=wf.graph.nodes_names_map[prev_func_name].lzout.out, start=wf.lzin.start, args=wf.lzin.args, it=i))

    wf.set_output([("out", wf.graph.nodes_names_map[func_name].lzout.out)])


with pydra.Submitter(plugin="cf") as sub:
    sub(wf)
print(wf.result())


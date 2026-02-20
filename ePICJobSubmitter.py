#!/usr/bin/env python3
# Written by Dener De Souza Lemos (BNL)
# February 20th 2026 - Upgraded for XRDFS input

import os
import subprocess
import argparse

def main():
    parser = argparse.ArgumentParser(description="ePIC Condor Submitter (XRDFS input)")
    parser.add_argument("--tag", required=True, help="Job tag prefix, e.g. ep_test")
    parser.add_argument("--exec", default="./job.sh", help="Shell script executable")
    parser.add_argument("--input-dir", required=True, help="XRDFS directory path with input ROOT files")
    parser.add_argument("--output-dir", default="./results", help="Directory for output ROOT files")
    parser.add_argument("--njobs", type=int, default=1, help="Number of jobs to split into")
    parser.add_argument("--job-args", default="", help="Optional extra args")
    args = parser.parse_args()

    # 1. List files from XRDFS
    xrdfs_path = args.input_dir
    print(f"Listing files from XRDFS directory: {xrdfs_path} ...")

    try:
        result = subprocess.run(
            ["xrdfs", "dtn-eic.jlab.org", "ls", xrdfs_path],
            check=True, stdout=subprocess.PIPE, text=True
        )
        files = [f"root://dtn-eic.jlab.org/{line.strip()}" for line in result.stdout.splitlines() if line.strip()]

        if not files:
            print(f"No files found in {xrdfs_path}")
            return

    except subprocess.CalledProcessError as e:
        print(f"Error listing XRDFS files: {e}")
        return

    total_files = len(files)
    n_requested = min(args.njobs, total_files)
    print(f"Found {total_files} files, splitting into {n_requested} jobs...")

    # 2. Prepare output directories
    os.makedirs(args.output_dir, exist_ok=True)
    job_folder = os.path.join(args.output_dir, f"job_{args.tag}")
    os.makedirs(job_folder, exist_ok=True)

    # Save the XRDFS file list to disk for Condor jobs
    input_list_file = os.path.join(job_folder, f"{args.tag}.list")
    with open(input_list_file, "w") as f:
        f.write("\n".join(files))

    # 3. Partition files and prepare the item list for Condor
    master_condor_file = os.path.join(job_folder, f"submit_{args.tag}.sub")
    condor_items_file = os.path.join(job_folder, f"{args.tag}.items")
    job_rows = []

    for i in range(n_requested):
        filenumber = i + 1
        start = i * total_files // n_requested
        end = (i + 1) * total_files // n_requested
        files_for_job = files[start:end]

        # Create per-job input list
        job_input_txt = os.path.join(job_folder, f"{args.tag}_{filenumber}_input.txt")
        with open(job_input_txt, "w") as ftxt:
            ftxt.write("\n".join(files_for_job))

        # Output ROOT file path
        output_root = os.path.join(job_folder, f"{args.tag}_{filenumber}.root")
        job_tag = f"{args.tag}_{filenumber}"

        # Add row to items file
        job_rows.append(f"{job_input_txt}, {output_root}, {job_tag}")

    with open(condor_items_file, "w") as fitem:
        fitem.write("\n".join(job_rows))

    # 4. Write Condor submit file
    extra = f" {args.job_args}" if args.job_args else ""
    with open(master_condor_file, "w") as f:
        f.write("# HTCondor Submit File - V24 Compatible\n")
        f.write("Universe       = vanilla\n")
        f.write(f"Executable     = {args.exec}\n")
        f.write("getenv         = true\n")
        f.write("request_memory = 4G\n")
        f.write("notification   = Never\n\n")
        f.write(f"Arguments      = $(InFile) $(OutFile){extra}\n")
        f.write(f"Output         = {job_folder}/$(Tag).out\n")
        f.write(f"Error          = {job_folder}/$(Tag).err\n")
        f.write(f"Log            = {job_folder}/$(Tag).log\n\n")
        f.write(f"queue InFile, OutFile, Tag from {condor_items_file}\n")

    # 5. Submit to Condor
    print(f"Submitting to Condor using {master_condor_file} ...")
    try:
        subprocess.run(["condor_submit", master_condor_file], check=True)
        print("Submission successful.")
    except subprocess.CalledProcessError as e:
        print(f"Error during submission: {e}")

if __name__ == "__main__":
    main()

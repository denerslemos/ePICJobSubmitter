#!/usr/bin/env python3
# Written by Dener De Souza Lemos (BNL)
# February 19th 2026

import os
import subprocess
import argparse

def main():
    parser = argparse.ArgumentParser(description="V24-Ready ePIC Condor Submitter")
    parser.add_argument("--tag", required=True, help="Job tag prefix, e.g. eAu_test")
    parser.add_argument("--exec", default="./job.sh", help="Shell script executable")
    parser.add_argument("--input-list", default="./input.list", help="Text file with input ROOT files")
    parser.add_argument("--output-dir", default="./results", help="Directory for output ROOT files")
    parser.add_argument("--njobs", type=int, default=1, help="Number of jobs to split into")
    parser.add_argument("--job-args", default="", help="Optional extra args")
    args = parser.parse_args()

    # 1. Read and validate input list
    if not os.path.exists(args.input_list):
        print(f"Error: {args.input_list} not found.")
        return

    with open(args.input_list) as f:
        files = [line.strip() for line in f if line.strip()]

    total_files = len(files)
    n_requested = min(args.njobs, total_files)

    # 2. Prepare Directories
    os.makedirs(args.output_dir, exist_ok=True)
    job_folder = os.path.join(args.output_dir, "job")
    os.makedirs(job_folder, exist_ok=True)

    master_condor_file = os.path.join(job_folder, f"submit_{args.tag}.sub")
    item_list_file = os.path.join(job_folder, f"{args.tag}.items")

    # 3. Partition files and prepare the Item List
    # We create the small input text files now, but we don't write the .sub file yet.
    job_rows = []
    print(f"Partitioning {total_files} files into {n_requested} jobs...")

    for i in range(n_requested):
        filenumber = i + 1
        start = i * total_files // n_requested
        end = (i + 1) * total_files // n_requested
        files_for_job = files[start:end]

        # Create the specific input list for this job chunk
        job_input_txt = os.path.join(job_folder, f"{args.tag}_{filenumber}_input.txt")
        with open(job_input_txt, "w") as ftxt:
            ftxt.write("\n".join(files_for_job))

        output_root = os.path.join(args.output_dir, f"{args.tag}_{filenumber}.root")
        job_tag = f"{args.tag}_{filenumber}"

        # Each row defines: InFile, OutFile, Tag
        job_rows.append(f"{job_input_txt}, {output_root}, {job_tag}")

    # Write the manifest (item list) to disk
    with open(item_list_file, "w") as fitem:
        fitem.write("\n".join(job_rows))

    # 4. Write the SINGLE Submit File (Outside the loop)
    # This uses the modern 'queue ... from' syntax to avoid deprecation warnings.
    with open(master_condor_file, "w") as f:
        f.write("# HTCondor Submit File - V24 Compatible\n")
        f.write("Universe       = vanilla\n")
        f.write(f"Executable     = {args.exec}\n")
        f.write("getenv         = true\n")
        f.write("request_memory = 4G\n")
        f.write("notification   = Never\n\n")

        # Map the columns from our .items file to Condor variables
        extra = f" {args.job_args}" if args.job_args else ""
        f.write(f"Arguments = $(InFile) $(OutFile){extra}\n")
        f.write(f"Output         = {job_folder}/$(Tag).out\n")
        f.write(f"Error          = {job_folder}/$(Tag).err\n")
        f.write(f"Log            = {job_folder}/$(Tag).log\n\n")

        # The single Queue statement that replaces 1000 individual ones
        f.write(f"queue InFile, OutFile, Tag from {item_list_file}\n")

    # 5. Submit to Cluster
    print(f"Submitting to Condor...")
    try:
        subprocess.run(["condor_submit", master_condor_file], check=True)
        print("Submission successful.")
    except subprocess.CalledProcessError as e:
        print(f"Error during submission: {e}")

if __name__ == "__main__":
    main()

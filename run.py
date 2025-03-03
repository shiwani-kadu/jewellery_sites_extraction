import subprocess
import sys
import os
import time

def run_crawler(platform, region, crawler_type, max_retries=3):
    """
    Run a crawler script for a specific platform, region, and crawler type, with capabilities
    for retrying upon failure. The function executes the script in a subprocess while ensuring
    retries in case of errors, with an exponential backoff strategy between attempts. The current
    working directory is temporarily changed to the script's directory during execution.

    Arguments:
        platform (str): The name of the platform for which the crawler should run.
        region (str): The geographical region targeted by the crawler.
        crawler_type (str): The type of crawler to execute (e.g., "data", "log").
        max_retries (int): Optional, specifies the maximum number of retry attempts.
            Defaults to 3.

    Raises:
        subprocess.CalledProcessError: If the subprocess fails and all retry attempts
            are exhausted.
    """
    original_cwd = os.getcwd()
    crawler_path = f"crawlers/{platform}/{crawler_type}.py"
    
    for attempt in range(max_retries):
        try:
            os.chdir(os.path.dirname(crawler_path))
            command = [
                "python",
                os.path.basename(crawler_path),
                "--region", region
            ]
            result = subprocess.run(command, check=True)
            print(f"{crawler_type.capitalize()} crawler for {platform} in {region} completed successfully.")
            break  # Exit the loop if the crawler runs successfully
        except subprocess.CalledProcessError as e:
            print(f"Attempt {attempt + 1} failed: Error running {crawler_type} crawler for {platform} in {region}: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {2 ** attempt} seconds...")
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"Max retries exceeded for {crawler_type} crawler for {platform} in {region}.")
        finally:
            os.chdir(original_cwd)

def main():
    """
    Main script entry point for running different crawlers.

    This script accepts two command-line arguments, `platform` and `region`,
    to specify the platform and region for the crawlers. It runs both the
    listing crawler and PDP crawler for the given platform and region.

    Arguments:
        sys.argv[1] (str): The platform for which the crawlers are to be run.
        sys.argv[2] (str): The region for which the crawlers are to be run.

    Behavior:
        The script prints the purpose and details of the processes being
        executed, and runs two types of crawlers sequentially: the listing
        crawler and the PDP crawler.

    Usage:
        Requires exactly two arguments: the platform and region. Prints a
        usage message and exits if the provided arguments are incorrect.

    Raises:
        SystemExit: If the incorrect number of command-line arguments is
        provided.
    """
    if len(sys.argv) != 3:
        print("Usage: python run.py <platform> <region>")
        sys.exit(1)

    platform = sys.argv[1]
    region = str(sys.argv[2]).lower()

    # Run the listing crawler
    print(f"Running listing crawler for {platform} in {region}...")
    run_crawler(platform, region, "listing")

    # Run the PDP crawler
    print(f"Running PDP crawler for {platform} in {region}...")
    run_crawler(platform, region, "pdp")

if __name__ == "__main__":
    main()
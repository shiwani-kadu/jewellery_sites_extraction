### 1. **The `run_crawler` Function**
The `run_crawler` function is responsible for running a specific crawler script for a given platform, region, and type of crawler with retry capabilities. The details are as follows:
#### **Arguments**
- **`platform` **: The name of the platform for which the crawler should run (e.g., "siteA").
- **`region` **: The geographical region targeted by the crawler (e.g., "US").
- **`crawler_type` **: The type of crawler to execute (e.g., "data", "listing", "pdp").
- **`max_retries` **: How many times the function will attempt to rerun the crawler if an error occurs (default = 3).

#### **Function Functionality**
1. **Determine Crawler Script Path**
Creates the path to the specific crawler script:
``` python
crawler_path = f"crawlers/{platform}/{crawler_type}.py"
```
For example, for `platform="siteA"`, `crawler_type="listing"`, the script path would be `crawlers/siteA/listing.py`.
1. **Retry with Exponential Backoff**
The function uses a loop that attempts to run the crawler up to `max_retries` times. If it fails due to an error, it retries after waiting for an exponentially increasing duration (`2^attempt` seconds) before reattempting.
2. **Change Working Directory**
Temporarily switches the working directory to the location of the crawler script (so the crawler will run with the right context):
``` python
os.chdir(os.path.dirname(crawler_path))
```
1. **Run the Crawler Script**
Uses `subprocess.run` to execute the crawler script in a new process:
``` python
subprocess.run(command, check=True)
```
The `check=True` ensures an exception (`subprocess.CalledProcessError`) is raised if the script exits with an error code.
1. **Handle Errors and Retry**
If an exception occurs:
    - Prints an error message with details.
    - Waits before retrying (with exponential backoff).
    - Gives up after the maximum retries and prints that the crawler failed.

2. **Restore Original Directory**
Ensures the working directory is restored to its original state after each retry attempt:
``` python
os.chdir(original_cwd)
```
#### **Example Execution**
For a call like:
``` python
run_crawler("siteA", "US", "listing")
```
The function:
- Tries to run the `crawlers/siteA/listing.py` script.
- Retries up to 3 times if it encounters errors, waiting `2^0`, `2^1`, and `2^2` seconds between retries.

### 2. **The `main` Function**
The `main` function serves as the entry point of the script, accepting command-line arguments and orchestrating the crawler execution:
#### **Command-Line Arguments**
The script requires two arguments:
1. **`<platform>` **: The name of the platform (e.g., "siteA").
2. **`<region>` **: The target region (e.g., "US").

#### **Command-Line Input Validation**
The function ensures the correct number of arguments are provided:
``` python
if len(sys.argv) != 3:
    print("Usage: python run.py <platform> <region>")
    sys.exit(1)
```
#### **Sequential Execution of Crawlers**
The function runs two types of crawlers (both executed by calling `run_crawler`):
1. `listing` crawler: Executes the `run_crawler` function for the "listing" type:
``` python
run_crawler(platform, region, "listing")
```
1. `PDP` crawler: Executes the `run_crawler` function for the "pdp" type:
``` python
run_crawler(platform, region, "pdp")
```
#### **Example Execution**
If this command is run:
``` bash
python run.py siteA US
```
The `main` function will:
1. Run the "listing" crawler for the `siteA` platform in the `US` region.
2. Run the "pdp" crawler for the same platform and region.

If either crawler fails, it will retry up to three times as described in the `run_crawler` function.
### 3. **Key Features of the Script**
- **Retry Logic with Exponential Backoff**
Ensures that intermittent failures don't stop the process immediately, giving the crawler script a second (or third) chance.
- **Command-Line Interface**
The script dynamically accepts arguments for `platform` and `region`, making it flexible to use for any configuration.
- **Isolation with Subprocesses**
Each crawler is run in a separate process, which keeps the main script isolated and able to handle errors explicitly.
- **Dynamic Script Targeting**
The script can handle multiple platforms, regions, and crawler types without requiring further modifications.

### 4. **Usage**
To use the script:
1. Save the code in a file named `run.py`.
2. Ensure the crawler scripts (e.g., `listing.py` and `pdp.py`) exist under the expected directory structure, e.g., `crawlers/<platform>/<type>.py`.
3. Run the script from the command line:
``` bash
   python run.py <platform> <region>
```
For example:
``` bash
python run.py siteA US
```
Would:
- Run the `listing` crawler for `siteA` in the `US` region.
- Then run the `PDP` crawler for the same platform/region.

### 5. **Error Handling**
- If the required arguments are not provided, the script exits with a usage message.
- If an error occurs while running a crawler, it retries with a delay.
- If all retries fail, it logs the failure and exits.
